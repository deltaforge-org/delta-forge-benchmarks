"""Graph algorithms: GPU vs CPU crossover benchmark (DeltaForge only).

This workload answers one question with reproducible numbers: **for which
graph algorithms, and at what graph size, does GPU acceleration actually
beat the multi-core CPU path?** It is an honest crossover study, not a
GPU sales sheet: it runs the memory-bandwidth-bound algorithms (where the
CPU stays competitive) right next to the compute-bound ones (where the
GPU pulls ahead), at four graph sizes, so the table itself shows where
the line is.

Why this is a fair A/B
----------------------

In DeltaForge, GPU execution is strictly **opt-in**: a Cypher algorithm
call dispatches to the GPU only when the statement carries the `ON GPU`
hint (see `delta-forge-graph/src/columnar_executor/algorithms.rs`). Drop
the hint and the identical `CALL algo.X(...)` runs the CPU (Rayon,
all-cores) implementation. So the two arms of this benchmark are the same
query differing only by two words:

    CPU:  USE <graph> CALL algo.pageRank({...}) YIELD ... RETURN ...
    GPU:  USE <graph> ON GPU THRESHOLD 1 CALL algo.pageRank({...}) YIELD ... RETURN ...

`ON GPU THRESHOLD 1` forces the GPU path at every size (the default
per-algorithm node threshold would otherwise let small graphs fall back
to CPU silently, which would corrupt the comparison). To keep the GPU
column honest we only include algorithms whose GPU implementation is
*confirmed* by the `graph-gpu-10m-finance` correctness demo
(PageRank, Connected Components, Louvain, Betweenness, Triangle Count);
for those, `ON GPU` cannot silently degrade to CPU because the kernel
exists.

What is measured
----------------

The headline per (algorithm, size, device) is the **warm-median** of the
synchronous Cypher `execution_time_ms` (plan + compute + result drain on
the already-cached CSR topology). The one-time CSR build and GPU-buffer
(`.dgpu`) build happen in untimed setup via `CREATE GRAPHCSR` /
`CREATE GRAPHGPU`, exactly mirroring how the read benchmarks pay
table-attach cost in untimed setup. The per-phase GPU breakdown
(host->device transfer vs kernel vs readback) is exposed through the
`SHOW STATS ACTUAL` `cypher.gpu.*` rows; the adapter captures it into each
record's `extra` and the report surfaces it alongside the headline. The
end-to-end number stays the headline because it is what a client actually
experiences.

Graph sizes
-----------

1M / 5M / 10M / 30M nodes, roughly 4.5x edges per node (a mix of strided
dense neighbourhoods, hub fan-out, and pseudo-random weak ties, all
generated in-engine from `df_generate_table` + `generate_series` so there
is no on-disk input and the topology is byte-reproducible).

    RESOURCE NOTE: the 30M-node tier builds ~135M edges and needs a host
    with ~32 GB+ addressable memory for the CSR + algorithm score arrays.
    On a 16 GB host, run the smaller tiers only:
        GRAPH_BENCH_SIZES_M="1,5,10" python bench_runner.py ...

    TIMEOUT NOTE: the cold CSR/GPU-sidecar build for the large tiers can
    exceed the CLI's default HTTP timeout. Run with DF_HTTP_TIMEOUT_SECS=0
    (or >= 1800) exported in the environment.

Tunable via environment (read once at import):
    GRAPH_BENCH_SIZES_M  comma list of node counts in millions. Default "1,5,10,30".
    GRAPH_BENCH_WARM     warm runs per measured step. Default 3.
"""
from __future__ import annotations

import os

from engines.base import STEP_CYPHER_QUERY, STEP_SQL_DDL, WorkloadStep
from .spec import Workload


# ---------------------------------------------------------------------------
# Tunables (read once at import; the runner builds WORKLOAD from these)
# ---------------------------------------------------------------------------

def _sizes() -> list[int]:
    raw = os.environ.get("GRAPH_BENCH_SIZES_M", "1,5,10,30")
    out: list[int] = []
    for tok in raw.split(","):
        tok = tok.strip()
        if tok:
            out.append(int(tok))
    if not out:
        raise ValueError("GRAPH_BENCH_SIZES_M resolved to an empty size list")
    return out


def _warm_runs() -> int:
    return max(1, int(os.environ.get("GRAPH_BENCH_WARM", "3")))


_SIZES = _sizes()
_WARM = _warm_runs()

# Delta zone + storage root. `{data_dir}` is substituted by the runner to
# the workload's data directory (data/graph_gpu_vs_cpu), which is the same
# path the df server sees in both the docker stack and a local run.
_ZONE = "graph_bench"
_ROOT = "{data_dir}/graph_bench"


# ---------------------------------------------------------------------------
# Algorithm matrix. Every entry has a CONFIRMED GPU implementation (proven
# by demos/graph/graph-gpu-10m-finance running it ON GPU with asserted
# results), so the GPU arm cannot silently fall back to CPU.
#
# `klass` is the expected crossover class, used only by the report's
# framing:
#   memory_bound  -> sparse, latency/bandwidth limited; CPU stays competitive
#   compute_bound -> more arithmetic per edge / per sample; GPU pulls ahead
#
# YIELD signatures are copied verbatim from the verified demos.
# ---------------------------------------------------------------------------

ALGOS: list[tuple[str, str, str, str]] = [
    (
        "pagerank_i5",
        "PageRank (5 iter)",
        "CALL algo.pageRank({dampingFactor: 0.85, iterations: 5}) "
        "YIELD node_id, score, rank "
        "RETURN node_id, score, rank ORDER BY score DESC LIMIT 25",
        "memory_bound",
    ),
    (
        "pagerank_i50",
        "PageRank (50 iter)",
        "CALL algo.pageRank({dampingFactor: 0.85, iterations: 50}) "
        "YIELD node_id, score, rank "
        "RETURN node_id, score, rank ORDER BY score DESC LIMIT 25",
        "compute_bound",
    ),
    (
        "wcc",
        "Connected Components",
        "CALL algo.connectedComponents() "
        "YIELD node_id, component_id "
        "RETURN component_id, count(*) AS sz ORDER BY sz DESC LIMIT 25",
        "memory_bound",
    ),
    (
        "louvain",
        "Louvain",
        "CALL algo.louvain({resolution: 1.0}) "
        "YIELD node_id, community_id "
        "RETURN community_id, count(*) AS sz ORDER BY sz DESC LIMIT 25",
        "memory_bound",
    ),
    (
        "triangles",
        "Triangle Count",
        "CALL algo.triangleCount() "
        "YIELD node_id, triangle_count "
        "RETURN node_id, triangle_count ORDER BY triangle_count DESC LIMIT 25",
        "compute_bound",
    ),
    (
        "betweenness",
        "Betweenness (sampled 128)",
        "CALL algo.betweenness({samplingSize: 128}) "
        "YIELD node_id, centrality, rank "
        "RETURN node_id, centrality, rank ORDER BY centrality DESC LIMIT 25",
        "compute_bound",
    ),
]


# ---------------------------------------------------------------------------
# Synthetic graph generation (parametric on N = nodes).
#
# Token replacement / inlined integer literals are used rather than
# str.format / f-strings around the df_generate_table JSON, because that
# JSON contains `{` / `}` that would collide with format-field syntax.
# ---------------------------------------------------------------------------

def _node_insert_sql(nodes: str, n: int) -> str:
    return (
        "INSERT INTO " + nodes + "\n"
        "SELECT id, first_name || ' ' || last_name AS name, grp\n"
        "FROM df_generate_table(@N@, '[\n"
        '  {"type":"row_index","name":"id","start":1},\n'
        '  {"type":"fake","name":"first_name","kind":"first_name","seed":7},\n'
        '  {"type":"fake","name":"last_name","kind":"last_name","seed":8},\n'
        '  {"type":"cyclic_lookup","name":"grp","values":["A","B","C","D","E","F","G","H"]}\n'
        "]')"
    ).replace("@N@", str(n))


def _stride_batch_sql(edges: str, n: int, stride: int, id_off: int) -> str:
    """A dense strided neighbourhood batch: every node i links to i+stride
    (mod N). N edges before the self-loop filter. Cyclic so it stays
    well-connected at any size."""
    return (
        "INSERT INTO " + edges + "\n"
        "SELECT id, src, dst,\n"
        "  ROUND(0.5 + 0.5*((CAST(src*7 + dst*13 AS DOUBLE)*0.618033988749895) % 1.0), 3) AS weight,\n"
        "  CASE (CAST(src + dst AS BIGINT) % 4) "
        "WHEN 0 THEN 'wire' WHEN 1 THEN 'ach' WHEN 2 THEN 'card' ELSE 'debit' END AS etype\n"
        "FROM (\n"
        f"  SELECT {id_off} + gs AS id, ((gs - 1) % {n}) + 1 AS src, "
        f"(((gs - 1) + {stride}) % {n}) + 1 AS dst\n"
        f"  FROM generate_series(1, {n}) AS t(gs)\n"
        ") sub\n"
        "WHERE src <> dst"
    )


def _hub_batch_sql(edges: str, n: int) -> str:
    """Hub fan-out: every 1000th node is a hub that links to 50 nearby
    targets. Creates the high-degree vertices and triangles that give the
    compute-bound algorithms (triangle count, betweenness) real work."""
    hubs = n // 1000
    id_off = 4 * n
    return (
        "INSERT INTO " + edges + "\n"
        "SELECT id, src, dst,\n"
        "  ROUND(0.4 + 0.5*((CAST(src*31 + dst*37 AS DOUBLE)*0.618033988749895) % 1.0), 3) AS weight,\n"
        "  'hub' AS etype\n"
        "FROM (\n"
        f"  SELECT {id_off} + ((h - 1) * 50 + k) AS id, (h * 1000) AS src, "
        f"(((h * 1000 - 1) + k * 7) % {n}) + 1 AS dst\n"
        f"  FROM (SELECT gs AS h FROM generate_series(1, {hubs}) AS t(gs)) hh\n"
        "  CROSS JOIN (SELECT gs AS k FROM generate_series(1, 50) AS t(gs)) kk\n"
        ") sub\n"
        f"WHERE src <> dst AND src BETWEEN 1 AND {n} AND dst BETWEEN 1 AND {n}"
    )


def _random_batch_sql(edges: str, n: int) -> str:
    """Pseudo-random weak ties (N/2 edges) via two coprime multipliers, so
    src and dst streams are effectively independent. Spreads diffuse links
    across the whole graph and lengthens shortest paths."""
    half = n // 2
    id_off = 5 * n
    return (
        "INSERT INTO " + edges + "\n"
        "SELECT id, src, dst,\n"
        "  ROUND(0.05 + 0.2*((CAST(src*43 + dst*47 AS DOUBLE)*0.618033988749895) % 1.0), 3) AS weight,\n"
        "  'p2p' AS etype\n"
        "FROM (\n"
        f"  SELECT {id_off} + i AS id, ((i * 104729 + 56891) % {n}) + 1 AS src, "
        f"((i * 224737 + 31547) % {n}) + 1 AS dst\n"
        f"  FROM generate_series(1, {half}) AS t(i)\n"
        ") sub\n"
        "WHERE src <> dst"
    )


def _setup_sql(m: int) -> str:
    """Full build script for the m-million-node graph: tables, ~4.5x edges,
    Z-ORDER, graph definition, CSR sidecar, GPU sidecar. Idempotent:
    drops the Delta tables WITH FILES first so a re-run rebuilds cleanly."""
    n = m * 1_000_000
    schema = f"g{m}m"
    nodes = f"{_ZONE}.{schema}.nodes"
    edges = f"{_ZONE}.{schema}.edges"
    g = f"{_ZONE}.{schema}.g"

    parts: list[str] = [
        f"CREATE SCHEMA IF NOT EXISTS {_ZONE}.{schema}",
        f"DROP DELTA TABLE IF EXISTS {edges} WITH FILES",
        f"DROP DELTA TABLE IF EXISTS {nodes} WITH FILES",
        f"CREATE DELTA TABLE {nodes} (id BIGINT, name STRING, grp STRING) "
        f"LOCATION '{_ROOT}/{schema}/nodes'",
        f"CREATE DELTA TABLE {edges} "
        f"(id BIGINT, src BIGINT, dst BIGINT, weight DOUBLE, etype STRING) "
        f"LOCATION '{_ROOT}/{schema}/edges'",
        _node_insert_sql(nodes, n),
        _stride_batch_sql(edges, n, 20, 0),
        _stride_batch_sql(edges, n, 40, n),
        _stride_batch_sql(edges, n, 200, 2 * n),
        _stride_batch_sql(edges, n, 21, 3 * n),
        _hub_batch_sql(edges, n),
        _random_batch_sql(edges, n),
        f"OPTIMIZE {edges} ZORDER BY (src, dst)",
        f"OPTIMIZE {nodes} ZORDER BY (id)",
        f"CREATE GRAPH IF NOT EXISTS {g} "
        f"VERTEX TABLE {nodes} ID COLUMN id NODE TYPE COLUMN grp NODE NAME COLUMN name "
        f"EDGE TABLE {edges} SOURCE COLUMN src TARGET COLUMN dst "
        f"WEIGHT COLUMN weight EDGE TYPE COLUMN etype DIRECTED",
        f"CREATE GRAPHCSR {g}",
        f"CREATE GRAPHGPU {g}",
    ]
    return ";\n".join(parts) + ";\n"


def _cleanup_sql(m: int) -> str:
    schema = f"g{m}m"
    nodes = f"{_ZONE}.{schema}.nodes"
    edges = f"{_ZONE}.{schema}.edges"
    g = f"{_ZONE}.{schema}.g"
    parts = [
        f"DROP GRAPH IF EXISTS {g}",
        f"DROP DELTA TABLE IF EXISTS {edges} WITH FILES",
        f"DROP DELTA TABLE IF EXISTS {nodes} WITH FILES",
    ]
    return ";\n".join(parts) + ";\n"


# ---------------------------------------------------------------------------
# Step construction
# ---------------------------------------------------------------------------

def _algo_step(m: int, key: str, label: str, call: str, device: str) -> WorkloadStep:
    g = f"{_ZONE}.g{m}m.g"
    if device == "gpu":
        # THRESHOLD 1 forces GPU dispatch at every size (no silent
        # below-threshold fall-back to CPU).
        sql = f"USE {g} ON GPU THRESHOLD 1 {call}"
    else:
        sql = f"USE {g} {call}"
    return WorkloadStep(
        id=f"{key}__{m}m__{device}",
        kind=STEP_CYPHER_QUERY,
        sql=sql,
        description=f"{label} on {m}M-node graph ({device.upper()})",
    )


def _setup_steps() -> list[WorkloadStep]:
    steps = [
        WorkloadStep(
            id="prep_zone",
            kind=STEP_SQL_DDL,
            sql=f"CREATE ZONE IF NOT EXISTS {_ZONE} TYPE DELTA STORAGE_ROOT '{_ROOT}'",
            description="Create the Delta zone for graph fixtures",
            measured=False,
        )
    ]
    for m in _SIZES:
        steps.append(
            WorkloadStep(
                id=f"build_{m}m",
                kind=STEP_SQL_DDL,
                sql=_setup_sql(m),
                description=f"Build {m}M-node graph + CSR + GPU sidecar",
                measured=False,
            )
        )
    return steps


def _measured_steps() -> list[WorkloadStep]:
    out: list[WorkloadStep] = []
    # Group by size so each graph's CSR stays warm in the worker across its
    # whole algorithm sweep; within a size, CPU then GPU for each algorithm.
    for m in _SIZES:
        for key, label, call, _klass in ALGOS:
            for device in ("cpu", "gpu"):
                out.append(_algo_step(m, key, label, call, device))
    return out


def _cleanup_steps() -> list[WorkloadStep]:
    return [
        WorkloadStep(
            id=f"drop_{m}m",
            kind=STEP_SQL_DDL,
            sql=_cleanup_sql(m),
            description=f"Drop {m}M-node graph + tables",
            measured=False,
        )
        for m in _SIZES
    ]


WORKLOAD = Workload(
    name="graph_gpu_vs_cpu",
    description=(
        "Graph algorithm GPU-vs-CPU crossover on synthetic graphs "
        f"({'/'.join(f'{m}M' for m in _SIZES)} nodes); DeltaForge only."
    ),
    setup_steps=_setup_steps(),
    measured_steps=_measured_steps(),
    cleanup_steps=_cleanup_steps(),
    cold_runs=1,
    warm_runs=_WARM,
    requires_input_data=False,
    applicable_engines=("df",),
    data_subdir="graph_gpu_vs_cpu",
)
