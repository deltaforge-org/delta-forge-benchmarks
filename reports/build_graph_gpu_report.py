"""Aggregate a graph_gpu_vs_cpu run into a publish-ready crossover page.

Unlike the four read benchmarks (engine-vs-engine on the same query), this
workload runs ONE engine (DeltaForge) and compares two devices, CPU vs
GPU, across several graph sizes. So it gets its own report shape: per-size
tables of (algorithm -> CPU ms, GPU ms, speedup) plus a crossover summary
that shows where the GPU overtakes the CPU.

Usage:
    python reports/build_graph_gpu_report.py \
        --results-dir results/<timestamp>-<host> \
        --out published/graph_gpu.md

Reads only `raw/df.jsonl` (the workload is DeltaForge-only). Step ids are
`<algo>__<m>m__<cpu|gpu>`; algorithm labels and crossover classes come
from the workload module so there is a single source of truth.
"""
from __future__ import annotations

import argparse
import json
import re
import statistics
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))  # so `import workloads.*` works standalone

from workloads.graph_gpu_vs_cpu import ALGOS  # noqa: E402

# (key -> (label, klass)) and the canonical algorithm order for the tables.
ALGO_LABEL = {k: label for (k, label, _call, _klass) in ALGOS}
ALGO_KLASS = {k: klass for (k, _label, _call, klass) in ALGOS}
ALGO_ORDER = [k for (k, _l, _c, _k) in ALGOS]

KLASS_LABEL = {
    "memory_bound": "memory-bound",
    "compute_bound": "compute-bound",
}

_STEP_RE = re.compile(r"^(?P<algo>[a-z0-9_]+?)__(?P<m>\d+)m__(?P<dev>cpu|gpu)$")


# Per-phase metrics scraped from each record's `extra` (populated by
# df_engine.py from the SHOW STATS ACTUAL `cypher.*` rows). Times are decimal
# ms; counts carry thousands separators, so every value is comma-stripped
# before float(). `time.total_engine_time_ms` is the engine's auth-free
# apples-to-apples number; the `cypher.gpu.*` keys are present only on the GPU
# arm (NULL on CPU), so their presence also confirms the GPU path fired.
_PHASE_KEYS = (
    "time.total_engine_time_ms",
    "cypher.algorithm.algorithm_ms",
    "cypher.gpu.gpu_wall_ms",
    "cypher.gpu.compute_ms",
    "cypher.gpu.transfer_to_gpu_ms",
    "cypher.gpu.transfer_from_gpu_ms",
    "cypher.csr.load_ms",
)


def _num(val) -> float | None:
    if val is None:
        return None
    try:
        return float(str(val).replace(",", ""))
    except (TypeError, ValueError):
        return None


def load_df_records(jsonl_path: Path) -> tuple[dict, dict, set[int], dict]:
    """Parse df.jsonl into:
      warm[(algo, m, dev)]    -> warm-median ms (engine-reported, wall fallback)
      cold[(algo, m, dev)]    -> cold ms (first run; includes topology load)
      sizes                   -> set of m values present
      phases[(algo, m, dev)]  -> {phase_key: warm-median ms} from SHOW STATS
    A step with `error` set is dropped from the median (a failed run is not
    a measurement of useful work)."""
    warm_runs: dict[tuple, list[float]] = defaultdict(list)
    cold_runs: dict[tuple, float] = {}
    sizes: set[int] = set()
    phase_runs: dict[tuple, dict[str, list[float]]] = defaultdict(lambda: defaultdict(list))

    for line in jsonl_path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        r = json.loads(line)
        sid = r.get("step_id") or ""
        mobj = _STEP_RE.match(sid)
        if not mobj:
            continue
        if r.get("error") is not None:
            continue
        ms = r.get("engine_reported_ms")
        if ms is None:
            ms = r.get("wall_ms")
        if ms is None:
            continue
        algo = mobj.group("algo")
        m = int(mobj.group("m"))
        dev = mobj.group("dev")
        sizes.add(m)
        key = (algo, m, dev)
        if r.get("cold"):
            cold_runs[key] = float(ms)
        else:
            warm_runs[key].append(float(ms))
            extra = r.get("extra") or {}
            for pk in _PHASE_KEYS:
                pv = _num(extra.get(pk))
                if pv is not None:
                    phase_runs[key][pk].append(pv)

    warm = {k: statistics.median(v) for k, v in warm_runs.items() if v}
    phases = {
        k: {pk: statistics.median(vs) for pk, vs in d.items() if vs}
        for k, d in phase_runs.items()
    }
    return warm, cold_runs, sizes, phases


def fmt_ms(ms: float | None) -> str:
    return "-" if ms is None else f"{ms:,.1f}"


def fmt_speedup(cpu: float | None, gpu: float | None) -> str:
    """GPU speedup vs CPU. >1.00x means GPU is faster."""
    if cpu is None or gpu is None or gpu <= 0:
        return "-"
    return f"{cpu / gpu:.2f}x"


def render_table(rows: list[tuple], header: list[str], align: list[str]) -> str:
    out = ["| " + " | ".join(header) + " |",
           "| " + " | ".join(align) + " |"]
    for row in rows:
        out.append("| " + " | ".join(str(c) for c in row) + " |")
    return "\n".join(out)


def per_size_section(m: int, warm: dict) -> str:
    rows = []
    for algo in ALGO_ORDER:
        cpu = warm.get((algo, m, "cpu"))
        gpu = warm.get((algo, m, "gpu"))
        rows.append((
            ALGO_LABEL[algo],
            KLASS_LABEL.get(ALGO_KLASS[algo], ALGO_KLASS[algo]),
            fmt_ms(cpu),
            fmt_ms(gpu),
            fmt_speedup(cpu, gpu),
        ))
    table = render_table(
        rows,
        ["algorithm", "class", "CPU warm (ms)", "GPU warm (ms)", "GPU speedup"],
        ["---", "---", "---:", "---:", "---:"],
    )
    return f"### {m:,}M nodes\n\n{table}\n"


def crossover_section(warm: dict, sizes: list[int]) -> str:
    """One row per algorithm: GPU speedup at each size, plus the smallest
    size at which GPU first reaches parity (>= 1.0x)."""
    header = ["algorithm", "class"] + [f"{m}M" for m in sizes] + ["GPU wins from"]
    align = ["---", "---"] + ["---:" for _ in sizes] + ["---:"]
    rows = []
    for algo in ALGO_ORDER:
        cells = []
        crossover = "never (in range)"
        for m in sizes:
            cpu = warm.get((algo, m, "cpu"))
            gpu = warm.get((algo, m, "gpu"))
            cells.append(fmt_speedup(cpu, gpu))
            if cpu is not None and gpu is not None and gpu > 0 and cpu / gpu >= 1.0:
                if crossover == "never (in range)":
                    crossover = f"{m}M"
        rows.append((ALGO_LABEL[algo], KLASS_LABEL.get(ALGO_KLASS[algo], ALGO_KLASS[algo]),
                     *cells, crossover))
    return render_table(rows, header, align)


def _has_phase_data(phases: dict) -> bool:
    """True when at least one record carried SHOW STATS cypher.* breakdown
    rows (i.e. the engine build emits them and df_engine captured them)."""
    return any(d for d in phases.values())


def gpu_phase_section(m: int, warm: dict, phases: dict) -> str:
    """Where the GPU end-to-end time goes, per algorithm: total vs the device
    wall (transfers + kernel) vs the pure kernel, with the host-side remainder
    (planning + expansion + property gather + result drain + any CSR load)
    called out. Sourced from the SHOW STATS ACTUAL cypher.gpu.* rows."""
    rows = []
    for algo in ALGO_ORDER:
        gkey = (algo, m, "gpu")
        ph = phases.get(gkey, {})
        end_to_end = warm.get(gkey)
        gpu_wall = ph.get("cypher.gpu.gpu_wall_ms")
        kernel = ph.get("cypher.gpu.compute_ms")
        t_to = ph.get("cypher.gpu.transfer_to_gpu_ms")
        t_from = ph.get("cypher.gpu.transfer_from_gpu_ms")
        transfers = None
        if t_to is not None or t_from is not None:
            transfers = (t_to or 0.0) + (t_from or 0.0)
        host_rest = None
        if end_to_end is not None and gpu_wall is not None:
            host_rest = max(0.0, end_to_end - gpu_wall)
        rows.append((
            ALGO_LABEL[algo],
            fmt_ms(end_to_end),
            fmt_ms(gpu_wall),
            fmt_ms(kernel),
            fmt_ms(transfers),
            fmt_ms(host_rest),
        ))
    table = render_table(
        rows,
        ["algorithm", "end-to-end (ms)", "GPU wall (ms)", "kernel (ms)",
         "transfers H2D+D2H (ms)", "host rest (ms)"],
        ["---", "---:", "---:", "---:", "---:", "---:"],
    )
    return f"### {m:,}M nodes: GPU time breakdown\n\n{table}\n"


def compute_crossover_section(phases: dict, sizes: list[int]) -> str:
    """Pure-compute crossover: CPU algorithm phase
    (cypher.algorithm.algorithm_ms, Rayon cores) vs GPU kernel
    (cypher.gpu.compute_ms). Strips host/device transfer, planning, and result
    drain from BOTH arms, so it is the symmetric kernel-vs-cores number that
    complements the end-to-end crossover."""
    header = ["algorithm", "class"] + [f"{m}M" for m in sizes] + ["compute wins from"]
    align = ["---", "---"] + ["---:" for _ in sizes] + ["---:"]
    rows = []
    for algo in ALGO_ORDER:
        cells = []
        crossover = "never (in range)"
        for m in sizes:
            cpu = phases.get((algo, m, "cpu"), {}).get("cypher.algorithm.algorithm_ms")
            gpu = phases.get((algo, m, "gpu"), {}).get("cypher.gpu.compute_ms")
            cells.append(fmt_speedup(cpu, gpu))
            if cpu is not None and gpu is not None and gpu > 0 and cpu / gpu >= 1.0:
                if crossover == "never (in range)":
                    crossover = f"{m}M"
        rows.append((ALGO_LABEL[algo],
                     KLASS_LABEL.get(ALGO_KLASS[algo], ALGO_KLASS[algo]),
                     *cells, crossover))
    return render_table(rows, header, align)


def render_md(results_dir: Path, manifest: dict) -> str:
    raw = results_dir / "raw" / "df.jsonl"
    if not raw.exists():
        raise SystemExit(f"no df.jsonl under {results_dir / 'raw'}")
    warm, _cold, size_set, phases = load_df_records(raw)
    if not size_set:
        raise SystemExit(
            "no graph_gpu_vs_cpu records found in df.jsonl "
            "(step ids must look like 'pagerank_i5__10m__gpu')"
        )
    sizes = sorted(size_set)

    host = manifest.get("host", {})
    cpu_facts = host.get("cpu", {})
    mem = host.get("memory", {})
    gpu_facts = (manifest.get("engine_versions", {}).get("df", {}) or {})
    when = manifest.get("started_at") or datetime.now(timezone.utc).isoformat()

    per_size = "\n".join(per_size_section(m, warm) for m in sizes)
    crossover = crossover_section(warm, sizes)

    # Per-phase breakdown + pure-compute crossover. Rendered only when the
    # engine build emitted the SHOW STATS ACTUAL cypher.* rows and df_engine
    # captured them; older runs (or an engine without the detailed stats)
    # simply omit these sections rather than print all-dash tables.
    if _has_phase_data(phases):
        gpu_breakdown = "\n".join(gpu_phase_section(m, warm, phases) for m in sizes)
        compute_crossover = compute_crossover_section(phases, sizes)
        breakdown_md = f"""## GPU time breakdown (where the end-to-end time goes)

For each algorithm on the GPU arm, the end-to-end `total_time_ms` is split
into the device wall (host->device transfer + kernel + device->host readback),
the pure kernel, the transfer cost, and the host-side remainder (planning,
expansion, property gather, result drain, and any CSR load). Sourced from the
`SHOW STATS ACTUAL` `cypher.gpu.*` rows, warm-median.

{gpu_breakdown}
## Pure-compute crossover (kernel vs cores)

CPU algorithm phase (`cypher.algorithm.algorithm_ms`, Rayon cores) vs GPU
kernel (`cypher.gpu.compute_ms`), with host/device transfer, planning, and
result drain removed from both arms. This is the symmetric kernel-vs-cores
counterpart to the end-to-end crossover above: it isolates raw compute, so
the GPU advantage shows earlier here than in the end-to-end number that a
client actually pays.

{compute_crossover}
"""
    else:
        breakdown_md = ""

    md = f"""# Graph analytics: GPU vs CPU (DeltaForge benchmark)

Same query, two devices. Every algorithm runs as a Cypher `CALL algo.*`
once on the CPU (Rayon, all cores) and once on the GPU (`ON GPU THRESHOLD
1`), across graph sizes of {", ".join(f"{m}M" for m in sizes)} nodes
(~4.5x edges per node). The point is the **crossover**: which algorithms
the GPU actually wins, and from what size.

> One page per benchmark; full methodology in [methodology.md](methodology.md).
> Other benchmarks: [TPC-H](tpch.md) · [TPC-DS](tpcds.md) · [SSB](ssb.md) · [JOB](job.md) · [Writes](writes.md) · [Index](index.md)

## How to read this

- **GPU speedup > 1.00x** means the GPU finished faster than the CPU on
  the identical query. **< 1.00x** means the CPU won (the GPU's
  host/device overhead was not repaid by the available parallel work).
- Numbers are the **warm-median** synchronous `execution_time_ms` (plan +
  compute + result drain) on the already-cached CSR topology. The
  one-time CSR build and GPU-buffer build run in untimed setup
  (`CREATE GRAPHCSR` / `CREATE GRAPHGPU`), the same way the read
  benchmarks pay table-attach cost in untimed setup.
- This is an **honest crossover study**. Memory-bandwidth-bound
  algorithms (Connected Components, Louvain, low-iteration PageRank) are
  expected to keep the CPU competitive; compute-bound ones (Triangle
  Count, sampled Betweenness, high-iteration PageRank) are where the GPU
  earns its keep, and the advantage grows with graph size.

## Crossover summary (GPU speedup vs CPU)

{crossover}

## Per-size detail

{per_size}
{breakdown_md}
## What this benchmark does NOT claim

- **End-to-end is the headline.** The crossover summary and per-size
  tables use the full per-request wall-clock (`total_time_ms`: plan +
  host/device transfer + kernel + result drain), measured identically on
  both arms, because that is the time a client actually experiences. The
  GPU time breakdown and pure-compute crossover sections (when present)
  expose where that time goes, but they do not change the headline.
- **Only confirmed-GPU algorithms.** The matrix is limited to algorithms
  whose GPU kernel is verified by the `graph-gpu-10m-finance` correctness
  demo (PageRank, Connected Components, Louvain, Betweenness, Triangle
  Count). `ON GPU THRESHOLD 1` forces GPU dispatch at every size, so the
  GPU column cannot silently degrade to the CPU path.
- **A timing study, not a centrality-accuracy study.** Betweenness uses a
  small fixed sample so the CPU arm completes at 30M; the GPU/CPU ratio is
  the signal, not the centrality values.

## Host

| | |
| --- | --- |
| CPU | {cpu_facts.get("model", "?")} ({cpu_facts.get("physical_cores", "?")} physical / {cpu_facts.get("logical_threads", "?")} threads) |
| Memory | {mem.get("total_gib", "?")} GiB total |
| GPU | see worker logs / `engine_versions.df` in manifest |
| Run started | {when} |
| Run id | `{results_dir.name}` |

DeltaForge build: `{gpu_facts.get("cli_version", "?")}` (git `{gpu_facts.get("df_git_sha", "?")}`).

## Reproducing this table

```bash
# 30M needs ~32 GB RAM; on a smaller host set GRAPH_BENCH_SIZES_M="1,5,10".
# DF_HTTP_TIMEOUT_SECS=0 keeps the cold CSR/GPU build from tripping the
# CLI HTTP timeout at the large tiers.
export DF_HTTP_TIMEOUT_SECS=0
python bench_runner.py --engines df --workloads graph_gpu_vs_cpu --no-purge
python reports/build_graph_gpu_report.py \\
    --results-dir results/{results_dir.name} --out published/graph_gpu.md
```

Raw per-step records: [`{results_dir.name}/raw/df.jsonl`](../{results_dir}/raw/df.jsonl).
Manifest: [`{results_dir.name}/manifest.json`](../{results_dir}/manifest.json).

---

Next: [TPC-H](tpch.md) · [Methodology](methodology.md) · [Index](index.md)
"""
    return md


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--results-dir", required=True, type=Path)
    p.add_argument("--out", required=True, type=Path)
    args = p.parse_args()

    manifest_path = args.results_dir / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8")) if manifest_path.exists() else {}
    md = render_md(args.results_dir, manifest)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(md, encoding="utf-8")
    print(f"wrote {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
