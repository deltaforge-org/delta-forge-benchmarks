"""Aggregate a four-engine bench run into a publish-ready markdown.

Reads one run's `raw/*.jsonl` files, computes warm-median per
(engine, query), produces the published `<bench>.md` page for the
marketing site to link to.

Usage:
    python reports/build_published.py \
        --results-dir results/<timestamp>-<host> \
        --bench tpch_read_delta \
        --out published/tpch.md

The four bench results land at published/{tpch,tpcds,ssb,job}.md and
are cross-linked via the index. Run this once per workload after the
bench completes.
"""
from __future__ import annotations

import argparse
import json
import statistics
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path


BENCH_TITLES = {
    "tpch_read_delta":  ("TPC-H",  "22 canonical TPC-H queries on the 8-table schema, SF=1"),
    "tpcds_read_delta": ("TPC-DS", "99 canonical TPC-DS queries on the 24-table snowflake schema, SF=1"),
    "ssb_read_delta":   ("SSB",    "13 Star Schema Benchmark queries (O'Neil et al. 2009) on a 5-table star, SF=1"),
    "job_read_delta":   ("JOB",    "113 Join Order Benchmark queries (Leis et al. VLDB 2015) on the 21-table IMDB snapshot"),
    "synthetic_write_delta": ("Writes", "Synthetic 10M-row CTAS into plain Delta from deterministic in-memory generators"),
}

BENCH_NEIGHBOURS = {
    "tpch_read_delta":  ("TPC-DS", "tpcds.md"),
    "tpcds_read_delta": ("SSB",    "ssb.md"),
    "ssb_read_delta":   ("JOB",    "job.md"),
    "job_read_delta":   ("Writes", "writes.md"),
    "synthetic_write_delta": ("TPC-H",  "tpch.md"),
}

# Workloads that produce a single measured step (writes), rendered with a
# throughput-oriented page instead of a per-query table.
WRITE_WORKLOADS = {"synthetic_write_delta"}

# Row count per write workload (used to compute throughput).
WRITE_ROW_COUNTS = {
    "synthetic_write_delta": 10_000_000,
}

ENGINE_ORDER = ("df", "duckdb", "spark-default", "spark-tuned")
ENGINE_LABEL = {
    "df":             "DeltaForge",
    "duckdb":         "DuckDB",
    "spark-default":  "Spark (default)",
    "spark-tuned":    "Spark (tuned)",
}


def load_engine_records(jsonl_path: Path) -> tuple[dict, dict]:
    """Return (warm_medians_by_query, summary) for one engine's JSONL.

    warm_medians_by_query: {q_id: median_warm_ms or None for failed}
    summary: {"ok": int, "fail": int, "first_error": str|None,
              "cold_start_ready_ms": float|None}
    """
    warm: dict[str, list[float]] = defaultdict(list)
    cold: dict[str, float] = {}
    fail = 0
    ok = 0
    first_err: str | None = None
    for line in jsonl_path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        r = json.loads(line)
        sid = r.get("step_id") or ""
        if not sid.startswith("q"):
            continue
        wall = r.get("wall_ms")
        eng = r.get("engine_reported_ms")
        ms = eng if eng is not None else wall
        # Any step with `error` set is a failure regardless of wall_ms.
        # Parse failures take ~100-300ms of round-trip; that wall_ms is
        # not a measurement of useful work and must not leak into the
        # published median.
        if ms is None or r.get("error") is not None:
            fail += 1
            if first_err is None:
                first_err = (r.get("error") or "unknown").splitlines()[0][:140]
            continue
        ok += 1
        if r.get("cold"):
            cold[sid] = ms
        else:
            warm[sid].append(ms)
    meds = {q: (statistics.median(v) if v else None) for q, v in warm.items()}
    return meds, {
        "ok": ok,
        "fail": fail,
        "first_error": first_err,
        "n_queries": len(warm),
    }


def speedup(df_ms: float | None, other_ms: float | None) -> str:
    """Format an engine's warm time as a speedup vs df.

    >1.0x means engine is faster than df. <1.0x means engine is slower."""
    if df_ms is None or other_ms is None or other_ms <= 0:
        return "-"
    return f"{df_ms / other_ms:.2f}x"


def render_table(rows: list[tuple], cols: list[tuple[str, str]]) -> str:
    """Render a markdown table.

    rows: list of tuples matching cols
    cols: list of (header, align) tuples where align is ":---", "---:", or "---"
    """
    out = ["| " + " | ".join(h for h, _ in cols) + " |",
           "| " + " | ".join(a for _, a in cols) + " |"]
    for row in rows:
        out.append("| " + " | ".join(str(c) for c in row) + " |")
    return "\n".join(out)


def fmt_ms(ms: float | None) -> str:
    if ms is None:
        return "-"
    return f"{ms:,.2f}"


def render_bench_md(bench: str, results_dir: Path, manifest: dict) -> str:
    """Produce the per-bench publish markdown."""
    title, subtitle = BENCH_TITLES[bench]
    raw_dir = results_dir / "raw"

    medians: dict[str, dict[str, float | None]] = {}
    summaries: dict[str, dict] = {}
    queries: set[str] = set()
    for eng in ENGINE_ORDER:
        jsonl = raw_dir / f"{eng}.jsonl"
        if not jsonl.exists():
            medians[eng] = {}
            summaries[eng] = {"ok": 0, "fail": 0, "first_error": "engine not run", "n_queries": 0}
            continue
        m, s = load_engine_records(jsonl)
        medians[eng] = m
        summaries[eng] = s
        queries.update(m.keys())

    qs = sorted(queries)
    cols = [("query", "---")] + [(ENGINE_LABEL[e], "---:") for e in ENGINE_ORDER]
    perq_rows = []
    for q in qs:
        row = [q]
        for eng in ENGINE_ORDER:
            row.append(fmt_ms(medians[eng].get(q)))
        perq_rows.append(tuple(row))

    # Aggregate medians per engine across queries present (only on queries
    # the engine actually completed; "-" rows excluded).
    agg = {}
    for eng in ENGINE_ORDER:
        m = [v for v in medians[eng].values() if v is not None]
        agg[eng] = statistics.median(m) if m else None
    median_row = ["**median (across completed queries)**"] + [
        f"**{fmt_ms(agg[e])}**" for e in ENGINE_ORDER
    ]

    # Speedup table vs df
    sp_cols = [("query", "---")] + [
        (ENGINE_LABEL[e], "---:") for e in ENGINE_ORDER if e != "df"
    ]
    sp_rows = []
    df_med = medians["df"]
    for q in qs:
        row = [q]
        for eng in ENGINE_ORDER:
            if eng == "df":
                continue
            row.append(speedup(df_med.get(q), medians[eng].get(q)))
        sp_rows.append(tuple(row))

    # Completion summary
    comp_rows = []
    for eng in ENGINE_ORDER:
        s = summaries[eng]
        n_total = s["ok"] + s["fail"]
        ok_q = sum(1 for v in medians[eng].values() if v is not None)
        comp_rows.append((
            ENGINE_LABEL[eng],
            f"{ok_q} / {len(qs)}",
            fmt_ms(agg[eng]),
            s["first_error"] or "no failures",
        ))

    host = manifest.get("host", {})
    cpu = host.get("cpu", {})
    mem = host.get("memory", {})
    disk = host.get("disk", {})
    when = manifest.get("started_at") or datetime.now(timezone.utc).isoformat()

    neighbour_name, neighbour_file = BENCH_NEIGHBOURS[bench]

    md = f"""# {title} — DeltaForge benchmark

{subtitle}.

> One page per benchmark; full methodology in [methodology.md](methodology.md).
> Other benchmarks: [TPC-H](tpch.md) · [TPC-DS](tpcds.md) · [SSB](ssb.md) · [JOB](job.md) · [Index](index.md)

## Completion summary

{render_table(comp_rows, [("engine", "---"), ("queries completed", "---:"), ("warm-median (ms)", "---:"), ("first error", "---")])}

## Warm-median execution time (ms)

The headline number per (engine, query) is the median of 9 warm runs.
df numbers are server-reported `SHOW STATS ACTUAL.total_time_ms`;
DuckDB and Spark numbers are wall around the SELECT (views pre-registered
in untimed setup, attach cost excluded for every engine).

{render_table(perq_rows + [tuple(median_row)], cols)}

## Speedup vs DeltaForge (higher = faster than df)

{render_table(sp_rows, sp_cols)}

## Host

| | |
| --- | --- |
| CPU | {cpu.get("model", "?")} ({cpu.get("physical_cores", "?")} physical / {cpu.get("logical_threads", "?")} threads) |
| Memory | {mem.get("total_gib", "?")} GiB total |
| Cgroup | {host.get("cgroup", {}).get("cpu_cores", "?")} CPUs, {host.get("cgroup", {}).get("memory_mib", "?")} MiB |
| Disk | {disk.get("filesystem", "?")} on {disk.get("backing_device", "?")} — read {disk.get("read_mb_per_sec", "?")} MB/s, write {disk.get("write_mb_per_sec", "?")} MB/s |
| Virt | {host.get("virtualization", "?")} |
| Run started | {when} |
| Run id | `{results_dir.name}` |

## Engine versions

{render_table([(ENGINE_LABEL[e], manifest.get("engine_versions", {}).get(e, {}).get("version", "?")) for e in ENGINE_ORDER if e in (manifest.get("engine_versions") or {})], [("engine", "---"), ("version", "---")])}

## Reproducing this table

```bash
docker compose exec bench python data_gen/generate_{bench.replace("_read_delta", "")}_delta.py --scale 1
docker compose exec bench python bench_runner.py \\
    --scale 1 --engines df,duckdb,spark-default,spark-tuned \\
    --workloads {bench}
```

Raw per-step records: [`{results_dir.name}/raw/`](../{results_dir}/raw/).
Manifest: [`{results_dir.name}/manifest.json`](../{results_dir}/manifest.json).

---

Next: [{neighbour_name}]({neighbour_file}) · [Methodology](methodology.md) · [Index](index.md)
"""
    return md


def render_write_md(bench: str, results_dir: Path, manifest: dict) -> str:
    """Produce the write-bench publish markdown (throughput-oriented)."""
    title, subtitle = BENCH_TITLES[bench]
    n_rows = WRITE_ROW_COUNTS[bench]
    raw_dir = results_dir / "raw"

    # For writes, we want median wall_ms and rows/sec per engine.
    per_engine_stats: dict[str, dict] = {}
    for eng in ENGINE_ORDER:
        jsonl = raw_dir / f"{eng}.jsonl"
        if not jsonl.exists():
            per_engine_stats[eng] = None
            continue
        warm_wall: list[float] = []
        cold_wall: float | None = None
        ok = 0
        fail = 0
        first_err: str | None = None
        for line in jsonl.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            r = json.loads(line)
            sid = r.get("step_id") or ""
            if not sid or sid.endswith("setup") or sid.endswith("cleanup"):
                continue
            wall = r.get("wall_ms")
            # Treat any step with `error` set as a failure regardless of
            # wall_ms. Parse failures cost ~100-300ms of round-trip on
            # Spark; without this guard those would leak into the median
            # as if they were real writes. (This is the bug that made the
            # earlier Spark write number look 10x faster than df.)
            if wall is None or r.get("error") is not None:
                fail += 1
                if first_err is None:
                    first_err = (r.get("error") or "unknown").splitlines()[0][:140]
                continue
            ok += 1
            if r.get("cold"):
                cold_wall = wall
            else:
                warm_wall.append(wall)
        if not warm_wall:
            per_engine_stats[eng] = None
            continue
        med = statistics.median(warm_wall)
        per_engine_stats[eng] = {
            "warm_median_ms": med,
            "warm_min_ms":    min(warm_wall),
            "warm_max_ms":    max(warm_wall),
            "warm_p95_ms":    sorted(warm_wall)[max(0, int(0.95 * len(warm_wall)) - 1)],
            "cold_ms":        cold_wall,
            "rows_per_sec":   n_rows / (med / 1000.0),
            "first_error":    first_err,
            "ok": ok,
            "fail": fail,
        }

    # Headline table: engine, completion, warm-median, rows/sec, MB/sec (approx)
    rows_per_byte = 100  # ~100 bytes per row at the 9-column synthetic schema
    headline_rows = []
    for eng in ENGINE_ORDER:
        s = per_engine_stats.get(eng)
        if s is None:
            headline_rows.append((ENGINE_LABEL[eng], "skipped", "-", "-", "-", "engine not run"))
            continue
        ok_total = s["ok"]
        n_total = ok_total + s["fail"]
        headline_rows.append((
            ENGINE_LABEL[eng],
            f"{ok_total} / {n_total}",
            fmt_ms(s["warm_median_ms"]),
            f"{s['rows_per_sec'] / 1_000_000:.2f}M",
            f"{s['rows_per_sec'] * rows_per_byte / 1e6:.1f}",
            s["first_error"] or "ok",
        ))

    # Detailed stats: cold, warm-min, warm-median, warm-p95, warm-max
    detail_rows = []
    for eng in ENGINE_ORDER:
        s = per_engine_stats.get(eng)
        if s is None:
            continue
        detail_rows.append((
            ENGINE_LABEL[eng],
            fmt_ms(s["cold_ms"]),
            fmt_ms(s["warm_min_ms"]),
            fmt_ms(s["warm_median_ms"]),
            fmt_ms(s["warm_p95_ms"]),
            fmt_ms(s["warm_max_ms"]),
        ))

    # Speedup vs DeltaForge (rows/sec ratio)
    df_stats = per_engine_stats.get("df")
    speed_rows = []
    for eng in ENGINE_ORDER:
        if eng == "df":
            continue
        s = per_engine_stats.get(eng)
        if s is None or df_stats is None:
            speed_rows.append((ENGINE_LABEL[eng], "-"))
            continue
        ratio = s["rows_per_sec"] / df_stats["rows_per_sec"]
        speed_rows.append((ENGINE_LABEL[eng], f"{ratio:.2f}x"))

    host = manifest.get("host", {})
    cpu = host.get("cpu", {})
    mem = host.get("memory", {})
    disk = host.get("disk", {})
    when = manifest.get("started_at") or datetime.now(timezone.utc).isoformat()
    neighbour_name, neighbour_file = BENCH_NEIGHBOURS[bench]

    md = f"""# {title} — DeltaForge benchmark

{subtitle}. **{n_rows:,} rows written by each engine into its own plain
Delta directory from a deterministic synthetic source** (`generate_series`
on df, `range` on Spark, arithmetic-derived columns for 9 typed
columns). The measured timer covers the full `CTAS` statement.

> One page per benchmark; full methodology in [methodology.md](methodology.md).
> Other benchmarks: [TPC-H](tpch.md) · [TPC-DS](tpcds.md) · [SSB](ssb.md) · [JOB](job.md) · [Writes](writes.md) · [Index](index.md)

## Why DuckDB is not in this table

DuckDB's `delta` extension is **read-only**. There is no DuckDB code path
that writes a Delta-protocol table; the comparison is df vs Spark only.
DuckDB participates in every read benchmark in this suite.

## Throughput headline

{render_table(headline_rows, [("engine", "---"), ("runs completed", "---:"), ("warm-median (ms)", "---:"), ("rows/sec", "---:"), ("MB/sec (est)", "---:"), ("notes", "---")])}

MB/sec estimate uses ~100 bytes per row (the 9-column synthetic schema
averages out around there); the real on-disk Snappy-compressed parquet
size per row is smaller, so this number is closer to "logical row
throughput" than disk throughput.

## Wall-time distribution per engine

Wall-clock around the CTAS statement. 1 cold + 9 warm runs.

{render_table(detail_rows, [("engine", "---"), ("cold (ms)", "---:"), ("warm min (ms)", "---:"), ("warm median (ms)", "---:"), ("warm p95 (ms)", "---:"), ("warm max (ms)", "---:")])}

## Speedup vs DeltaForge (rows/sec ratio)

Values > 1.0x mean the engine wrote faster than df; values < 1.0x mean df was faster.

{render_table(speed_rows, [("engine", "---"), ("rows/sec ratio", "---:")])}

## Schema

9 columns covering 7 distinct types, all derived deterministically
from the row index `i`:

| Column | Type | Derivation |
| --- | --- | --- |
| id | BIGINT | i |
| customer_id | INT | `cast((i*13) % 10000 as int)` |
| order_date | DATE | `date_add(date '2024-01-01', cast(i % 365 as int))` |
| quantity | INT | `cast((i % 100) + 1 as int)` |
| unit_price | DECIMAL(10, 2) | `cast((i % 9999) / 100.0 as decimal(10, 2))` |
| discount | DOUBLE | `cast((i % 30) / 100.0 as double)` |
| region | STRING | 5-value `CASE` on `i % 5` |
| is_priority | BOOLEAN | `(i % 7) = 0` |
| notes | STRING | `concat('order_', lpad(cast(i % 10000 as varchar), 6, '0'))` |

Every engine produces byte-equivalent column content (modulo Delta writer
file layout); the only thing that differs across engines is the speed of
producing rows and persisting them as a Delta commit.

## Measurement contract for writes

Unlike the read benchmarks (where df is measured with `SHOW STATS
ACTUAL.total_time_ms` server-side), the write benchmark measures
**wall-clock around the CTAS for every engine**. df's wall includes
the CLI-to-server HTTP round-trip (~80-90 ms on localhost), which is a
small fraction of the multi-second CTAS but should be disclosed. In
exchange, every engine is on the same wall-clock contract: timer starts
before the SQL is issued, stops after the engine reports "done".

The measured SQL for each engine:

```sql
-- df
DROP DELTA TABLE IF EXISTS write_zone.bench.synth_fact WITH FILES;
CREATE DELTA TABLE write_zone.bench.synth_fact
    LOCATION '/workspace/data/synth_write/df/synth_fact' AS
SELECT <9 columns> FROM generate_series(0, {n_rows - 1}) AS t(i);

-- Spark (default and tuned, separate target paths)
CREATE OR REPLACE TABLE delta.`<path>` USING DELTA AS
SELECT <9 columns> FROM range(0, {n_rows}) AS t(i);
```

The Spark `CREATE OR REPLACE TABLE` is atomic; df's two-statement
script (drop + create) covers the same semantic. Drop overhead on df
is ~100-200 ms versus several seconds of CTAS, so the wall_ms penalty
is bounded.

## Host

| | |
| --- | --- |
| CPU | {cpu.get("model", "?")} ({cpu.get("physical_cores", "?")} physical / {cpu.get("logical_threads", "?")} threads) |
| Memory | {mem.get("total_gib", "?")} GiB total |
| Cgroup | {host.get("cgroup", {}).get("cpu_cores", "?")} CPUs, {host.get("cgroup", {}).get("memory_mib", "?")} MiB |
| Disk | {disk.get("filesystem", "?")} on {disk.get("backing_device", "?")} — read {disk.get("read_mb_per_sec", "?")} MB/s, write {disk.get("write_mb_per_sec", "?")} MB/s |
| Virt | {host.get("virtualization", "?")} |
| Run started | {when} |
| Run id | `{results_dir.name}` |

## Reproducing this table

```bash
docker compose exec bench python bench_runner.py \\
    --scale 1 --engines df,spark-default,spark-tuned \\
    --workloads synthetic_write_delta
```

Raw per-step records: [`{results_dir.name}/raw/`](../{results_dir}/raw/).
Manifest: [`{results_dir.name}/manifest.json`](../{results_dir}/manifest.json).

---

Next: [{neighbour_name}]({neighbour_file}) · [Methodology](methodology.md) · [Index](index.md)
"""
    return md


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--results-dir", required=True, type=Path)
    p.add_argument("--bench", required=True, choices=list(BENCH_TITLES.keys()))
    p.add_argument("--out", required=True, type=Path)
    args = p.parse_args()

    manifest = json.loads((args.results_dir / "manifest.json").read_text(encoding="utf-8"))
    if args.bench in WRITE_WORKLOADS:
        md = render_write_md(args.bench, args.results_dir, manifest)
    else:
        md = render_bench_md(args.bench, args.results_dir, manifest)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(md, encoding="utf-8")
    print(f"wrote {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
