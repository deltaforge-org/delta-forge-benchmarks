# DeltaForge benchmark results

Published numbers from `delta-forge-benchmarks/`. Four standardized
read benchmarks against the same plain-Delta fixtures, four engines on
the same single-node host.

## Benchmarks in this set

### Reads (df + DuckDB + Spark default + Spark tuned)

| Benchmark | Standard | Source | Queries | Tables |
| --- | --- | --- | --: | --: |
| [TPC-H](tpch.md) | TPC body | [`tpch_read_delta.py`](../workloads/tpch_read_delta.py) | 22 | 8 |
| [TPC-DS](tpcds.md) | TPC body | [`tpcds_read_delta.py`](../workloads/tpcds_read_delta.py) | 99 | 24 |
| [SSB](ssb.md) | O'Neil et al. 2009 | [`ssb_read_delta.py`](../workloads/ssb_read_delta.py) | 13 | 5 |
| [JOB](job.md) | Leis et al. VLDB 2015 | [`job_read_delta.py`](../workloads/job_read_delta.py) | 113 | 21 |

### Writes (df + Spark default + Spark tuned; DuckDB read-only)

| Benchmark | Source | Rows written | Source format |
| --- | --- | --: | --- |
| [Writes](writes.md) | [`synthetic_write_delta.py`](../workloads/synthetic_write_delta.py) | 10,000,000 | Synthetic in-memory generator (no on-disk input) |

## Engines compared

Four engines reading the **same on-disk plain Delta tables** (no
deletion vectors, no column mapping, no row tracking):

- **DeltaForge** — native server + worker, query via `OPEN DELTA TABLE`
- **DuckDB** — in-process, `delta_scan(...)` via the `delta` extension
- **Spark (default)** — PySpark 4.0 + delta-spark 4.0, `local[*]`,
  4 GB driver, no AQE override (what `pip install pyspark` gives you)
- **Spark (tuned)** — same versions, 8 GB driver + 4 GB off-heap, AQE
  / DPP / runtime bloom filter / ANSI / CBO / Kryo / Arrow on. Full
  config in [`engines/spark_tuned_engine.py`](../engines/spark_tuned_engine.py).

## What you'll find on each page

| Section | What's there |
| --- | --- |
| Completion summary | Did the engine finish every query? Where did it fail? |
| Warm-median table | Per-query times in milliseconds. The headline number. |
| Speedup vs DeltaForge | Engine-by-engine ratio. >1.0x = faster than df. |
| Host | Exact hardware the bench ran on |
| Reproducing this table | The two commands a reviewer types to recreate the numbers |

## Methodology

See [methodology.md](methodology.md) for the full measurement contract:
plain Delta fixture protocol, per-engine setup paths (DuckDB views,
Spark views, df OPEN preamble), why df uses `SHOW STATS ACTUAL`
instead of raw `execution_time_ms`, what gets excluded from the
published number and why.

## Quick navigation

[Methodology](methodology.md) · [TPC-H](tpch.md) · [TPC-DS](tpcds.md) · [SSB](ssb.md) · [JOB](job.md) · [Writes](writes.md)
