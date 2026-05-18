# JOB — DeltaForge benchmark

113 Join Order Benchmark queries (Leis et al. VLDB 2015) on the
21-table IMDB snapshot.

> **Results pending.** This run is currently in flight. The aggregator
> drops the populated page here when
> [`reports/build_published.py`](../reports/build_published.py)
> processes the `results/<timestamp>-<host>-publish-job/` directory.

> One page per benchmark; full methodology in [methodology.md](methodology.md).
> Other benchmarks: [TPC-H](tpch.md) · [TPC-DS](tpcds.md) · [SSB](ssb.md) · [JOB](job.md) · [Writes](writes.md) · [Index](index.md)

## What this measures

JOB is purpose-built to stress **query-optimizer cardinality
estimation**. The 113 queries were chosen by Leis et al. specifically
because they expose the gap between the cardinalities a planner
estimates and the cardinalities it actually encounters on a real
schema (IMDB, ~3.6 GB unpacked, fixed size with no scale factor).

Largest tables driving the join cost on this fixture:

- `cast_info` — 36,244,344 rows
- `movie_info` — 14,835,720 rows
- `movie_keyword` — 4,523,930 rows
- `name` — 4,167,491 rows
- `char_name` — 3,140,339 rows

## Reproducing

```bash
docker compose exec bench python data_gen/generate_job_delta.py
docker compose exec bench python bench_runner.py \
    --scale 1 --engines df,duckdb,spark-default,spark-tuned \
    --workloads job_read_delta
```

The data generator downloads `imdb.tgz` from the CWI mirror (~1.3 GB
compressed, ~3.6 GB unpacked) on first run, applies the JOB schema via
DuckDB, exports each of the 21 tables to parquet, then Spark writes
plain Delta. Subsequent runs reuse the cached tarball.

---

Next: [Writes](writes.md) · [Methodology](methodology.md) · [Index](index.md)
