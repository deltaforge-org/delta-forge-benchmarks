# Writes — DeltaForge benchmark

Synthetic 10M-row CTAS into plain Delta from deterministic in-memory generators. **10,000,000 rows written by each engine into its own plain
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

| engine | runs completed | warm-median (ms) | rows/sec | MB/sec (est) | notes |
| --- | ---: | ---: | ---: | ---: | --- |
| DeltaForge | 10 / 10 | 1,542.40 | 6.48M | 648.3 | ok |
| DuckDB | skipped | - | - | - | engine not run |
| Spark (default) | 10 / 10 | 6,640.44 | 1.51M | 150.6 | ok |
| Spark (tuned) | 10 / 10 | 6,227.78 | 1.61M | 160.6 | ok |

MB/sec estimate uses ~100 bytes per row (the 9-column synthetic schema
averages out around there); the real on-disk Snappy-compressed parquet
size per row is smaller, so this number is closer to "logical row
throughput" than disk throughput.

## Wall-time distribution per engine

Wall-clock around the CTAS statement. 1 cold + 9 warm runs.

| engine | cold (ms) | warm min (ms) | warm median (ms) | warm p95 (ms) | warm max (ms) |
| --- | ---: | ---: | ---: | ---: | ---: |
| DeltaForge | 1,398.22 | 1,496.61 | 1,542.40 | 1,569.82 | 1,608.44 |
| Spark (default) | 11,948.87 | 6,187.71 | 6,640.44 | 7,105.05 | 10,760.27 |
| Spark (tuned) | 6,537.51 | 6,094.24 | 6,227.78 | 6,388.93 | 6,785.25 |

## Speedup vs DeltaForge (rows/sec ratio)

Values > 1.0x mean the engine wrote faster than df; values < 1.0x mean df was faster.

| engine | rows/sec ratio |
| --- | ---: |
| DuckDB | - |
| Spark (default) | 0.23x |
| Spark (tuned) | 0.25x |

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
SELECT <9 columns> FROM generate_series(0, 9999999) AS t(i);

-- Spark (default and tuned, separate target paths)
CREATE OR REPLACE TABLE delta.`<path>` USING DELTA AS
SELECT <9 columns> FROM range(0, 10000000) AS t(i);
```

The Spark `CREATE OR REPLACE TABLE` is atomic; df's two-statement
script (drop + create) covers the same semantic. Drop overhead on df
is ~100-200 ms versus several seconds of CTAS, so the wall_ms penalty
is bounded.

## Host

| | |
| --- | --- |
| CPU | 85 (18 physical / 36 threads) |
| Memory | ? GiB total |
| Cgroup | ? CPUs, ? MiB |
| Disk | ? on ? — read ? MB/s, write ? MB/s |
| Virt | {'container_hint': False, 'hypervisor_flag': True, 'wsl2': True} |
| Run started | 2026-05-18T16:05:11.529162+00:00 |
| Run id | `20260518T155455Z-8ae0556ef6b8-publish-writes` |

## Reproducing this table

```bash
docker compose exec bench python bench_runner.py \
    --scale 1 --engines df,spark-default,spark-tuned \
    --workloads synthetic_write_delta
```

Raw per-step records: [`20260518T155455Z-8ae0556ef6b8-publish-writes/raw/`](../results/20260518T155455Z-8ae0556ef6b8-publish-writes/raw/).
Manifest: [`20260518T155455Z-8ae0556ef6b8-publish-writes/manifest.json`](../results/20260518T155455Z-8ae0556ef6b8-publish-writes/manifest.json).

---

Next: [TPC-H](tpch.md) · [Methodology](methodology.md) · [Index](index.md)
