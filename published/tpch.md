# TPC-H — DeltaForge benchmark

22 canonical TPC-H queries on the 8-table schema, SF=1.

> One page per benchmark; full methodology in [methodology.md](methodology.md).
> Other benchmarks: [TPC-H](tpch.md) · [TPC-DS](tpcds.md) · [SSB](ssb.md) · [JOB](job.md) · [Index](index.md)

## Completion summary

| engine | queries completed | warm-median (ms) | first error |
| --- | ---: | ---: | --- |
| DeltaForge | 22 / 22 | 255.44 | no failures |
| DuckDB | 22 / 22 | 173.12 | no failures |
| Spark (default) | 22 / 22 | 1,477.74 | no failures |
| Spark (tuned) | 21 / 22 | 1,527.79 | Py4JJavaError('An error occurred while calling o2096.collectToPython.\n', JavaObject id=o2097) |

## Warm-median execution time (ms)

The headline number per (engine, query) is the median of 9 warm runs.
df numbers are server-reported `SHOW STATS ACTUAL.total_time_ms`;
DuckDB and Spark numbers are wall around the SELECT (views pre-registered
in untimed setup, attach cost excluded for every engine).

| query | DeltaForge | DuckDB | Spark (default) | Spark (tuned) |
| --- | ---: | ---: | ---: | ---: |
| q01 | 479.20 | 189.18 | 2,750.44 | 2,593.72 |
| q02 | 167.84 | 147.56 | 1,714.15 | 1,291.67 |
| q03 | 288.96 | 180.48 | 1,507.46 | 1,744.64 |
| q04 | 229.32 | 140.21 | 1,175.88 | 1,527.79 |
| q05 | 383.61 | 188.16 | 2,346.87 | - |
| q06 | 152.67 | 86.46 | 333.61 | 316.18 |
| q07 | 286.60 | 202.08 | 1,798.81 | 1,673.40 |
| q08 | 297.87 | 241.58 | 2,038.35 | 1,722.26 |
| q09 | 382.68 | 298.78 | 2,414.01 | 1,844.77 |
| q10 | 317.54 | 271.79 | 1,962.45 | 1,855.07 |
| q11 | 101.93 | 109.29 | 1,306.33 | 1,213.71 |
| q12 | 287.02 | 132.72 | 988.73 | 1,320.47 |
| q13 | 188.84 | 173.23 | 1,175.60 | 1,723.19 |
| q14 | 153.66 | 139.11 | 595.11 | 589.17 |
| q15 | 239.45 | 108.94 | 1,352.43 | 1,153.82 |
| q16 | 162.56 | 154.02 | 1,244.48 | 1,209.29 |
| q17 | 271.43 | 173.01 | 1,602.09 | 2,506.71 |
| q18 | 617.00 | 219.62 | 2,658.12 | 2,649.64 |
| q19 | 222.93 | 170.38 | 663.65 | 684.83 |
| q20 | 221.44 | 187.50 | 1,448.02 | 1,203.50 |
| q21 | 526.15 | 381.75 | 3,244.01 | 2,965.08 |
| q22 | 108.66 | 130.17 | 1,415.45 | 1,439.54 |
| **median (across completed queries)** | **255.44** | **173.12** | **1,477.74** | **1,527.79** |

## Speedup vs DeltaForge (higher = faster than df)

| query | DuckDB | Spark (default) | Spark (tuned) |
| --- | ---: | ---: | ---: |
| q01 | 2.53x | 0.17x | 0.18x |
| q02 | 1.14x | 0.10x | 0.13x |
| q03 | 1.60x | 0.19x | 0.17x |
| q04 | 1.64x | 0.20x | 0.15x |
| q05 | 2.04x | 0.16x | - |
| q06 | 1.77x | 0.46x | 0.48x |
| q07 | 1.42x | 0.16x | 0.17x |
| q08 | 1.23x | 0.15x | 0.17x |
| q09 | 1.28x | 0.16x | 0.21x |
| q10 | 1.17x | 0.16x | 0.17x |
| q11 | 0.93x | 0.08x | 0.08x |
| q12 | 2.16x | 0.29x | 0.22x |
| q13 | 1.09x | 0.16x | 0.11x |
| q14 | 1.10x | 0.26x | 0.26x |
| q15 | 2.20x | 0.18x | 0.21x |
| q16 | 1.06x | 0.13x | 0.13x |
| q17 | 1.57x | 0.17x | 0.11x |
| q18 | 2.81x | 0.23x | 0.23x |
| q19 | 1.31x | 0.34x | 0.33x |
| q20 | 1.18x | 0.15x | 0.18x |
| q21 | 1.38x | 0.16x | 0.18x |
| q22 | 0.83x | 0.08x | 0.08x |

## Host

| | |
| --- | --- |
| CPU | 85 (18 physical / 36 threads) |
| Memory | ? GiB total |
| Cgroup | ? CPUs, ? MiB |
| Disk | ? on ? — read ? MB/s, write ? MB/s |
| Virt | {'container_hint': False, 'hypervisor_flag': True, 'wsl2': True} |
| Run started | 2026-05-18T16:05:14.494487+00:00 |
| Run id | `20260518T131418Z-8ae0556ef6b8-publish-tpch` |

## Engine versions

| engine | version |
| --- | --- |
| DeltaForge | ? |
| DuckDB | ? |
| Spark (default) | ? |
| Spark (tuned) | ? |

## Reproducing this table

```bash
docker compose exec bench python data_gen/generate_tpch_delta.py --scale 1
docker compose exec bench python bench_runner.py \
    --scale 1 --engines df,duckdb,spark-default,spark-tuned \
    --workloads tpch_read_delta
```

Raw per-step records: [`20260518T131418Z-8ae0556ef6b8-publish-tpch/raw/`](../results/20260518T131418Z-8ae0556ef6b8-publish-tpch/raw/).
Manifest: [`20260518T131418Z-8ae0556ef6b8-publish-tpch/manifest.json`](../results/20260518T131418Z-8ae0556ef6b8-publish-tpch/manifest.json).

---

Next: [TPC-DS](tpcds.md) · [Methodology](methodology.md) · [Index](index.md)
