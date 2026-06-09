# SSB — DeltaForge benchmark

13 Star Schema Benchmark queries (O'Neil et al. 2009) on a 5-table star, SF=1.

> One page per benchmark; full methodology in [methodology.md](methodology.md).
> Other benchmarks: [TPC-H](tpch.md) · [TPC-DS](tpcds.md) · [SSB](ssb.md) · [JOB](job.md) · [Index](index.md)

## Completion summary

| engine | queries completed | warm-median (ms) | first error |
| --- | ---: | ---: | --- |
| DeltaForge | 13 / 13 | 190.57 | no failures |
| DuckDB | 13 / 13 | 75.46 | no failures |
| Spark (default) | 13 / 13 | 684.63 | no failures |
| Spark (tuned) | 13 / 13 | 628.23 | no failures |

## Warm-median execution time (ms)

The headline number per (engine, query) is the median of 9 warm runs.
df numbers are server-reported `SHOW STATS ACTUAL.total_time_ms`;
DuckDB and Spark numbers are wall around the SELECT (views pre-registered
in untimed setup, attach cost excluded for every engine).

| query | DeltaForge | DuckDB | Spark (default) | Spark (tuned) |
| --- | ---: | ---: | ---: | ---: |
| q11 | 145.16 | 92.54 | 703.98 | 521.35 |
| q12 | 145.03 | 81.93 | 596.83 | 503.64 |
| q13 | 138.07 | 82.45 | 581.44 | 540.57 |
| q21 | 190.57 | 57.42 | 698.58 | 646.13 |
| q22 | 194.49 | 52.06 | 648.96 | 609.13 |
| q23 | 191.46 | 55.63 | 636.16 | 594.73 |
| q31 | 297.67 | 186.09 | 1,110.50 | 1,013.32 |
| q32 | 137.18 | 47.22 | 684.63 | 617.72 |
| q33 | 113.62 | 54.90 | 662.92 | 649.46 |
| q34 | 136.64 | 75.46 | 671.33 | 628.23 |
| q41 | 324.10 | 78.91 | 809.87 | 752.99 |
| q42 | 207.40 | 118.52 | 788.48 | 801.00 |
| q43 | 257.76 | 66.26 | 782.04 | 770.93 |
| **median (across completed queries)** | **190.57** | **75.46** | **684.63** | **628.23** |

## Speedup vs DeltaForge (higher = faster than df)

| query | DuckDB | Spark (default) | Spark (tuned) |
| --- | ---: | ---: | ---: |
| q11 | 1.57x | 0.21x | 0.28x |
| q12 | 1.77x | 0.24x | 0.29x |
| q13 | 1.67x | 0.24x | 0.26x |
| q21 | 3.32x | 0.27x | 0.29x |
| q22 | 3.74x | 0.30x | 0.32x |
| q23 | 3.44x | 0.30x | 0.32x |
| q31 | 1.60x | 0.27x | 0.29x |
| q32 | 2.91x | 0.20x | 0.22x |
| q33 | 2.07x | 0.17x | 0.17x |
| q34 | 1.81x | 0.20x | 0.22x |
| q41 | 4.11x | 0.40x | 0.43x |
| q42 | 1.75x | 0.26x | 0.26x |
| q43 | 3.89x | 0.33x | 0.33x |

## Host

| | |
| --- | --- |
| CPU | 85 (18 physical / 36 threads) |
| Memory | ? GiB total |
| Cgroup | ? CPUs, ? MiB |
| Disk | ? on ? — read ? MB/s, write ? MB/s |
| Virt | {'container_hint': False, 'hypervisor_flag': True, 'wsl2': True} |
| Run started | 2026-05-18T16:05:13.043059+00:00 |
| Run id | `20260518T130908Z-8ae0556ef6b8-publish-ssb` |

## Engine versions

| engine | version |
| --- | --- |
| DeltaForge | ? |
| DuckDB | ? |
| Spark (default) | ? |
| Spark (tuned) | ? |

## Reproducing this table

```bash
docker compose exec bench python data_gen/generate_ssb_delta.py --scale 1
docker compose exec bench python bench_runner.py \
    --scale 1 --engines df,duckdb,spark-default,spark-tuned \
    --workloads ssb_read_delta
```

Raw per-step records: [`20260518T130908Z-8ae0556ef6b8-publish-ssb/raw/`](../results/20260518T130908Z-8ae0556ef6b8-publish-ssb/raw/).
Manifest: [`20260518T130908Z-8ae0556ef6b8-publish-ssb/manifest.json`](../results/20260518T130908Z-8ae0556ef6b8-publish-ssb/manifest.json).

---

Next: [JOB](job.md) · [Methodology](methodology.md) · [Index](index.md)
