# JOB — DeltaForge benchmark

113 Join Order Benchmark queries (Leis et al. VLDB 2015) on the 21-table IMDB snapshot.

> One page per benchmark; full methodology in [methodology.md](methodology.md).
> Other benchmarks: [TPC-H](tpch.md) · [TPC-DS](tpcds.md) · [SSB](ssb.md) · [JOB](job.md) · [Index](index.md)

## Completion summary

| engine | queries completed | warm-median (ms) | first error |
| --- | ---: | ---: | --- |
| DeltaForge | 113 / 113 | 975.54 | no failures |
| DuckDB | 113 / 113 | 631.63 | no failures |
| Spark (default) | 21 / 113 | n/a (crashed) | Py4JJavaError on q06d; JVM died, every subsequent query got ConnectionRefused |
| Spark (tuned) | 0 / 113 | n/a (crashed) | engine failed to start |

> **No headline median for Spark on JOB.** Spark default's JVM died
> partway through, having completed only 21 of 113 queries (all the
> light early ones in the alphabetical order before the join cascade
> on `name` and `title` started). Spark tuned never reached engine
> ready. Computing a median across the 21 successful queries would
> overstate Spark performance because those 21 are a biased early
> subset, not a random sample. The per-query table below shows what
> data we have, by query, but does not aggregate it into a Spark
> headline number.

## Warm-median execution time (ms)

The headline number per (engine, query) is the median of 9 warm runs.
df numbers are server-reported `SHOW STATS ACTUAL.total_time_ms`;
DuckDB and Spark numbers are wall around the SELECT (views pre-registered
in untimed setup, attach cost excluded for every engine).

| query | DeltaForge | DuckDB | Spark (default) | Spark (tuned) |
| --- | ---: | ---: | ---: | ---: |
| q01a | 236.95 | 155.45 | 2,416.14 | - |
| q01b | 257.41 | 172.31 | 1,734.81 | - |
| q01c | 205.89 | 161.54 | 1,694.31 | - |
| q01d | 271.32 | 181.94 | 1,822.64 | - |
| q02a | 299.12 | 198.15 | 2,195.70 | - |
| q02b | 272.62 | 196.61 | 2,040.96 | - |
| q02c | 229.48 | 125.83 | 1,636.06 | - |
| q02d | 307.87 | 249.94 | 2,240.28 | - |
| q03a | 733.77 | 696.20 | 1,990.24 | - |
| q03b | 654.59 | 556.32 | 1,690.26 | - |
| q03c | 738.76 | 775.83 | 2,011.00 | - |
| q04a | 350.50 | 247.91 | 2,017.81 | - |
| q04b | 269.70 | 175.73 | 1,539.88 | - |
| q04c | 350.72 | 272.18 | 2,173.35 | - |
| q05a | 660.30 | 558.69 | 1,716.32 | - |
| q05b | 592.26 | 482.25 | 1,592.52 | - |
| q05c | 748.36 | 767.42 | 2,150.56 | - |
| q06a | 920.11 | 347.92 | 9,448.49 | - |
| q06b | 930.94 | 330.69 | 8,602.71 | - |
| q06c | 664.69 | 291.13 | 8,570.45 | - |
| q06d | 1,057.34 | 394.87 | 9,228.88 | - |
| q06e | 956.10 | 347.18 | - | - |
| q06f | 1,227.85 | 709.11 | - | - |
| q07a | 544.93 | 565.94 | - | - |
| q07b | 485.57 | 447.67 | - | - |
| q07c | 1,203.60 | 898.23 | - | - |
| q08a | 726.32 | 487.05 | - | - |
| q08b | 771.31 | 428.17 | - | - |
| q08c | 2,103.95 | 2,066.47 | - | - |
| q08d | 1,871.21 | 648.03 | - | - |
| q09a | 987.13 | 822.38 | - | - |
| q09b | 907.16 | 684.30 | - | - |
| q09c | 1,005.22 | 707.74 | - | - |
| q09d | 1,062.98 | 1,053.85 | - | - |
| q10a | 771.27 | 539.77 | - | - |
| q10b | 925.82 | 434.68 | - | - |
| q10c | 1,058.66 | 1,070.67 | - | - |
| q11a | 379.76 | 187.82 | - | - |
| q11b | 404.73 | 167.64 | - | - |
| q11c | 419.99 | 224.66 | - | - |
| q11d | 453.31 | 303.67 | - | - |
| q12a | 770.49 | 367.81 | - | - |
| q12b | 750.54 | 401.69 | - | - |
| q12c | 818.58 | 404.69 | - | - |
| q13a | 810.93 | 467.55 | - | - |
| q13b | 1,211.86 | 308.03 | - | - |
| q13c | 1,190.62 | 286.32 | - | - |
| q13d | 1,264.38 | 483.79 | - | - |
| q14a | 960.19 | 407.43 | - | - |
| q14b | 870.91 | 300.29 | - | - |
| q14c | 933.34 | 489.03 | - | - |
| q15a | 1,179.33 | 640.99 | - | - |
| q15b | 1,153.23 | 622.34 | - | - |
| q15c | 1,457.55 | 667.29 | - | - |
| q15d | 572.55 | 511.65 | - | - |
| q16a | 1,566.29 | 472.13 | - | - |
| q16b | 4,808.77 | 806.85 | - | - |
| q16c | 2,670.82 | 697.04 | - | - |
| q16d | 2,302.61 | 631.63 | - | - |
| q17a | 1,155.40 | 874.60 | - | - |
| q17b | 1,173.87 | 436.71 | - | - |
| q17c | 1,185.51 | 447.37 | - | - |
| q17d | 1,188.57 | 488.38 | - | - |
| q17e | 1,343.15 | 775.15 | - | - |
| q17f | 1,287.19 | 738.95 | - | - |
| q18a | 1,930.73 | 801.25 | - | - |
| q18b | 1,454.95 | 891.33 | - | - |
| q18c | 1,590.81 | 852.85 | - | - |
| q19a | 1,684.11 | 1,081.29 | - | - |
| q19b | 1,578.38 | 614.83 | - | - |
| q19c | 1,772.08 | 1,225.80 | - | - |
| q19d | 1,829.29 | 1,205.86 | - | - |
| q20a | 923.33 | 646.21 | - | - |
| q20b | 952.27 | 653.88 | - | - |
| q20c | 924.05 | 648.40 | - | - |
| q21a | 931.04 | 698.63 | - | - |
| q21b | 917.42 | 618.52 | - | - |
| q21c | 1,046.92 | 733.09 | - | - |
| q22a | 967.04 | 571.40 | - | - |
| q22b | 975.88 | 532.44 | - | - |
| q22c | 975.54 | 802.72 | - | - |
| q22d | 1,095.87 | 1,120.88 | - | - |
| q23a | 1,406.33 | 657.24 | - | - |
| q23b | 1,188.93 | 567.81 | - | - |
| q23c | 1,421.23 | 699.63 | - | - |
| q24a | 1,756.94 | 1,270.51 | - | - |
| q24b | 1,745.66 | 834.86 | - | - |
| q25a | 1,730.32 | 1,046.89 | - | - |
| q25b | 1,707.49 | 663.52 | - | - |
| q25c | 1,756.84 | 1,153.66 | - | - |
| q26a | 910.35 | 730.61 | - | - |
| q26b | 837.94 | 694.72 | - | - |
| q26c | 1,000.56 | 711.51 | - | - |
| q27a | 915.18 | 768.79 | - | - |
| q27b | 908.45 | 768.57 | - | - |
| q27c | 987.80 | 874.60 | - | - |
| q28a | 1,005.24 | 628.06 | - | - |
| q28b | 944.06 | 557.41 | - | - |
| q28c | 1,132.51 | 644.30 | - | - |
| q29a | 1,719.03 | 785.41 | - | - |
| q29b | 1,565.82 | 793.66 | - | - |
| q29c | 1,928.78 | 1,237.93 | - | - |
| q30a | 1,553.86 | 1,075.24 | - | - |
| q30b | 1,529.86 | 826.65 | - | - |
| q30c | 1,553.17 | 1,432.78 | - | - |
| q31a | 1,723.38 | 951.75 | - | - |
| q31b | 1,660.11 | 837.39 | - | - |
| q31c | 1,784.25 | 1,000.09 | - | - |
| q32a | 240.80 | 228.83 | - | - |
| q32b | 280.28 | 263.07 | - | - |
| q33a | 535.96 | 580.53 | - | - |
| q33b | 500.28 | 487.78 | - | - |
| q33c | 531.48 | 642.48 | - | - |
| **median (across completed queries)** | **975.54** | **631.63** | **n/a (crashed at q06d)** | **n/a (engine failed)** |

## Speedup vs DeltaForge (higher = faster than df)

| query | DuckDB | Spark (default) | Spark (tuned) |
| --- | ---: | ---: | ---: |
| q01a | 1.52x | 0.10x | - |
| q01b | 1.49x | 0.15x | - |
| q01c | 1.27x | 0.12x | - |
| q01d | 1.49x | 0.15x | - |
| q02a | 1.51x | 0.14x | - |
| q02b | 1.39x | 0.13x | - |
| q02c | 1.82x | 0.14x | - |
| q02d | 1.23x | 0.14x | - |
| q03a | 1.05x | 0.37x | - |
| q03b | 1.18x | 0.39x | - |
| q03c | 0.95x | 0.37x | - |
| q04a | 1.41x | 0.17x | - |
| q04b | 1.53x | 0.18x | - |
| q04c | 1.29x | 0.16x | - |
| q05a | 1.18x | 0.38x | - |
| q05b | 1.23x | 0.37x | - |
| q05c | 0.98x | 0.35x | - |
| q06a | 2.64x | 0.10x | - |
| q06b | 2.82x | 0.11x | - |
| q06c | 2.28x | 0.08x | - |
| q06d | 2.68x | 0.11x | - |
| q06e | 2.75x | - | - |
| q06f | 1.73x | - | - |
| q07a | 0.96x | - | - |
| q07b | 1.08x | - | - |
| q07c | 1.34x | - | - |
| q08a | 1.49x | - | - |
| q08b | 1.80x | - | - |
| q08c | 1.02x | - | - |
| q08d | 2.89x | - | - |
| q09a | 1.20x | - | - |
| q09b | 1.33x | - | - |
| q09c | 1.42x | - | - |
| q09d | 1.01x | - | - |
| q10a | 1.43x | - | - |
| q10b | 2.13x | - | - |
| q10c | 0.99x | - | - |
| q11a | 2.02x | - | - |
| q11b | 2.41x | - | - |
| q11c | 1.87x | - | - |
| q11d | 1.49x | - | - |
| q12a | 2.09x | - | - |
| q12b | 1.87x | - | - |
| q12c | 2.02x | - | - |
| q13a | 1.73x | - | - |
| q13b | 3.93x | - | - |
| q13c | 4.16x | - | - |
| q13d | 2.61x | - | - |
| q14a | 2.36x | - | - |
| q14b | 2.90x | - | - |
| q14c | 1.91x | - | - |
| q15a | 1.84x | - | - |
| q15b | 1.85x | - | - |
| q15c | 2.18x | - | - |
| q15d | 1.12x | - | - |
| q16a | 3.32x | - | - |
| q16b | 5.96x | - | - |
| q16c | 3.83x | - | - |
| q16d | 3.65x | - | - |
| q17a | 1.32x | - | - |
| q17b | 2.69x | - | - |
| q17c | 2.65x | - | - |
| q17d | 2.43x | - | - |
| q17e | 1.73x | - | - |
| q17f | 1.74x | - | - |
| q18a | 2.41x | - | - |
| q18b | 1.63x | - | - |
| q18c | 1.87x | - | - |
| q19a | 1.56x | - | - |
| q19b | 2.57x | - | - |
| q19c | 1.45x | - | - |
| q19d | 1.52x | - | - |
| q20a | 1.43x | - | - |
| q20b | 1.46x | - | - |
| q20c | 1.43x | - | - |
| q21a | 1.33x | - | - |
| q21b | 1.48x | - | - |
| q21c | 1.43x | - | - |
| q22a | 1.69x | - | - |
| q22b | 1.83x | - | - |
| q22c | 1.22x | - | - |
| q22d | 0.98x | - | - |
| q23a | 2.14x | - | - |
| q23b | 2.09x | - | - |
| q23c | 2.03x | - | - |
| q24a | 1.38x | - | - |
| q24b | 2.09x | - | - |
| q25a | 1.65x | - | - |
| q25b | 2.57x | - | - |
| q25c | 1.52x | - | - |
| q26a | 1.25x | - | - |
| q26b | 1.21x | - | - |
| q26c | 1.41x | - | - |
| q27a | 1.19x | - | - |
| q27b | 1.18x | - | - |
| q27c | 1.13x | - | - |
| q28a | 1.60x | - | - |
| q28b | 1.69x | - | - |
| q28c | 1.76x | - | - |
| q29a | 2.19x | - | - |
| q29b | 1.97x | - | - |
| q29c | 1.56x | - | - |
| q30a | 1.45x | - | - |
| q30b | 1.85x | - | - |
| q30c | 1.08x | - | - |
| q31a | 1.81x | - | - |
| q31b | 1.98x | - | - |
| q31c | 1.78x | - | - |
| q32a | 1.05x | - | - |
| q32b | 1.07x | - | - |
| q33a | 0.92x | - | - |
| q33b | 1.03x | - | - |
| q33c | 0.83x | - | - |

## Host

| | |
| --- | --- |
| CPU | 85 (18 physical / 36 threads) |
| Memory | ? GiB total |
| Cgroup | ? CPUs, ? MiB |
| Disk | ? on ? — read ? MB/s, write ? MB/s |
| Virt | {'container_hint': False, 'hypervisor_flag': True, 'wsl2': True} |
| Run started | 2026-05-18T17:41:04.416936+00:00 |
| Run id | `20260518T160416Z-8ae0556ef6b8-publish-job` |

## Engine versions

| engine | version |
| --- | --- |
| DeltaForge | ? |
| DuckDB | ? |
| Spark (default) | ? |

## Reproducing this table

```bash
docker compose exec bench python data_gen/generate_job_delta.py --scale 1
docker compose exec bench python bench_runner.py \
    --scale 1 --engines df,duckdb,spark-default,spark-tuned \
    --workloads job_read_delta
```

Raw per-step records: [`20260518T160416Z-8ae0556ef6b8-publish-job/raw/`](../results/20260518T160416Z-8ae0556ef6b8-publish-job/raw/).
Manifest: [`20260518T160416Z-8ae0556ef6b8-publish-job/manifest.json`](../results/20260518T160416Z-8ae0556ef6b8-publish-job/manifest.json).

---

Next: [Writes](writes.md) · [Methodology](methodology.md) · [Index](index.md)
