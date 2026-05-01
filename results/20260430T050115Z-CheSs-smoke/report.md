# Bench run report

## Run context

- captured_at_utc: `2026-04-30T05:01:15+00:00`
- hostname:        `CheSs`
- cpu_model:       `Intel(R) Core(TM) i9-7980XE CPU @ 2.60GHz`
- mem_total_kb:    `32697172`
- kernel:          `6.6.87.2-microsoft-standard-WSL2`
- python:          `3.10.12`
- scale:           SF=1
- engines:         `['spark-default']`
- workloads:       `['tpch_read']`

## Cold-start (reported separately, NOT folded into query times)

| engine | session_ready_ms | first_query_ms |
|---|--:|--:|
| `spark-default` | 8537.8 | 2926.8 |

## Correctness

All cross-engine result hashes agree where both engines ran the same step. No disagreements detected.

## Workload: `tpch_read`

| step | engine | cold_median_ms | warm_median_ms | warm_p95_ms | warm_mean_ms | warm_stddev_ms | warm_n | failures |
|---|---|--:|--:|--:|--:|--:|--:|--:|
| `q01` | `spark-default` | 4438.0 | 2635.3 | 2889.5 | 2661.7 | 145.9 | 9 | 0 |
| `q02` | `spark-default` | 3265.6 | 1538.1 | 1806.3 | 1578.8 | 151.3 | 9 | 0 |
| `q03` | `spark-default` | 2573.6 | 2005.8 | 2171.2 | 2012.0 | 119.2 | 9 | 0 |
| `q04` | `spark-default` | 2303.3 | 1551.6 | 1786.3 | 1571.6 | 176.6 | 9 | 0 |
| `q05` | `spark-default` | 3286.6 | 2665.8 | 2956.0 | 2719.1 | 146.3 | 9 | 0 |
| `q06` | `spark-default` | 892.9 | 323.1 | 478.0 | 350.0 | 79.2 | 9 | 0 |
| `q07` | `spark-default` | 3420.3 | 2177.2 | 2333.5 | 2188.0 | 101.9 | 9 | 0 |
| `q08` | `spark-default` | 3792.7 | 2560.6 | 2599.7 | 2533.5 | 56.9 | 9 | 0 |
| `q09` | `spark-default` | 3496.6 | 2385.6 | 2528.4 | 2378.4 | 114.5 | 9 | 0 |
| `q10` | `spark-default` | 3469.7 | 2431.1 | 2804.9 | 2416.4 | 285.2 | 9 | 0 |
| `q11` | `spark-default` | 2602.8 | 1208.5 | 1343.0 | 1230.1 | 94.9 | 9 | 0 |
| `q12` | `spark-default` | 2274.6 | 1815.3 | 1894.5 | 1765.3 | 140.0 | 9 | 0 |
| `q13` | `spark-default` | 2007.4 | 1032.9 | 1172.4 | 1035.8 | 88.7 | 9 | 0 |
| `q14` | `spark-default` | 1230.9 | 601.6 | 678.4 | 597.4 | 56.6 | 9 | 0 |
| `q15` | `spark-default` | 2954.8 | 2732.3 | 2899.5 | 2665.2 | 296.1 | 9 | 0 |
| `q16` | `spark-default` | 2198.6 | 1226.4 | 1415.6 | 1258.7 | 105.1 | 9 | 0 |
| `q17` | `spark-default` | 3179.4 | 2463.7 | 2619.2 | 2460.8 | 128.6 | 9 | 0 |
| `q18` | `spark-default` | 4087.7 | 3198.5 | 3260.4 | 3149.4 | 114.7 | 9 | 0 |
| `q19` | `spark-default` | 1826.1 | 575.3 | 700.3 | 600.2 | 57.8 | 9 | 0 |
| `q20` | `spark-default` | 2605.0 | 1612.2 | 1742.7 | 1603.0 | 104.5 | 9 | 0 |
| `q21` | `spark-default` | 5305.6 | 3947.8 | 4394.4 | 3996.1 | 306.9 | 9 | 0 |
| `q22` | `spark-default` | 1948.8 | 1196.9 | 1286.6 | 1203.6 | 57.8 | 9 | 0 |

