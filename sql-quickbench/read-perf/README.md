# read-perf

Three queries x two engines, run against the same physical parquet files (the `pbi.bench.dim_customer_insert` table written by the [write-perf](../write-perf/) suite). Measures DeltaForge's read engine against DuckDB as a single-node reference.

| Query | What it tests |
| --- | --- |
| `q1_count` | `SELECT COUNT(*)` (metadata-only, both engines should answer from row-group metadata) |
| `q2_agg` | 4-column group-by aggregation over 5M rows |
| `q3_topk` | Filter + projection + top-K with order-by |

Each query exists as two scripts (one per engine) so the source binding stays explicit:

- DeltaForge reads the catalog table: `pbi.bench.dim_customer_insert`
- DuckDB reads the parquet files directly: `read_parquet('B:/.../dim_customer_insert/*.parquet')`

DuckDB does not understand Delta deletion vectors, but this table has none, so the bare parquet read is equivalent.

## Run

```bash
./run.sh
```

The runner primes the OS page cache once per query, then records `WARM_RUNS` warm runs (default 1). Tag a run with a note that ends up in `results.csv`:

```bash
NOTE="after predicate push-down fix" ./run.sh
```

For multi-run timing distributions:

```bash
WARM_RUNS=3 ./run.sh
```

## Output

- `results.csv` grows by `2 * 3 * WARM_RUNS` rows per invocation (one row per engine per query per warm run).
- `BASELINE.md` documents current expected numbers and any known issues this suite has surfaced.

## Prerequisites

This suite assumes `pbi.bench.dim_customer_insert` already exists. Run [write-perf](../write-perf/run.sh) first if it does not, or if you need a fresh write under your current build.
