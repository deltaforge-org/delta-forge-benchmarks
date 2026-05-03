# read-perf-10m

10M-row sibling of [read-perf](../read-perf/). Same three queries (q1_count, q2_agg, q3_topk), same engines (DeltaForge vs DuckDB), same projection (43 columns of dim_customer), but against `pbi.bench.dim_customer_insert_10m` so we can see whether the DF/DuckDB ratio scales with row count.

| | read-perf | read-perf-10m |
| --- | --- | --- |
| Rows | 5,000,000 | 10,000,000 |
| Table | `pbi.bench.dim_customer_insert` | `pbi.bench.dim_customer_insert_10m` |
| Storage | `B:/odbc_df/df-demo/perf_test/dim_customer_insert/` | `B:/odbc_df/df-demo/perf_test/dim_customer_insert_10m/` |

Both engines read the **same physical parquet files** (DuckDB via `read_parquet(...)` directly), so any difference is engine-side, not bytes.

## Run

```bash
./run.sh                      # writes data once if missing, then benches
SKIP_WRITE=1 ./run.sh         # bench only (assumes the 10M table already exists)
WARM_RUNS=3 ./run.sh          # 3 warm runs per query, more stable variance
NOTE="ratio at 10M" ./run.sh  # tag results.csv
```

The runner primes the OS page cache once per query (one DF + one DuckDB run), then records `WARM_RUNS` warm runs and appends to `results.csv` (one row per engine per query per warm run).

## Files

| File | Purpose |
| --- | --- |
| `df_write.sql` | DeltaForge: writes 10M rows of dim_customer into `pbi.bench.dim_customer_insert_10m` via INSERT INTO + `df_generate_table`. |
| `df_q1_count.sql`, `df_q2_agg.sql`, `df_q3_topk.sql` | The three queries against the catalog table. |
| `duck_q1_count.sql`, `duck_q2_agg.sql`, `duck_q3_topk.sql` | The same three queries against the same physical parquet files via `read_parquet`. |
| `run.sh` | Orchestrator: write-once + timed bench. |

## Why no separate DuckDB-write step

The 5M sibling exists to compare engine-on-bytes; it doesn't matter who wrote the bytes as long as both engines read the same ones. Writing twice (once via DF, once via DuckDB COPY) would let us also compare write paths, but that's what [write-perf](../write-perf/) is for. This bench is read-only by design.
