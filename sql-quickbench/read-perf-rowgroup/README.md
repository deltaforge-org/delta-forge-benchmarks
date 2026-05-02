# read-perf-rowgroup

Ad-hoc bench that isolates **row-group quality** as a read-side performance variable. The headline question:

> If we hand DeltaForge's read engine the *same logical 5M rows* but with parquet files written by DuckDB instead of by DF, do the q1 / q2 / q3 read times in [../read-perf](../read-perf/) change?

If yes, the gap against DuckDB in `../read-perf/BASELINE.md` is largely a writer problem (row-group sizing, dictionary thresholds, page index, statistics) and not a reader problem. If no, the read engine itself is the bottleneck.

## What the suite does

1. DuckDB writes 5M rows of the standard dim_customer shape into `B:/odbc_df/df-demo/perf_test/dim_customer_duck/data.parquet` (same projection as `../write-perf/duckdb_copy.sql`, only the destination differs).
2. DeltaForge wraps that directory in place via `CONVERT TO DELTA` (writes a fresh `_delta_log` alongside the duck-authored parquet without rewriting any data) and registers it in the catalog as `pbi.bench.dim_customer_duck`.
3. For each of the three queries from [../read-perf](../read-perf/) we time three reads of the same logical data:

| Slot | Engine | Table | What's being measured |
| --- | --- | --- | --- |
| `df-on-df` | DeltaForge | `pbi.bench.dim_customer_insert` | DF reading parquet that DF wrote (the existing read-perf baseline) |
| `df-on-duck` | DeltaForge | `pbi.bench.dim_customer_duck` | DF reading parquet that DuckDB wrote, wrapped via CONVERT TO DELTA |
| `duck` | DuckDB | `dim_customer_duck/*.parquet` | DuckDB reading its own parquet (engine reference) |

`df-on-df` vs `df-on-duck` is the headline comparison: same DF read engine, same query, same auth path, same `_delta_log`-driven planning, only the underlying parquet changes. Any speedup on `df-on-duck` is attributable to row-group / page-level differences in the parquet itself.

## Run

```bash
./run.sh
```

```bash
NOTE="row-group hypothesis check" ./run.sh
WARM_RUNS=3 ./run.sh                       # 3 warm runs per slot
SKIP_SETUP=1 ./run.sh                      # skip DuckDB write + DF register, reuse existing dim_customer_duck
```

The runner primes the OS page cache once per query slot, then records `WARM_RUNS` warm runs (default 1) and appends to `results.csv`.

## Output

- `results.csv` grows by `3 * 3 * WARM_RUNS` rows per invocation (one per slot per query per warm run). Columns: `timestamp,git_sha,engine,table_source,query,run,seconds,note`.
- Per-query stdout line shows the `df-on-df / df-on-duck` ratio. Above 1.0 means DF reads duck-authored parquet faster than DF-authored parquet, i.e. the row-group hypothesis is supported for that query.

## Prerequisites

- `pbi.bench.dim_customer_insert` already exists. If not, run `../write-perf/run.sh` first.
- DuckDB CLI on `$DUCKDB` (default `/a/tmp/duckdb/duckdb.exe`).
- DeltaForge CLI on `$DF_CLI` (default `/a/delta-forge/target/release/delta-forge-cli.exe`).
- DF credentials from `$CRED_FILE` (default `A:/delta-forge/.deltaforge/democred.txt`) or `DF_USERNAME` / `DF_PASSWORD` env.

## Files

| File | Purpose |
| --- | --- |
| `duck_write.sql` | DuckDB script: clears the directory and writes 5M rows of dim_customer parquet. |
| `df_register.sql` | DF script: `UNREGISTER` (idempotent), `CONVERT TO DELTA`, `REGISTER TABLE`, count. |
| `df_q1_count.sql`, `df_q2_agg.sql`, `df_q3_topk.sql` | The three read-perf queries pointed at `pbi.bench.dim_customer_duck`. |
| `duck_q1_count.sql`, `duck_q2_agg.sql`, `duck_q3_topk.sql` | The same three queries pointed at the duck parquet directly. |
| `run.sh` | Orchestrator: setup once, then time the three slots per query. |

## Why CONVERT TO DELTA and not "just point DF at the parquet"

`CONVERT TO DELTA` keeps the read path identical to a normal DF managed table: query goes through the catalog, the `_delta_log` is consulted, statistics are read from the log, the same planner and the same parquet reader run. If we instead read the parquet via `CREATE EXTERNAL TABLE`, we'd be measuring a different code path and a different set of optimizations and the comparison against `pbi.bench.dim_customer_insert` would not be apples-to-apples.
