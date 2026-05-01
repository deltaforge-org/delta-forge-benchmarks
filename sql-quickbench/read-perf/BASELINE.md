# read-perf baseline

What this suite measures: read-side engine performance against the same physical parquet files, with DuckDB as the reference. Source table is `pbi.bench.dim_customer_insert` (5M rows x 43 cols, snappy + dict, 224 MB across 3 files), produced by the post-fix INSERT path in [write-perf](../write-perf/).

DuckDB does not understand Delta deletion vectors, but this table has none, so reading the bare parquet files via `read_parquet('.../*.parquet')` is equivalent to a Delta read. That is what the duck-side scripts do.

## Queries

- **q1_count**: `SELECT COUNT(*)`. Pure metadata read; both engines should answer from row-group metadata without scanning data pages.
- **q2_agg**: `SELECT segment, COUNT(*), AVG(annual_income_usd), MAX(loyalty_points_balance), SUM(lifetime_revenue_usd) GROUP BY segment ORDER BY total_revenue DESC`. Reads 4 columns and aggregates into 5 groups. Tests columnar scan, decode, and aggregation.
- **q3_topk**: `SELECT customer_id, full_name, email, city, annual_income_usd WHERE region = 'NA' AND annual_income_usd > 200000 ORDER BY annual_income_usd DESC LIMIT 1000`. Tests predicate push-down (`region` filters to ~20% of rows), projection (5 columns), and top-K.

## Current numbers (warm cache, post-write-fix, 2026-05-01)

Box: Windows 11; one CLI invocation per query; first run primes the cache, second run is recorded. Wall-clock from the runner.

| Query | DF (s) | DuckDB (s) | DF / DuckDB |
| --- | ---: | ---: | ---: |
| q1_count | 1.36 | 0.35 | 3.9x |
| q2_agg | 13.83 | 0.47 | **29x** |
| q3_topk | 8.01 | 0.54 | **15x** |

These are wall-clock times from the runner, captured during the same session as the post-fix write-perf run. After q2 / q3 a control-plane restart (triggered by an unrelated dev-loop change, not a DF crash) interrupted a follow-up timing pass; the numbers above are from the runner's recorded warm pass before the restart.

DF "wall" includes ~700ms of fixed CLI auth + HTTP overhead per invocation (measured on q1, where the per-statement time reported by the CLI was 680ms vs the runner's 1.36s wall). Subtracting that overhead does not change the qualitative story: q2 and q3 are roughly 13-28x slower than DuckDB on the same physical files, which is much worse than expected for a DataFusion-backed engine.

## How to read a regression

Re-run `./run.sh` after a change. Each invocation appends two rows per query (one DF, one DuckDB). Compare the new run's `df` row against the baseline above.

- DF time +20% or more, DuckDB stable: regression on our side; investigate.
- DF and DuckDB both up: box is busy, re-run.
- DF down: a fix; verify the row count of the q2 / q3 results still matches.

DuckDB is the control. If DuckDB's q1/q2/q3 numbers move significantly the box is noisy and the run should be repeated.

## What we already think is wrong

Recorded so the next person looking at this file does not start from zero:

- Even at q1 (which DuckDB answers from parquet metadata in 0.35s), DF takes 1.36s. ~700ms is fixed CLI auth + control-plane round-trip overhead, but the remaining ~660ms is more than this query should cost.
- q2 at 13.8s for a 4-column group-by over 5M rows is far above DataFusion's typical ceiling for this shape (sub-second); result is 5 rows, so it is not a result-transport issue. Aggregation push-down or a redundant scan is the likely culprit.
- q3 at 8s for a predicate that filters to ~20% of rows suggests parquet predicate / projection push-down may not be reaching the row-group level. Recent commit `cb63c5beb` added parallel decode and page-index support; worth checking whether the page index is actually being consulted on this scan.

These are theories, not findings. The next pass should run the queries with `EXPLAIN` and timing instrumentation enabled to confirm where the cost is.

## Why DuckDB stays in the suite

Same reason as `write-perf`: it is a credible single-node parquet engine on the same hardware, it can read our exact files, and tracking it next to our numbers means a regression on our side that does not show on DuckDB is unambiguously in our code.
