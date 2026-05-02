# tpch

Industry-standard TPC-H benchmark suite. Materializes the dataset via DuckDB's
`tpch` extension, registers it as DeltaForge external tables, then runs the
same queries against both engines and produces a comparison report.

This is the "professional" baseline you cite when you want to compare DeltaForge
to DuckDB on a real workload, rather than the developer-flavoured probes in
`../write-perf/` and `../read-perf/`.

## Scope (current)

Four TPC-H queries that cover the main shapes you want to test:

| Query | Tables | Tests                                                                |
| ---   | ---    | ---                                                                  |
| Q1    | lineitem | single-table scan, predicate, group-by aggregation, ORDER BY       |
| Q3    | customer x orders x lineitem | 3-way join, predicate push-down, top-K  |
| Q6    | lineitem | selective range predicates, single-row aggregate                   |
| Q10   | customer x orders x lineitem x nation | 4-way join, wide group-by, top-K |

The full TPC-H is 22 queries. The four selected here all run on DataFusion
(and therefore DeltaForge) without correlated-subquery rewrites, so they
exercise the engine cleanly without a planner-features asterisk in the report.
Adding the remaining 18 is straightforward once we have the rewrites for the
correlated-subquery queries (Q2, Q4, Q17, Q20, Q22) committed.

## Setup

```bash
./setup.sh                   # SF1 (~1 GB raw, ~6M lineitem rows)
SCALE_FACTOR=10 ./setup.sh   # SF10 (~10 GB raw)
```

`setup.sh`:

1. Calls DuckDB's `tpch` extension to generate the dataset in memory at the requested scale factor.
2. Writes one `<table>/<table>.parquet` per table (snappy-compressed) under `B:/odbc_df/df-demo/tpch/sf<SF>/`.
3. Creates the `tpch` zone, the `tpch.sf<SF>` schema, and one external table per table in DeltaForge.

`CREATE OR REPLACE EXTERNAL TABLE` makes setup idempotent. Re-running
overwrites the parquet files and re-registers the catalog rows.

## Run

```bash
./run.sh                                # SF1, all 4 queries, 3 warm runs per engine
WARM_RUNS=5 ./run.sh                    # 5 warm runs per engine for tighter distributions
QUERIES="q01 q06" ./run.sh              # subset
NOTE="post X fix" ./run.sh              # tag rows in results.csv
SCALE_FACTOR=10 ./run.sh                # query the SF10 dataset (must have been set up)
```

Each query is primed once (so the OS page cache is warm and the engine has
seen the file) before any run is recorded. Failures are logged to
`results.csv` with `seconds=FAIL` so they show up in audit but do not
contaminate the timing aggregates.

## Report

```bash
python report.py
python report.py --note "post-morsel"
python report.py --scale-factor 10
python report.py --timestamp 2026-05-02T03:00:00
```

`report.py` filters rows from `results.csv` (most recent timestamp by default,
optionally narrowed by `--note` / `--scale-factor`) and writes:

- `report.md`  - tables: per-query mean / median / p95 / stdev, ratios at a glance.
- `report.png` - paired-bar chart with error bars (DF vs DuckDB), plus a ratio panel below the main chart.

The PNG is the chart you put in slides. The MD is the chart you cite in a PR description.

## Output schema

`results.csv` columns:

```
timestamp, git_sha, scale_factor, engine, query, run, seconds, note
```

One row per (engine, query, recorded run). `seconds=FAIL` rows mark queries
that errored on either engine; the report excludes them from aggregates and
makes the failure visible in the per-engine `n` column.

## Adding a query

1. Drop a SQL file under `queries/df/q<NN>.sql` and `queries/duck/q<NN>.sql`.
2. Both files get the `{{SF}}` placeholder substituted at run time, so reference tables as `tpch.sf{{SF}}.<name>` (DF) or `read_parquet('B:/odbc_df/df-demo/tpch/sf{{SF}}/<name>/*.parquet')` (DuckDB).
3. Validate the DF query against the DeltaForge parser (see `mcp__delta-forge__validate_syntax`) before committing. Validate the DuckDB query by running it once.
4. Add the new ID to the default `QUERIES` env in `run.sh`.

## Why DuckDB stays the reference

DuckDB is the most aggressive single-node parquet engine you can install in
five minutes, it can read our exact parquet files, and the gap between
"DeltaForge time" and "DuckDB time" on the same physical files is the cleanest
measure of how much engine work we still owe. Closing it is the goal; tracking
it suite by suite is how we know we are.
