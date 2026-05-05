# index-perf — measure read speedup from a row-level index

This benchmark runs the same four SQL queries against a 10-million-row Delta
table **with** and **without** a row-level index on `customer_id`, then
reports the speedup ratio per query.

The bench **reuses** the existing `pbi.bench.dim_customer_insert_10m` table
written by `read-perf-10m/df_write.sql`. Run that bench first if the table
isn't there yet.

## Modes compared

Three modes run the same four queries against the same physical bytes:

| Mode | What it does |
| --- | --- |
| `df_idx` | DeltaForge with the row-level `.dlef` index engaged |
| `df_no` | DeltaForge without the index — file-stats pruning + column scan |
| `duck` | DuckDB `read_parquet()` on the underlying files; no secondary index |

DuckDB is the external reference: a high-quality columnar engine without
any row-level secondary index. The `df_idx` vs `duck` ratio is the
"closest fair comparison" — same physical bytes, different access strategies.

## Queries

| Label | Predicate | Expected rows | Index expected to engage? |
| --- | --- | --- | --- |
| `point` | `customer_id = 5000000` | 1 | yes (NDV ~10M, 1/NDV is tiny) |
| `in_list` | `customer_id IN (1, 1000000, 5000000, 9999999, 12345)` | 5 | yes |
| `narrow_range` | `customer_id BETWEEN 5000000 AND 5000099` | 100 | yes |
| `medium_range` | `customer_id BETWEEN 1000000 AND 1099999` | 100000 | **no** (1% selectivity exceeds cost threshold; selector falls back) |

The fourth query is the sanity check: it confirms the cost-based selector
correctly declines to use the index when the result set is large enough that
random-pointer fetching would lose to a column scan.

## What the speedup measures

Wall-clock time of the CLI invocation, including DataFusion plan + execute.
This is end-to-end as a user would experience it. The bench primes the OS /
object-store cache with one priming run before each measured run, so we are
measuring **warm-cache** performance — the realistic case for service-desk
or interactive query workloads.

To measure cold-cache, set `DROP_OS_CACHE=1` (Linux only; uses
`/proc/sys/vm/drop_caches`).

## Usage

```sh
./run.sh                    # 1 warm run per query, with and without index
WARM_RUNS=3 ./run.sh        # 3 warm runs per query
SKIP_BUILD_INDEX=1 ./run.sh # use existing index if already built
NOTE="rebased onto X" ./run.sh
```

Results are appended to `results.csv` with the schema:

```csv
timestamp, git_sha, mode, query, run, seconds, note
```

where `mode` is one of `no_index`, `with_index`, or `duck`.

The script also prints a side-by-side ratio table to stdout, like:

```text
  query            run  df_idx(s)   df_no(s)    duck(s)     vs df_no    vs duck
  ---------------  ---  ----------  ----------  ----------  ----------  ----------
  point            1    0.041       0.812       0.385       19.80x      9.39x
  in_list          1    0.063       0.821       0.402       13.03x      6.38x
  narrow_range     1    0.087       0.834       0.397       9.59x       4.56x
  medium_range     1    0.847       0.840       0.418       0.99x       0.49x
```

## What it doesn't measure

- Insert / update / merge throughput with auto-update on (separate concern).
- REBUILD INDEX cost (covered by a different bench).
- Cold-start storage latency (use `DROP_OS_CACHE=1`).
- Multi-index queries on the same table (one index per run; rerun with
  different `df_create_index.sql` to compare composite vs single).
