# DeltaForge Benchmarks

A small catalog of perf scripts we re-run after engine changes to spot regressions.

Each subdirectory is one benchmark suite with:

- The SQL scripts that drive the workload
- A `run.sh` that executes the whole suite, times it, and appends one row per script to `results.csv`
- A `BASELINE.md` describing the current expected numbers, the box they were measured on, and any open issues the suite has surfaced

Re-run the suite when you change anything in the relevant write/read path. Compare the new `results.csv` row against `BASELINE.md` and the prior rows. If something regressed by more than the noise floor (call it 10%), open a fix before merging.

## Suites

| Suite                       | What it measures                                                                                              |
| --------------------------- | ------------------------------------------------------------------------------------------------------------- |
| [write-perf/](write-perf/)  | DeltaForge CTAS vs INSERT vs DuckDB COPY for a 5M-row x 43-col dim_customer shape                             |
| [read-perf/](read-perf/)    | DeltaForge vs DuckDB on count / group-by aggregation / filtered top-K against the table written by write-perf |

Add a new suite by creating a sibling directory with the same layout (scripts + `run.sh` + `results.csv` + `BASELINE.md` + `README.md`).
