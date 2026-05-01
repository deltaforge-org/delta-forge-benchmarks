# write-perf baseline

What this suite measures: how long DeltaForge takes to materialise a 5M-row x 43-col `dim_customer` shape via two different write paths (CTAS vs INSERT), with DuckDB as a reference point for "what a tuned parquet writer should produce on the same box."

## Current numbers (post-writer-fix, 2026-05-01)

Box: Windows 11, output dir `B:/odbc_df/df-demo/perf_test/`
Build: `target/release/delta-forge-cli.exe`, post-rebuild after the fix described below.

| Script | Write step | Output size | Files | Compression | Encoding |
| --- | ---: | ---: | ---: | --- | --- |
| `duckdb_copy.sql` | ~20s | 285 MB | 1 | snappy | PLAIN+RLE+DICT |
| `df_ctas.sql` | **9.3s** | 344 MB | 12 | snappy | PLAIN+RLE+DICT |
| `df_insert.sql` | **28.1s** | **224 MB** | 3 | snappy | PLAIN+RLE+DICT |

Row data is byte-identical between CTAS and INSERT (same row count, same distinct ids, same `SUM(customer_id) = 12,500,002,500,000`). All three paths use the same compression + encoding stack now.

DF INSERT now produces a smaller file than DuckDB on this workload (224 MB vs 285 MB), driven by larger default row groups (~1.7M rows per file, vs DuckDB's ~122k) which gives dictionary + RLE more redundancy to exploit per group. CTAS remains slightly larger than DuckDB at 344 MB because it splits into 12 smaller files (each ~28 MB), so dictionary pages and per-file metadata are paid 12 times.

## Pre-fix numbers (for context)

| Script | Write step | Output size | Compression | Encoding |
| --- | ---: | ---: | --- | --- |
| `duckdb_copy.sql` | 19.5s | 285 MB | snappy | PLAIN+RLE+DICT |
| `df_ctas.sql` | 7.8s | 342 MB | snappy | PLAIN+RLE (no dict) |
| `df_insert.sql` | 25.8s | **639 MB** | **UNCOMPRESSED** | PLAIN+RLE+DICT |

The INSERT path was emitting uncompressed parquet because [datafusion_engine.rs:2045](../../delta-forge-engine/src/datafusion_engine.rs#L2045) constructed the `ArrowWriter` with `None` for `WriterProperties`, falling back to parquet-rs defaults (UNCOMPRESSED). The CTAS path went through `direct_file_writer.rs` which had `set_dictionary_enabled(false)` baked in.

After the fix the INSERT path went from 1.87x larger than CTAS to 35% **smaller**, and the CTAS encoding finally matches DuckDB's. CTAS got ~20% slower (7.8s -> 9.3s) for no measurable size benefit, which matches the CPU cost noted in the original `direct_file_writer.rs` comment; we accepted that trade because reader-side decode also benefits from dictionary pages.

## How to read a regression

Re-run `./run.sh` after a change. Compare the new row in `results.csv` against the baseline:

- Wall time +10% or more: regression. Run again to rule out noise; if it sticks, find the cause.
- Output size up at all: regression. We changed the writer config; figure out why.
- Output size down: usually a fix; double-check the parquet is still readable end-to-end.

The DuckDB row is a control. If it moves significantly between runs the box is busy and the DeltaForge numbers are unreliable for that run; re-run when the system is quiet.

## Wall-clock vs per-statement

The runner records **wall-clock** (one CLI invocation, all statements). The numbers in the table at the top of this file are per-statement (just the write step, parsed out of CLI output) because that is the apples-to-apples figure for writer perf. Add 3-5s to those for the wall-clock you will see in `results.csv` (auth + schema + drop + count overhead).

## Why DuckDB stays in the suite

It is the most aggressive parquet-tuning baseline you can install in 5 minutes, and our writer is the same shape of code (Arrow record batches -> parquet pages -> file). Tracking it alongside our numbers means a regression in our writer that DuckDB does not see is unambiguous: it is in our code, not the box.

## Open follow-ups

- **`BYTE_STREAM_SPLIT` for floats.** DuckDB encodes `latitude`, `longitude`, `churn_risk_score` with byte-stream-split, which compresses much better than our PLAIN. Worth measuring on a workload with more float-heavy columns; not the dominant lever on this table.
- **CTAS file count.** 12 small files vs INSERT's 3 large files explains most of the residual gap to DuckDB on size. The CTAS path could merge to fewer, larger files if size on disk matters more than write parallelism.
