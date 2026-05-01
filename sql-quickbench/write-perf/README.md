# write-perf

Three scripts that build the same 5M-row x 43-col `dim_customer` table and time the write phase, so we can spot regressions in the DeltaForge writer pipeline.

| Script | What it does |
|---|---|
| `df_ctas.sql` | DeltaForge `CREATE DELTA TABLE ... AS SELECT` from `df_generate_table(...)` |
| `df_insert.sql` | DeltaForge `INSERT INTO ...` into a pre-created Delta table from the same generator |
| `duckdb_copy.sql` | DuckDB `COPY (...) TO '...' (FORMAT PARQUET, COMPRESSION 'snappy')`, same shape inline |

DuckDB is the reference: it is roughly the most aggressive parquet writer you can install quickly, on the same hardware, with no Delta protocol overhead.

## Run

```bash
./run.sh
```

Defaults assume the dev box layout (paths in the script header). Override with `DF_CLI=`, `DUCKDB=`, `OUT_DIR=`, `CRED_FILE=`, or `DF_USERNAME=`/`DF_PASSWORD=` env vars.

You can tag a run with a short note that ends up in `results.csv`:

```bash
NOTE="after enabling dict encoding" ./run.sh
```

## Output

- `results.csv` grows by three rows per run (one per script).
- `BASELINE.md` documents the current expected numbers and any open writer issues this suite has surfaced.

## Adding rows on a fresh box

The SQL scripts hard-code:

- `LOCATION 'B:/odbc_df/df-demo/perf_test/...'` for the DeltaForge tables and the DuckDB output file
- `pbi.bench` as the schema target for the DeltaForge tables (so the `pbi` zone needs to exist)

Edit the scripts (or sed-replace at run time) if your dev box differs. Keeping the on-disk paths box-specific is intentional: this is a developer benchmark, not a portable test, and rewriting paths every run is more friction than the portability is worth.
