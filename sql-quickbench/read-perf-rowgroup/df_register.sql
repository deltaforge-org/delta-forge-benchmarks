-- After DuckDB has populated B:/odbc_df/df-demo/perf_test/dim_customer_duck
-- with parquet files, this script wraps that directory in-place as a Delta
-- table (CONVERT TO DELTA writes a fresh _delta_log alongside the existing
-- parquet without rewriting any data) and then registers the result in the
-- catalog so the read-perf queries can address it as a normal table.
--
-- UNREGISTER is a no-op on first run; on re-runs it clears the prior
-- catalog entry without touching files (the duck_write step has already
-- nuked and re-created the directory).

CREATE SCHEMA IF NOT EXISTS pbi.bench;

UNREGISTER TABLE IF EXISTS pbi.bench.dim_customer_duck;

CONVERT TO DELTA 'B:/odbc_df/df-demo/perf_test/dim_customer_duck';

REGISTER TABLE pbi.bench.dim_customer_duck
LOCATION 'B:/odbc_df/df-demo/perf_test/dim_customer_duck';

SELECT COUNT(*) AS rows FROM pbi.bench.dim_customer_duck;
