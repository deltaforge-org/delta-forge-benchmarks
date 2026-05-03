-- After DuckDB has populated B:/odbc_df/df-demo/perf_test/dim_customer_duck
-- with parquet, wrap that directory in place by writing a fresh _delta_log
-- alongside the existing parquet. CONVERT TO DELTA does not rewrite or move
-- any data; it only adds the Delta metadata so a Delta-aware reader can
-- open the directory.
--
-- Each query script then opens this Delta-formatted directory under a
-- session-scoped alias via OPEN DELTA TABLE in the same CLI invocation.

CONVERT TO DELTA 'B:/odbc_df/df-demo/perf_test/dim_customer_duck';
