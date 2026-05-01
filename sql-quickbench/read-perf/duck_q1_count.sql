SELECT COUNT(*) AS rows
FROM read_parquet('B:/odbc_df/df-demo/perf_test/dim_customer_insert/*.parquet');
