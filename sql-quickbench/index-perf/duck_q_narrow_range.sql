SELECT
    customer_id,
    full_name,
    email,
    loyalty_tier,
    lifetime_revenue_usd
FROM read_parquet('B:/odbc_df/tmp/index-perf-data/idx_perf_customer_10m/*.parquet')
WHERE customer_id BETWEEN 5000000 AND 5000099;
