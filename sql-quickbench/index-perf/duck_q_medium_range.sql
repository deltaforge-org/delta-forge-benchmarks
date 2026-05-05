SELECT
    COUNT(*)                                AS matched,
    SUM(CAST(lifetime_revenue_usd AS DOUBLE))  AS sum_revenue,
    AVG(CAST(loyalty_points_balance AS DOUBLE)) AS avg_points
FROM read_parquet('B:/odbc_df/tmp/index-perf-data/idx_perf_customer_10m/*.parquet')
WHERE customer_id BETWEEN 1000000 AND 1099999;
