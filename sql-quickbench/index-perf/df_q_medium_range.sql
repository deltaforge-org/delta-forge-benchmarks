-- index-perf: medium range — 100k rows out of 10M (1% selectivity).
-- This is the cost-threshold sanity check: at 1% the selector should
-- decline the index and fall back to file-stats pruning + column
-- scan. Expected speedup ~1.0x (within noise).
SELECT
    COUNT(*)                                           AS matched,
    SUM(CAST(lifetime_revenue_usd AS DOUBLE))          AS sum_revenue,
    AVG(CAST(loyalty_points_balance AS DOUBLE))        AS avg_points
FROM pbi.bench.idx_perf_customer_10m
WHERE customer_id BETWEEN 1000000 AND 1099999;
