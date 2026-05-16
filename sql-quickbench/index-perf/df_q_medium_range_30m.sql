-- index-perf 30M: medium range — 300K rows out of 30M (1% selectivity).
-- At 1% the selector should decline the index and fall back to column scan
-- (same cost-threshold behaviour as 10M). Expected speedup ~1.0x.
SELECT
    COUNT(*)                                           AS matched,
    SUM(CAST(lifetime_revenue_usd AS DOUBLE))          AS sum_revenue,
    AVG(CAST(loyalty_points_balance AS DOUBLE))        AS avg_points
FROM pbi.bench.idx_perf_customer_30m
WHERE customer_id BETWEEN 5000000 AND 5299999;
