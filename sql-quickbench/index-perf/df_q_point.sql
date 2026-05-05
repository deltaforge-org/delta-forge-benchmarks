-- index-perf: point lookup. Single row out of 10M.
SELECT
    customer_id,
    full_name,
    email,
    loyalty_tier,
    lifetime_revenue_usd
FROM pbi.bench.idx_perf_customer_10m
WHERE customer_id = 5000000;
