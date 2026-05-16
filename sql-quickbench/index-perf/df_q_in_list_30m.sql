SELECT
    customer_id,
    full_name,
    email,
    loyalty_tier,
    lifetime_revenue_usd
FROM pbi.bench.idx_perf_customer_30m
WHERE customer_id IN (1, 12345, 5000000, 15000000, 29999999);
