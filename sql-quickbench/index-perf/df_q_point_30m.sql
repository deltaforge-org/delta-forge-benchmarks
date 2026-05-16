SELECT
    customer_id,
    full_name,
    email,
    loyalty_tier,
    lifetime_revenue_usd
FROM pbi.bench.idx_perf_customer_30m
WHERE customer_id = 15000000;
