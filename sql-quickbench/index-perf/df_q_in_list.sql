-- index-perf: small IN list. Five rows out of 10M, scattered across
-- the keyspace so they land in different row groups.
SELECT
    customer_id,
    full_name,
    email,
    loyalty_tier,
    lifetime_revenue_usd
FROM pbi.bench.idx_perf_customer_10m
WHERE customer_id IN (1, 12345, 1000000, 5000000, 9999999);
