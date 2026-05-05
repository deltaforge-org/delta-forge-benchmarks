-- index-perf: narrow range. 100 rows out of 10M (0.001% selectivity).
-- Index expected to engage; result fits in a couple of row groups
-- worth of leaf pages.
SELECT
    customer_id,
    full_name,
    email,
    loyalty_tier,
    lifetime_revenue_usd
FROM pbi.bench.idx_perf_customer_10m
WHERE customer_id BETWEEN 5000000 AND 5000099;
