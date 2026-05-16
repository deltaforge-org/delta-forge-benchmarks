-- index-perf 30M: narrow range — 100 rows out of 30M (0.0003% selectivity).
-- At this scale the no-index scan reads 3x more data than the 10M table,
-- while the indexed path still reads only the same handful of row pointers.
SELECT
    customer_id,
    full_name,
    email,
    city,
    annual_income_usd
FROM pbi.bench.idx_perf_customer_30m
WHERE customer_id BETWEEN 15000000 AND 15000099;
