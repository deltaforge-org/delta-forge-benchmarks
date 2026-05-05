-- index-perf: drop the row-level index. Used both as cleanup and as
-- the "no_index" mode preparation step before measuring baseline.

DROP INDEX IF EXISTS idx_customer_id
    ON pbi.bench.idx_perf_customer_10m;
