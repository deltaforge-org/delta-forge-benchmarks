-- index-perf: DuckDB sibling for the point lookup. Reads the
-- DeltaForge-written parquet files directly via read_parquet, so the
-- physical bytes scanned are identical (no Delta log parsing).
-- DuckDB has no row-level secondary index for parquet; this is the
-- external reference point — what a high-quality columnar engine
-- without an index does.
SELECT
    customer_id,
    full_name,
    email,
    loyalty_tier,
    lifetime_revenue_usd
FROM read_parquet('B:/odbc_df/tmp/index-perf-data/idx_perf_customer_10m/*.parquet')
WHERE customer_id = 5000000;
