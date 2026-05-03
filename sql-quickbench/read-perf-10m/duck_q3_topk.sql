SELECT
  customer_id,
  full_name,
  email,
  city,
  annual_income_usd
FROM read_parquet('B:/odbc_df/df-demo/perf_test/dim_customer_insert_10m/*.parquet')
WHERE region = 'NA'
  AND annual_income_usd > 200000
ORDER BY annual_income_usd DESC
LIMIT 1000;
