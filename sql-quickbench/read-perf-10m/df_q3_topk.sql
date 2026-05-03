SELECT
  customer_id,
  full_name,
  email,
  city,
  annual_income_usd
FROM pbi.bench.dim_customer_insert_10m
WHERE region = 'NA'
  AND annual_income_usd > 200000
ORDER BY annual_income_usd DESC
LIMIT 1000;
