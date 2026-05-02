SELECT
  segment,
  COUNT(*)                           AS customers,
  AVG(annual_income_usd)             AS avg_income,
  MAX(loyalty_points_balance)        AS max_points,
  SUM(lifetime_revenue_usd)          AS total_revenue
FROM read_parquet('B:/odbc_df/df-demo/perf_test/dim_customer_duck/*.parquet')
GROUP BY segment
ORDER BY total_revenue DESC;
