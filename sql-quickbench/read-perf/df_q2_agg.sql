SELECT
  segment,
  COUNT(*)                           AS customers,
  AVG(annual_income_usd)             AS avg_income,
  MAX(loyalty_points_balance)        AS max_points,
  SUM(lifetime_revenue_usd)          AS total_revenue
FROM pbi.bench.dim_customer_insert
GROUP BY segment
ORDER BY total_revenue DESC;
