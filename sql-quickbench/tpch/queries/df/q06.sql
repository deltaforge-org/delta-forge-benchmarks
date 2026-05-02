-- TPC-H Q6: forecasting revenue change
-- Tests: single-table scan with selective range predicates, single-row aggregate.
-- Predicate selectivity ~1.9% of lineitem, so good signal on push-down quality.
SELECT
    SUM(l_extendedprice * l_discount) AS revenue
FROM tpch.sf{{SF}}.lineitem
WHERE l_shipdate >= DATE '1994-01-01'
  AND l_shipdate <  DATE '1995-01-01'
  AND l_discount BETWEEN 0.05 AND 0.07
  AND l_quantity <  24;
