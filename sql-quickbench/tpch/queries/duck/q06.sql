-- TPC-H Q6 against bare parquet files (DuckDB reference).
SELECT
    SUM(l_extendedprice * l_discount) AS revenue
FROM read_parquet('B:/odbc_df/df-demo/tpch/sf{{SF}}/lineitem/*.parquet')
WHERE l_shipdate >= DATE '1994-01-01'
  AND l_shipdate <  DATE '1995-01-01'
  AND l_discount BETWEEN 0.05 AND 0.07
  AND l_quantity <  24;
