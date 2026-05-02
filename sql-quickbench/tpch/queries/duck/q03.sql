-- TPC-H Q3 against bare parquet files (DuckDB reference).
SELECT
    l_orderkey,
    SUM(l_extendedprice * (1 - l_discount)) AS revenue,
    o_orderdate,
    o_shippriority
FROM read_parquet('B:/odbc_df/df-demo/tpch/sf{{SF}}/customer/*.parquet'),
     read_parquet('B:/odbc_df/df-demo/tpch/sf{{SF}}/orders/*.parquet'),
     read_parquet('B:/odbc_df/df-demo/tpch/sf{{SF}}/lineitem/*.parquet')
WHERE c_mktsegment = 'BUILDING'
  AND c_custkey    = o_custkey
  AND l_orderkey   = o_orderkey
  AND o_orderdate  < DATE '1995-03-15'
  AND l_shipdate   > DATE '1995-03-15'
GROUP BY l_orderkey, o_orderdate, o_shippriority
ORDER BY revenue DESC, o_orderdate
LIMIT 10;
