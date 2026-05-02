-- TPC-H Q10 against bare parquet files (DuckDB reference).
SELECT
    c_custkey,
    c_name,
    SUM(l_extendedprice * (1 - l_discount)) AS revenue,
    c_acctbal,
    n_name,
    c_address,
    c_phone,
    c_comment
FROM read_parquet('B:/odbc_df/df-demo/tpch/sf{{SF}}/customer/*.parquet'),
     read_parquet('B:/odbc_df/df-demo/tpch/sf{{SF}}/orders/*.parquet'),
     read_parquet('B:/odbc_df/df-demo/tpch/sf{{SF}}/lineitem/*.parquet'),
     read_parquet('B:/odbc_df/df-demo/tpch/sf{{SF}}/nation/*.parquet')
WHERE c_custkey   = o_custkey
  AND l_orderkey  = o_orderkey
  AND o_orderdate >= DATE '1993-10-01'
  AND o_orderdate <  DATE '1994-01-01'
  AND l_returnflag = 'R'
  AND c_nationkey = n_nationkey
GROUP BY c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment
ORDER BY revenue DESC
LIMIT 20;
