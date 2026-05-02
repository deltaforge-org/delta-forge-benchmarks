-- TPC-H Q10: returned-item reporting
-- Tests: 4-way join (customer x orders x lineitem x nation), wide group-by,
-- top-K with ORDER BY ... LIMIT.
SELECT
    c_custkey,
    c_name,
    SUM(l_extendedprice * (1 - l_discount)) AS revenue,
    c_acctbal,
    n_name,
    c_address,
    c_phone,
    c_comment
FROM tpch.sf{{SF}}.customer,
     tpch.sf{{SF}}.orders,
     tpch.sf{{SF}}.lineitem,
     tpch.sf{{SF}}.nation
WHERE c_custkey   = o_custkey
  AND l_orderkey  = o_orderkey
  AND o_orderdate >= DATE '1993-10-01'
  AND o_orderdate <  DATE '1994-01-01'
  AND l_returnflag = 'R'
  AND c_nationkey = n_nationkey
GROUP BY c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment
ORDER BY revenue DESC
LIMIT 20;
