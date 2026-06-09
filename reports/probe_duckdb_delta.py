"""Smoke-test that DuckDB's delta extension can read the fixture."""
import duckdb

c = duckdb.connect(":memory:")
c.execute("INSTALL delta")
c.execute("LOAD delta")
n = c.execute(
    "SELECT COUNT(*) FROM delta_scan('/workspace/data/tpch_sf1_delta/lineitem')"
).fetchone()
print(f"DuckDB delta_scan lineitem rows = {n[0]:,}")
n = c.execute(
    "SELECT l_returnflag, COUNT(*) FROM delta_scan('/workspace/data/tpch_sf1_delta/lineitem') "
    "GROUP BY l_returnflag ORDER BY l_returnflag"
).fetchall()
print(f"DuckDB delta_scan group-by: {n}")
