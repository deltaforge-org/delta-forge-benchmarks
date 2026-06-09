"""Verify: OPEN + SHOW STATS in one CLI invocation works and gives total_time_ms."""
import json
import re
import subprocess

CLI = "delta-forge-cli"
CONTROL = "http://127.0.0.1:3000"
NODE = "bench-local"

sql = (
    "OPEN DELTA TABLE '/workspace/data/tpch_sf1_delta/lineitem' AS lineitem; "
    "OPEN DELTA TABLE '/workspace/data/tpch_sf1_delta/orders' AS orders; "
    "SHOW STATS ACTUAL "
    "SELECT l_returnflag, COUNT(*) AS n FROM lineitem GROUP BY l_returnflag"
)
proc = subprocess.run(
    [CLI, "--format", "json", "--control-url", CONTROL,
     "query", "--node", NODE, sql],
    capture_output=True, text=True, timeout=120,
)
out = proc.stdout
m = re.search(r"^\{", out, re.M)
if m is None:
    print(out)
    raise SystemExit(0)
j = json.loads(out[m.start():])
print(f"handler exec_ms = {j.get('execution_time_ms')}")
print(f"rows in stats   = {j.get('row_count')}")
keep = {"total_time_ms", "execution_time_ms", "planning_time_ms",
        "streaming_time_ms", "decode_time_ms", "compile_time_ms",
        "rows_returned"}
for r in j["rows"]:
    metric = r.get("metric") or ""
    if metric in keep:
        cat = r.get("category") or ""
        val = r.get("value")
        unit = r.get("unit") or ""
        print(f"  {cat:<6} {metric:<25} {val}  {unit}")
