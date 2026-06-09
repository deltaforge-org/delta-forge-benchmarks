"""Parse SHOW STATS ACTUAL output from /tmp/stats.json and print phase times."""
import json
import re

with open("/tmp/stats.json") as f:
    text = f.read()
m = re.search(r"^\{", text, re.M)
if not m:
    print(text)
    raise SystemExit(0)
j = json.loads(text[m.start():])
print(f"server execution_time_ms (wall around handler): {j.get('execution_time_ms')}")
print()
print("--- SHOW STATS ACTUAL: time category breakdown ---")
for r in j["rows"]:
    if r.get("category") == "time":
        v = r.get("value")
        metric = r["metric"]
        unit = r.get("unit")
        print(f"  {metric:<30}  {v}  {unit}")
