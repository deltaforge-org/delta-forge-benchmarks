"""List every SHOW STATS metric key so df_engine.py can extract the right ones."""
import json
import re
import subprocess

proc = subprocess.run(
    ["delta-forge-cli", "--format", "json",
     "--control-url", "http://127.0.0.1:3000", "query",
     "--node", "bench-local",
     "SHOW STATS ACTUAL SELECT COUNT(*) FROM bench_ext.tpch_read.lineitem"],
    capture_output=True, text=True, timeout=120,
)
text = proc.stdout
m = re.search(r"^\{", text, re.M)
if not m:
    print(text)
    raise SystemExit(0)
j = json.loads(text[m.start():])
seen_cats = sorted({r.get("category") for r in j["rows"] if r.get("category")})
print("categories:", seen_cats)
print()
for cat in seen_cats:
    print(f"--- {cat} ---")
    for r in j["rows"]:
        if r.get("category") == cat:
            metric = r.get("metric") or ""
            value = r.get("value")
            unit = r.get("unit") or ""
            print(f"  {metric:<32} {value!r:<14} {unit}")
    print()
