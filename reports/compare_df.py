"""Compare warm-medians for ONE engine: previous run vs current run.

Usage:
    python compare_df.py [engine] [prev_run_tag] [curr_run_tag]

Defaults to engine=df, prev=20260516T222720Z-e030feb2b63c (the audit-off
pure-reader run), curr=20260517T230117Z-8ae0556ef6b8 (the post-fix
audit-off run).
"""
import json
import statistics
import sys


def load(path):
    by = {}
    for line in open(path):
        r = json.loads(line)
        if r.get("step_kind") != "sql_query" or r.get("error"):
            continue
        e = r.get("engine_reported_ms")
        if e is None:
            e = r["wall_ms"]
        by.setdefault(r["step_id"], []).append((r["cold"], float(e)))
    return by


engine = sys.argv[1] if len(sys.argv) > 1 else "df"
prev_tag = sys.argv[2] if len(sys.argv) > 2 else "20260516T222720Z-e030feb2b63c"
curr_tag = sys.argv[3] if len(sys.argv) > 3 else "20260517T230117Z-8ae0556ef6b8"
print(f"engine={engine}  prev={prev_tag}  new={curr_tag}\n")
prev = load(f"/workspace/results/{prev_tag}/raw/{engine}.jsonl")
curr = load(f"/workspace/results/{curr_tag}/raw/{engine}.jsonl")

hdr_q = "q"
hdr_p = "prev_med"
hdr_n = "new_med"
hdr_d = "delta"
hdr_pct = "delta%"
print(f"{hdr_q:<5}{hdr_p:>12}{hdr_n:>12}{hdr_d:>10}{hdr_pct:>10}")
print("-" * 55)
for q in sorted(set(prev) | set(curr)):
    p_warm = sorted(m for c, m in prev.get(q, []) if not c)
    n_warm = sorted(m for c, m in curr.get(q, []) if not c)
    pm = statistics.median(p_warm) if p_warm else None
    nm = statistics.median(n_warm) if n_warm else None
    if pm is None or nm is None:
        ps = "-" if pm is None else f"{pm:.2f}"
        ns = "-" if nm is None else f"{nm:.2f}"
        print(f"{q:<5}{ps:>12}{ns:>12}{'-':>10}{'-':>10}")
        continue
    d = nm - pm
    p = 100 * (d / pm) if pm else 0
    print(f"{q:<5}{pm:>12.2f}{nm:>12.2f}{d:>+10.2f}{p:>+9.1f}%")

print()
all_prev = sorted(m for q in prev for c, m in prev[q] if not c)
all_curr = sorted(m for q in curr for c, m in curr[q] if not c)
prev_overall = statistics.median(all_prev) if all_prev else None
curr_overall = statistics.median(all_curr) if all_curr else None
if prev_overall and curr_overall:
    d = curr_overall - prev_overall
    p = 100 * (d / prev_overall)
    print(
        f"overall median  prev={prev_overall:.2f}  new={curr_overall:.2f}  "
        f"delta={d:+.2f}  ({p:+.1f}%)"
    )
