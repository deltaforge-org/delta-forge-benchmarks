"""Quantify the gap between df's CLI-reported execution_time_ms (what
the bench currently records as `engine_reported_ms`) and SHOW STATS'
total_time_ms / execution_time_ms (pure engine phases).

For each query, runs:
  1. N plain warmup queries
  2. M plain timed queries (records execution_time_ms from JSON)
  3. M SHOW STATS ACTUAL queries (records total_time_ms + per-phase ms)

Compares medians side by side. The delta between handler wall and
SHOW STATS total is the response-framing / JSON-serialization overhead
df carries that DuckDB/Spark don't.
"""
from __future__ import annotations

import json
import re
import statistics
import subprocess

CLI = "delta-forge-cli"
CONTROL = "http://127.0.0.1:3000"
NODE = "bench-local"

WARMUP = 3
TIMED = 5

# Query, expected result row count (for context on JSON overhead).
QUERIES = [
    ("count_groups",
     "SELECT l_returnflag, COUNT(*) FROM bench_ext.tpch_read.lineitem GROUP BY l_returnflag"),
    ("scan_all_lineitem_top5",
     "SELECT * FROM bench_ext.tpch_read.lineitem LIMIT 5"),
    ("q01_shape",
     "SELECT l_returnflag, l_linestatus, sum(l_quantity) AS s "
     "FROM bench_ext.tpch_read.lineitem "
     "WHERE l_shipdate <= CAST('1998-09-02' AS date) "
     "GROUP BY l_returnflag, l_linestatus "
     "ORDER BY l_returnflag, l_linestatus"),
]


def run_json(sql: str) -> dict | None:
    """Run via the CLI, parse JSON response, return parsed dict (or None)."""
    proc = subprocess.run(
        [CLI, "--format", "json", "--control-url", CONTROL, "query",
         "--node", NODE, sql],
        capture_output=True, text=True, timeout=600,
    )
    out = proc.stdout
    m = re.search(r"^\{", out, re.M)
    if not m:
        return None
    try:
        return json.loads(out[m.start():])
    except json.JSONDecodeError:
        return None


def show_stats_phases(sql: str) -> tuple[float | None, float | None, float | None]:
    """Run `SHOW STATS ACTUAL <sql>`. Return
    (handler_wall_ms, phase_total_ms, phase_execution_ms)."""
    j = run_json(f"SHOW STATS ACTUAL {sql}")
    if j is None:
        return (None, None, None)
    handler = j.get("execution_time_ms")
    total = None
    pure_exec = None
    for r in j.get("rows", []):
        if r.get("category") != "time":
            continue
        if r["metric"] == "total_time_ms":
            v = r.get("value")
            total = float(v) if v not in (None, "None") else None
        if r["metric"] == "execution_time_ms":
            v = r.get("value")
            pure_exec = float(v) if v not in (None, "None") else None
    return (float(handler) if handler is not None else None, total, pure_exec)


def measure_plain(sql: str) -> tuple[float | None, int]:
    """Single plain query run. Return (handler_wall_ms_from_json, row_count)."""
    j = run_json(sql)
    if j is None:
        return (None, 0)
    return (float(j.get("execution_time_ms") or 0), int(j.get("row_count") or 0))


def main() -> None:
    for label, sql in QUERIES:
        print(f"\n=== {label} ===")
        print(f"  {sql[:90]}")
        # Warm both paths
        for _ in range(WARMUP):
            run_json(sql)
            run_json(f"SHOW STATS ACTUAL {sql}")

        plain = []
        rc = 0
        for _ in range(TIMED):
            t, rc = measure_plain(sql)
            if t is not None:
                plain.append(t)
        stats = []
        for _ in range(TIMED):
            handler, total, pure_exec = show_stats_phases(sql)
            if total is not None:
                stats.append((handler or 0, total, pure_exec or 0))

        if not plain or not stats:
            print("  measurement failed")
            continue

        m_plain = statistics.median(plain)
        m_handler_ss = statistics.median(h for h, _, _ in stats)
        m_total_ss = statistics.median(t for _, t, _ in stats)
        m_pure_ss = statistics.median(p for _, _, p in stats)

        print(f"  result row count                : {rc}")
        print(f"  plain handler wall ms (median)   : {m_plain:>8.2f}   "
              f"[what bench reports as df warm]")
        print(f"  SHOW STATS handler wall ms       : {m_handler_ss:>8.2f}   "
              f"[same handler shape, +instr overhead]")
        print(f"  SHOW STATS total_time_ms         : {m_total_ss:>8.2f}   "
              f"[sum of instrumented engine phases]")
        print(f"  SHOW STATS pure execution_time_ms: {m_pure_ss:>8.2f}   "
              f"[just the scan + agg]")
        if m_total_ss > 0:
            framing = m_plain - m_total_ss
            framing_pct = 100 * framing / m_plain
            print(f"  framing overhead (plain - total) : {framing:>8.2f}   "
                  f"({framing_pct:+.1f}% of plain handler wall)")


if __name__ == "__main__":
    main()
