"""Measure end-to-end CLI wall time vs server-reported execution_time_ms.

If the gap is large, it means the server is reporting only a fraction of
the actual query work (e.g. handler setup, not full batch drain).
"""
from __future__ import annotations
import json
import re
import subprocess
import statistics
import time

CLI = "delta-forge-cli"
CONTROL = "http://127.0.0.1:3000"
NODE = "bench-local"

QUERIES = [
    ("count_groups",
     "SELECT l_returnflag, COUNT(*) FROM bench_ext.tpch_read.lineitem GROUP BY l_returnflag"),
    ("q01_shape",
     "SELECT l_returnflag, l_linestatus, sum(l_quantity) AS s, sum(l_extendedprice) AS p "
     "FROM bench_ext.tpch_read.lineitem "
     "WHERE l_shipdate <= CAST('1998-09-02' AS date) "
     "GROUP BY l_returnflag, l_linestatus "
     "ORDER BY l_returnflag, l_linestatus"),
    ("full_scan_top10",
     "SELECT * FROM bench_ext.tpch_read.lineitem LIMIT 10"),
    ("aggregate_no_filter",
     "SELECT COUNT(*), SUM(l_quantity), AVG(l_extendedprice) "
     "FROM bench_ext.tpch_read.lineitem"),
]


def run_once(sql: str) -> tuple[float, float | None, int]:
    """Return (cli_wall_ms, server_reported_ms, row_count)."""
    t0 = time.perf_counter()
    proc = subprocess.run(
        [CLI, "--format", "json", "--control-url", CONTROL,
         "query", "--node", NODE, sql],
        capture_output=True, text=True, timeout=600,
    )
    wall_ms = (time.perf_counter() - t0) * 1000.0
    out = proc.stdout
    m = re.search(r"^\{", out, re.M)
    if m is None:
        return (wall_ms, None, 0)
    try:
        j = json.loads(out[m.start():])
    except json.JSONDecodeError:
        return (wall_ms, None, 0)
    return (wall_ms, float(j.get("execution_time_ms") or 0), int(j.get("row_count") or 0))


def main() -> None:
    WARMUP = 3
    REPS = 5
    print(f"{'query':<22}{'rows':>6}{'cli_wall':>10}{'server_ex':>12}"
          f"{'delta':>10}{'server%':>10}")
    print("-" * 70)
    for label, sql in QUERIES:
        # warm
        for _ in range(WARMUP):
            run_once(sql)
        walls = []
        servers = []
        rc = 0
        for _ in range(REPS):
            w, s, r = run_once(sql)
            walls.append(w)
            if s is not None:
                servers.append(s)
            rc = r
        w_med = statistics.median(walls)
        s_med = statistics.median(servers) if servers else 0.0
        delta = w_med - s_med
        pct = (100.0 * s_med / w_med) if w_med else 0
        print(f"{label:<22}{rc:>6}{w_med:>10.1f}{s_med:>12.1f}"
              f"{delta:>+10.1f}{pct:>9.1f}%")


if __name__ == "__main__":
    main()
