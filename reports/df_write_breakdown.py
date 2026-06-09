#!/usr/bin/env python3
"""Decompose df's CTAS-write time into engine phases via SHOW STATS ACTUAL.

Run inside the bench container:
    python /workspace/reports/df_write_breakdown.py
"""
from __future__ import annotations

import json
import os
import re
import subprocess
import sys


CLI = "delta-forge-cli"
CONTROL = "http://127.0.0.1:3000"
NODE = "bench-local"


def run(sql: str, require_json: bool = True) -> dict | None:
    proc = subprocess.run(
        [CLI, "--format", "json", "--control-url", CONTROL,
         "query", "--node", NODE, sql],
        capture_output=True, text=True, timeout=600,
    )
    out = proc.stdout
    m = re.search(r"^\{", out, re.M)
    if not m:
        if require_json:
            print(f"NON-JSON OUTPUT for {sql[:80]!r}:")
            print(out)
            print("STDERR:", proc.stderr)
            sys.exit(1)
        return None
    return json.loads(out[m.start():])


def time_ctas(label: str) -> float:
    """One DROP-then-CTAS cycle, return server-reported execution_time_ms."""
    run("DROP DELTA TABLE IF EXISTS bench_w.tpch_w.lineitem WITH FILES",
        require_json=False)
    j = run(
        "CREATE DELTA TABLE bench_w.tpch_w.lineitem LOCATION 'lineitem' "
        "AS SELECT * FROM bench_ext.csv_input.lineitem_pq",
        require_json=False,
    )
    # CTAS returns a plain-text 'OK' line with (NNNNms). Re-issue via the
    # JSON CLI by wrapping in an explain-style measurement.
    # Simpler path: run via the CLI in JSON mode again, parsing 'rows' for
    # the empty result.
    proc = subprocess.run(
        [CLI, "--format", "json", "--control-url", CONTROL,
         "query", "--node", NODE,
         "DROP DELTA TABLE IF EXISTS bench_w.tpch_w.lineitem WITH FILES"],
        capture_output=True, text=True, timeout=120,
    )
    # Run the CTAS again with JSON output to read execution_time_ms.
    proc = subprocess.run(
        [CLI, "--format", "json", "--control-url", CONTROL,
         "query", "--node", NODE,
         "CREATE DELTA TABLE bench_w.tpch_w.lineitem LOCATION 'lineitem' "
         "AS SELECT * FROM bench_ext.csv_input.lineitem_pq"],
        capture_output=True, text=True, timeout=600,
    )
    # The CLI emits a non-JSON success line for DDL even with --format json.
    m = re.search(r"\((\d+)ms\)", proc.stdout + proc.stderr)
    ms = float(m.group(1)) if m else float("nan")
    print(f"{label:<30} {ms:>10.1f} ms")
    return ms


def time_select_star() -> float:
    """SHOW STATS ACTUAL on a pure-read SELECT over the same source. Returns
    total_time_ms from the engine's instrumented phases."""
    j = run(
        "SHOW STATS ACTUAL SELECT * FROM bench_ext.csv_input.lineitem_pq"
    )
    by_metric = {r["metric"]: r.get("value") for r in j["rows"]
                 if r["category"] == "time"}
    total = float(by_metric.get("total_time_ms") or 0)
    print()
    print("SHOW STATS ACTUAL  SELECT * FROM <parquet>  --  pure read")
    print("-" * 60)
    print(f"  server execution_time_ms (wall):  {j.get('execution_time_ms')}")
    for k, v in by_metric.items():
        try:
            vf = f"{float(v):.2f}"
        except (TypeError, ValueError):
            vf = str(v)
        print(f"  {k:<30}  {vf:>12}")
    return total


def main() -> None:
    print("=== df write-phase breakdown ===\n")
    print(f"{'op':<30} {'wall':>10}")
    print("-" * 42)

    # Three CTAS samples so we can see variance.
    ctas_ms = []
    for i in range(3):
        ctas_ms.append(time_ctas(f"CTAS run {i+1}"))

    # Pure read of the same source through the same external table.
    read_total = time_select_star()

    avg_ctas = sum(ctas_ms) / len(ctas_ms)
    write_only = avg_ctas - read_total
    print()
    print("--- decomposition (avg CTAS) ---")
    print(f"  avg CTAS wall_ms     : {avg_ctas:>10.1f}")
    print(f"  pure-read total_time : {read_total:>10.1f}")
    print(f"  write-only (diff)    : {write_only:>10.1f}")
    if avg_ctas > 0:
        print(f"  write share of total : {100*write_only/avg_ctas:>9.1f}%")


if __name__ == "__main__":
    main()
