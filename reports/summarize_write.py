#!/usr/bin/env python3
"""Per-engine write throughput summary for the csv_to_delta workload."""
from __future__ import annotations

import json
import statistics
import sys
from pathlib import Path

SRC_ROWS = 6_001_215
SRC_BYTES = 274_753_951  # /workspace/data/tpch_sf1/lineitem.parquet


def main(run_dir: str) -> None:
    raw = Path(run_dir) / "raw"
    files = sorted(raw.glob("*.jsonl"))
    if not files:
        sys.exit(f"no *.jsonl under {raw}")

    print()
    print(f"Parquet input: lineitem.parquet  rows={SRC_ROWS:,}  bytes={SRC_BYTES:,}")
    print("Targets: df=Delta on disk, duckdb=parquet on disk, spark-default=Delta on disk")
    print()
    hdr = (
        f"{'engine':<16}{'cold_ms':>10}{'warm_med_ms':>13}"
        f"{'warm_p90_ms':>13}{'warm_min_ms':>13}"
        f"{'rows/s warm':>14}{'MB/s warm':>11}"
    )
    print(hdr)
    print("-" * len(hdr))

    results: dict[str, float] = {}
    for f in files:
        engine = f.stem
        recs = [json.loads(l) for l in f.open()]
        ok = [
            r for r in recs
            if r.get("step_kind") in ("sql_ddl", "sql_dml")
            and not r.get("error")
            and r.get("exit_code", 0) == 0
        ]
        if not ok:
            print(f"{engine:<16}  (no successful runs)")
            continue
        cold = next((r["wall_ms"] for r in ok if r.get("cold")), None)
        warm = sorted(r["wall_ms"] for r in ok if not r.get("cold"))
        if not warm:
            print(f"{engine:<16}  (no warm runs)")
            continue
        med = statistics.median(warm)
        p90 = warm[int(len(warm) * 0.9) - 1] if len(warm) > 1 else warm[0]
        mn = min(warm)
        rps = SRC_ROWS * 1000.0 / med
        mbs = (SRC_BYTES / 1e6) * 1000.0 / med
        cold_s = f"{cold:.0f}" if cold is not None else "-"
        print(
            f"{engine:<16}{cold_s:>10}{med:>13.0f}{p90:>13.0f}{mn:>13.0f}"
            f"{rps:>14,.0f}{mbs:>11,.1f}"
        )
        results[engine] = med

    if "df" in results:
        print()
        print("Speedup vs df (warm median, higher = faster than df):")
        for e, m in results.items():
            if e == "df":
                continue
            print(f"  {e:<16}{results['df'] / m:>6.2f}x")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "/workspace/results")
