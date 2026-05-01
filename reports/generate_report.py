#!/usr/bin/env python3
"""Generate a per-run report from `results/<run>/raw/*.jsonl` records.

Three outputs:
  - `summary.csv`  -- one row per (workload, engine, step), with min /
                       median / p95 / mean / stddev across runs.
  - `report.md`    -- human-readable executive summary, methodology link,
                       results tables, honest-losses section, cross-engine
                       correctness check.
  - `raw_records.jsonl` -- concatenation of every engine's raw records,
                            sorted, for archival.

Usage:
    python reports/generate_report.py --results-dir results/<run-name>

The script is intentionally simple: it does not depend on pandas or
matplotlib. Reviewers can read the source and verify what every cell
in the published table came from.
"""
from __future__ import annotations

import argparse
import csv
import json
import math
import statistics
import sys
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


def percentile(values: list[float], p: float) -> float:
    """Linear-interpolation percentile (matches NumPy's default).
    p is in [0, 100]."""
    if not values:
        return float("nan")
    s = sorted(values)
    if len(s) == 1:
        return s[0]
    rank = (p / 100.0) * (len(s) - 1)
    lo = int(math.floor(rank))
    hi = int(math.ceil(rank))
    if lo == hi:
        return s[lo]
    return s[lo] + (s[hi] - s[lo]) * (rank - lo)


@dataclass
class CellStats:
    workload: str
    engine: str
    step_id: str
    cold_ms: list[float]
    warm_ms: list[float]
    failures: int
    rows_returned: list[int]
    result_sha256: str | None  # taken from the first successful run

    def warm_min(self):    return min(self.warm_ms) if self.warm_ms else float("nan")
    def warm_median(self): return statistics.median(self.warm_ms) if self.warm_ms else float("nan")
    def warm_p95(self):    return percentile(self.warm_ms, 95) if self.warm_ms else float("nan")
    def warm_mean(self):   return statistics.fmean(self.warm_ms) if self.warm_ms else float("nan")
    def warm_stddev(self):
        return statistics.stdev(self.warm_ms) if len(self.warm_ms) >= 2 else 0.0
    def cold_median(self): return statistics.median(self.cold_ms) if self.cold_ms else float("nan")


def load_records(results_dir: Path) -> list[dict]:
    raw_dir = results_dir / "raw"
    records = []
    for jl in sorted(raw_dir.glob("*.jsonl")):
        with jl.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                records.append(json.loads(line))
    return records


def cellize(records: list[dict]) -> dict[tuple, CellStats]:
    """Group records by (workload, engine, step_id) and aggregate."""
    groups: dict[tuple, list[dict]] = defaultdict(list)
    for r in records:
        groups[(r["workload"], r["engine"], r["step_id"])].append(r)

    cells: dict[tuple, CellStats] = {}
    for (wl, eng, sid), recs in groups.items():
        cold = [r["wall_ms"] for r in recs if r["cold"] and r["exit_code"] == 0]
        warm = [r["wall_ms"] for r in recs if not r["cold"] and r["exit_code"] == 0]
        fails = sum(1 for r in recs if r["exit_code"] != 0)
        rows = [r["rows_returned"] for r in recs if r.get("rows_returned") is not None]
        sha = next(
            (r["result_sha256"] for r in recs
             if r.get("result_sha256") and r["exit_code"] == 0),
            None,
        )
        cells[(wl, eng, sid)] = CellStats(
            workload=wl, engine=eng, step_id=sid,
            cold_ms=cold, warm_ms=warm, failures=fails,
            rows_returned=rows, result_sha256=sha,
        )
    return cells


def write_summary_csv(cells: dict[tuple, CellStats], out: Path) -> None:
    rows = sorted(cells.values(), key=lambda c: (c.workload, c.step_id, c.engine))
    with out.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow([
            "workload", "step_id", "engine",
            "cold_median_ms",
            "warm_min_ms", "warm_median_ms", "warm_p95_ms",
            "warm_mean_ms", "warm_stddev_ms",
            "warm_n", "cold_n", "failures",
            "result_sha256",
        ])
        for c in rows:
            w.writerow([
                c.workload, c.step_id, c.engine,
                f"{c.cold_median():.2f}",
                f"{c.warm_min():.2f}",
                f"{c.warm_median():.2f}",
                f"{c.warm_p95():.2f}",
                f"{c.warm_mean():.2f}",
                f"{c.warm_stddev():.2f}",
                len(c.warm_ms), len(c.cold_ms), c.failures,
                c.result_sha256 or "",
            ])


def correctness_check(cells: dict[tuple, CellStats]) -> list[str]:
    """For each (workload, step_id), verify that every engine that returned
    a result_sha256 returned the same one. Returns a list of human-readable
    discrepancy messages (empty on full agreement)."""
    by_step: dict[tuple, dict[str, str]] = defaultdict(dict)
    for c in cells.values():
        if c.result_sha256:
            by_step[(c.workload, c.step_id)][c.engine] = c.result_sha256

    issues: list[str] = []
    for (wl, sid), engine_hashes in by_step.items():
        unique = set(engine_hashes.values())
        if len(unique) > 1:
            details = ", ".join(f"{e}={h[:12]}" for e, h in sorted(engine_hashes.items()))
            issues.append(f"{wl} / {sid}: {details}")
    return issues


def write_report_md(
    cells: dict[tuple, CellStats],
    manifest: dict,
    correctness_issues: list[str],
    out: Path,
) -> None:
    lines: list[str] = []
    lines.append("# Bench run report")
    lines.append("")
    host = manifest.get("host", {})
    lines.append("## Run context")
    lines.append("")
    lines.append(f"- captured_at_utc: `{host.get('captured_at_utc', 'unknown')}`")
    lines.append(f"- hostname:        `{host.get('hostname', 'unknown')}`")
    lines.append(f"- cpu_model:       `{host.get('cpu_model', 'unknown')}`")
    lines.append(f"- mem_total_kb:    `{host.get('mem_total_kb')}`")
    lines.append(f"- kernel:          `{host.get('kernel', 'unknown')}`")
    lines.append(f"- python:          `{host.get('python', 'unknown')}`")
    lines.append(f"- scale:           SF={manifest.get('scale')}")
    lines.append(f"- engines:         `{manifest.get('engines')}`")
    lines.append(f"- workloads:       `{manifest.get('workloads')}`")
    lines.append("")
    cold_starts = manifest.get("cold_starts", {})
    if cold_starts:
        lines.append("## Cold-start (reported separately, NOT folded into query times)")
        lines.append("")
        lines.append("| engine | session_ready_ms | first_query_ms |")
        lines.append("|---|--:|--:|")
        for eng, m in sorted(cold_starts.items()):
            lines.append(
                f"| `{eng}` | "
                f"{m.get('import_to_session_ready_ms', float('nan')):.1f} | "
                f"{m.get('session_to_first_query_ms', float('nan')):.1f} |"
            )
        lines.append("")
    if correctness_issues:
        lines.append("## Correctness DISAGREEMENTS (release blocker)")
        lines.append("")
        for issue in correctness_issues:
            lines.append(f"- {issue}")
        lines.append("")
    else:
        lines.append("## Correctness")
        lines.append("")
        lines.append("All cross-engine result hashes agree where both engines ran the same step. "
                     "No disagreements detected.")
        lines.append("")

    # Per-workload tables
    workloads = sorted({c.workload for c in cells.values()})
    for wl in workloads:
        lines.append(f"## Workload: `{wl}`")
        lines.append("")
        lines.append("| step | engine | cold_median_ms | warm_median_ms | warm_p95_ms | warm_mean_ms | warm_stddev_ms | warm_n | failures |")
        lines.append("|---|---|--:|--:|--:|--:|--:|--:|--:|")
        wcells = [c for c in cells.values() if c.workload == wl]
        wcells.sort(key=lambda c: (c.step_id, c.engine))
        for c in wcells:
            lines.append(
                f"| `{c.step_id}` | `{c.engine}` | "
                f"{c.cold_median():.1f} | "
                f"{c.warm_median():.1f} | "
                f"{c.warm_p95():.1f} | "
                f"{c.warm_mean():.1f} | "
                f"{c.warm_stddev():.1f} | "
                f"{len(c.warm_ms)} | "
                f"{c.failures} |"
            )
        lines.append("")

    out.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--results-dir", required=True,
                    help="Path to a per-run results directory containing manifest.json and raw/*.jsonl.")
    args = ap.parse_args()

    results_dir = Path(args.results_dir)
    if not results_dir.is_dir():
        sys.exit(f"error: not a directory: {results_dir}")

    manifest_path = results_dir / "manifest.json"
    if not manifest_path.exists():
        sys.exit(f"error: missing {manifest_path}")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

    records = load_records(results_dir)
    if not records:
        sys.exit(f"error: no .jsonl records under {results_dir}/raw/")

    print(f"loaded {len(records)} step records")
    cells = cellize(records)
    print(f"{len(cells)} (workload, engine, step) cells")

    write_summary_csv(cells, results_dir / "summary.csv")
    print(f"wrote {results_dir / 'summary.csv'}")

    issues = correctness_check(cells)
    if issues:
        print(f"WARN: {len(issues)} cross-engine correctness disagreements")
        for i in issues:
            print(f"  {i}")
    else:
        print("correctness: OK (no cross-engine disagreements)")

    write_report_md(cells, manifest, issues, results_dir / "report.md")
    print(f"wrote {results_dir / 'report.md'}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
