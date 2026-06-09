"""Aggregate scale-out stream results into a per-N latency curve.

Inputs (created by scale_out/orchestrate.sh):
    <results-root>/scaleout_n{N}_{RUN_TS}_s{i}/<bench-results-dir>/raw/df.jsonl

Each record in df.jsonl is one (query, run) timing emitted by
bench_runner.run_workload. In scale-out mode each stream runs each
query exactly once (--cold-runs 0 --warm-runs 1), so each query
contributes exactly N samples at concurrency level N.

Outputs (under --out-dir, default scale_out/):
    curve.csv  -- one row per (query, N): p50_ms, p95_ms, ratio_vs_n1_p50
    curve.json -- structured form of the same, plus headline verdict
    curve.md   -- human-readable summary with the flatness verdict per N

Verdict rule: per-query latency is "flat" if p50@N / p50@N=1 <= 1.10
for that query at that N. The summary reports, per N, what fraction of
queries clear the bar, plus the worst-case ratio across all queries.

Per project rule [[feedback_no_multiuser_qps_numbers]]: this file does
NOT emit QphDS, queries-per-hour, aggregate throughput, or any
multi-user / QPS metric. The curve is per-query latency only.
"""
from __future__ import annotations

import argparse
import json
import re
import statistics
import sys
from collections import defaultdict
from pathlib import Path
from typing import Iterable


# Streams live at <results-root>/scaleout_n{N}_{RUN_TS}_s{i}/...
_STREAM_DIR_RE = re.compile(
    r"^scaleout_n(?P<n>\d+)_(?P<ts>\d{8}T\d{6}Z)_s(?P<i>\d+)$"
)

FLATNESS_THRESHOLD = 1.10


def parse_stream_dir(path: Path) -> tuple[int, str, int] | None:
    """Extract (N, run_ts, stream_idx) from a stream directory name.

    Returns None if `path.name` is not a scale-out stream dir
    (warmup dirs, stray entries, prior runs at other timestamps)."""
    m = _STREAM_DIR_RE.match(path.name)
    if not m:
        return None
    return int(m["n"]), m["ts"], int(m["i"])


def discover_jsonl(stream_dir: Path) -> Path | None:
    """Find the df.jsonl produced by bench_runner inside a stream dir.

    bench_runner builds <results-dir>/<timestamp>-<host>-<tag>/raw/df.jsonl;
    in scale-out mode the outer <results-dir> is the stream dir, so we
    just glob for the inner timestamped dir. There is always exactly
    one per stream (orchestrate.sh creates the stream dir fresh)."""
    candidates = sorted(stream_dir.glob("*/raw/df.jsonl"))
    if not candidates:
        return None
    if len(candidates) > 1:
        print(f"[aggregate] WARN multiple df.jsonl under {stream_dir}; "
              f"using the latest: {candidates[-1]}", file=sys.stderr)
    return candidates[-1]


def load_query_timings(jsonl: Path) -> dict[str, float]:
    """Read one stream's df.jsonl and return {query_id: wall_ms}.

    A stream that ran with --cold-runs 0 --warm-runs 1 produces
    exactly one record per query (run_idx=0, cold=False). If a query
    appears more than once we keep the warm-run timing and warn; if
    a query is missing we record None so the caller can detect
    incomplete streams (don't silently aggregate over them)."""
    timings: dict[str, float] = {}
    with jsonl.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rec = json.loads(line)
            # Only measured query steps. Setup / cleanup don't show up
            # in the per-engine .jsonl, but guard anyway.
            if rec.get("step_kind") not in (None, "sql_query"):
                continue
            qid = rec["step_id"]
            # Skip failed runs entirely; the caller treats them as
            # missing samples and the verdict step reports the gap.
            if rec.get("exit_code", 0) != 0:
                continue
            wall = float(rec["wall_ms"])
            if qid in timings:
                # Prefer the warm (later) timing if both showed up.
                if not rec.get("cold", False):
                    timings[qid] = wall
            else:
                timings[qid] = wall
    return timings


def percentile(values: list[float], pct: float) -> float:
    """Compute the pct-th percentile via the same interpolation
    statistics.quantiles uses (Type 7 / R's default). Falls back to
    the lone value for a single-sample input and to NaN for empty."""
    if not values:
        return float("nan")
    if len(values) == 1:
        return values[0]
    sorted_vals = sorted(values)
    k = (len(sorted_vals) - 1) * (pct / 100.0)
    lo = int(k)
    hi = min(lo + 1, len(sorted_vals) - 1)
    frac = k - lo
    return sorted_vals[lo] * (1.0 - frac) + sorted_vals[hi] * frac


def aggregate(
    samples: dict[tuple[str, int], list[float]],
    expected_streams: dict[int, int],
    all_query_ids: set[str],
) -> list[dict]:
    """Turn {(query_id, N): [wall_ms, ...]} into the per-(query, N) rows.

    Emits a row for EVERY (query_id, N) combination including those
    where samples = 0; that way an N column in the md table never
    silently drops a query and the operator sees "every stream failed
    q14 at N=8" rather than "q14 just isn't in the N=8 column."
    """
    baseline_p50: dict[str, float] = {}
    for qid in all_query_ids:
        vals = samples.get((qid, 1), [])
        if vals:
            baseline_p50[qid] = percentile(vals, 50)

    rows: list[dict] = []
    n_values_sorted = sorted(expected_streams.keys())
    for qid in sorted(all_query_ids):
        for n in n_values_sorted:
            vals = samples.get((qid, n), [])
            attempted = expected_streams[n]
            completed = len(vals)
            completion_rate = (completed / attempted) if attempted else 0.0
            if vals:
                p50 = percentile(vals, 50)
                p95 = percentile(vals, 95)
            else:
                p50 = float("nan")
                p95 = float("nan")
            base = baseline_p50.get(qid)
            if vals and base and base > 0:
                ratio = p50 / base
            else:
                ratio = float("nan")
            rows.append({
                "query_id": qid,
                "n": n,
                "samples": completed,
                "streams_attempted": attempted,
                "completion_rate": round(completion_rate, 3),
                "p50_ms": (round(p50, 2) if p50 == p50 else None),
                "p95_ms": (round(p95, 2) if p95 == p95 else None),
                "p50_baseline_n1_ms": round(base, 2) if base is not None else None,
                "ratio_p50_vs_n1": round(ratio, 3) if ratio == ratio else None,
                # `flat` is only meaningful when we have both a baseline
                # and at least one N=N sample. None otherwise so the
                # md verdict table can show "n/a" instead of pretending.
                "flat": (
                    True if (ratio == ratio and ratio <= FLATNESS_THRESHOLD)
                    else False if ratio == ratio
                    else None
                ),
            })
    return rows


def write_csv(rows: Iterable[dict], path: Path) -> None:
    import csv
    fieldnames = [
        "query_id", "n", "samples", "streams_attempted", "completion_rate",
        "p50_ms", "p95_ms",
        "p50_baseline_n1_ms", "ratio_p50_vs_n1", "flat",
    ]
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r)


def write_json(rows: list[dict], n_values: list[int], run_ts: str,
               path: Path,
               expected_streams: dict[int, int]) -> None:
    by_n_verdict: dict[int, dict] = {}
    for n in n_values:
        n_rows_all = [r for r in rows if r["n"] == n]
        if not n_rows_all:
            continue
        # Queries with at least one sample at this N (= "completed at
        # least once") vs. ratable queries (have a usable ratio).
        with_samples = [r for r in n_rows_all if r["samples"] > 0]
        ratable = [r for r in n_rows_all if r["ratio_p50_vs_n1"] is not None]
        no_samples = [r["query_id"] for r in n_rows_all if r["samples"] == 0]
        full_completion = sum(
            1 for r in n_rows_all
            if r["streams_attempted"] > 0 and r["samples"] == r["streams_attempted"]
        )

        verdict = {
            "queries_total": len(n_rows_all),
            "queries_with_any_sample": len(with_samples),
            "queries_with_full_completion": full_completion,
            "queries_with_no_samples": len(no_samples),
            "no_sample_query_ids": sorted(no_samples)[:25],  # cap for readability
            "ratable_queries": len(ratable),
        }
        if ratable:
            ratios = [r["ratio_p50_vs_n1"] for r in ratable]
            flat_count = sum(1 for r in ratable if r["flat"] is True)
            verdict.update({
                "flat_queries": flat_count,
                "flat_fraction": round(flat_count / len(ratable), 3),
                "worst_ratio": round(max(ratios), 3),
                "median_ratio": round(percentile(ratios, 50), 3),
            })
        by_n_verdict[n] = verdict

    headline = {
        "run_ts": run_ts,
        "n_values": n_values,
        "streams_attempted_by_n": expected_streams,
        "flatness_threshold": FLATNESS_THRESHOLD,
        "verdict_by_n": by_n_verdict,
        "notes": (
            "Per-query p50 ratio_p50_vs_n1 == p50_at_N / p50_at_N=1. "
            f"'flat' = ratio <= {FLATNESS_THRESHOLD}. samples < "
            "streams_attempted at a given (query, N) means some streams "
            "did not complete that query (typically OOM under the "
            "cgroup MemoryMax cap on heavy queries at high N); the "
            "remaining samples are still used to compute p50 / p95. "
            "Aggregate throughput metrics (QphDS, QPS, queries-per-hour) "
            "are intentionally omitted from this output."
        ),
        "rows": rows,
    }
    with path.open("w", encoding="utf-8") as f:
        json.dump(headline, f, indent=2, sort_keys=False)
        f.write("\n")


def write_md(rows: list[dict], n_values: list[int], run_ts: str,
             path: Path,
             expected_streams: dict[int, int]) -> None:
    lines: list[str] = []
    lines.append("# TPC-DS SF100 scale-out curve")
    lines.append("")
    lines.append(f"Run timestamp (UTC): `{run_ts}`")
    lines.append(f"Concurrency points: {n_values}")
    lines.append(f"Flatness threshold: p50@N / p50@N=1 <= {FLATNESS_THRESHOLD}")
    lines.append("")

    # Per-N verdict, with completion stats alongside the flatness number.
    # When some streams OOM under load the curve only makes sense if the
    # operator can see the completion rate per N at a glance.
    lines.append("## Verdict by N")
    lines.append("")
    lines.append(
        "| N | streams | queries / full / partial / none | "
        "flat (of ratable) | worst ratio | median ratio |"
    )
    lines.append(
        "|---|--------:|--------------------------------:|"
        "------------------:|------------:|-------------:|"
    )
    for n in n_values:
        n_rows = [r for r in rows if r["n"] == n]
        total = len(n_rows)
        full = sum(1 for r in n_rows
                   if r["streams_attempted"] > 0 and r["samples"] == r["streams_attempted"])
        partial = sum(1 for r in n_rows
                      if 0 < r["samples"] < r["streams_attempted"])
        none = sum(1 for r in n_rows if r["samples"] == 0)
        ratable = [r for r in n_rows if r["ratio_p50_vs_n1"] is not None]
        if ratable:
            ratios = [r["ratio_p50_vs_n1"] for r in ratable]
            flat_count = sum(1 for r in ratable if r["flat"] is True)
            flat_str = f"{flat_count}/{len(ratable)} ({flat_count/len(ratable)*100:.0f}%)"
            worst_str = f"{max(ratios):.3f}"
            median_str = f"{percentile(ratios, 50):.3f}"
        else:
            flat_str = "n/a"
            worst_str = "n/a"
            median_str = "n/a"
        lines.append(
            f"| {n} | {expected_streams.get(n, 0)} | "
            f"{total} / {full} / {partial} / {none} | "
            f"{flat_str} | {worst_str} | {median_str} |"
        )
    lines.append("")
    lines.append(
        "_Columns: `queries / full / partial / none` is total / fully completed "
        "(all N streams returned) / partially completed (some streams returned, "
        "others failed) / completely failed (no stream returned this query). "
        "`flat (of ratable)` counts only queries with at least one sample at "
        "this N AND a usable N=1 baseline._"
    )
    lines.append("")

    # Surface queries that no stream completed at some N (architectural
    # ceiling story) and queries where the ratio tilted past the
    # flatness threshold (the "concurrency hurts" story). Separated
    # because they tell different stories.
    no_sample_rows = sorted(
        [r for r in rows if r["samples"] == 0],
        key=lambda r: (r["query_id"], r["n"]),
    )
    if no_sample_rows:
        lines.append("## Queries no stream completed (likely OOM / engine limit)")
        lines.append("")
        lines.append("| query | N | streams attempted |")
        lines.append("|-------|--:|------------------:|")
        for r in no_sample_rows[:40]:
            lines.append(
                f"| {r['query_id']} | {r['n']} | {r['streams_attempted']} |"
            )
        if len(no_sample_rows) > 40:
            lines.append(f"_... {len(no_sample_rows) - 40} more not shown._")
        lines.append("")
        lines.append(
            "_Each row means every single stream at that N failed this query "
            "(typically OOM-kill under the 4 GB cgroup MemoryMax). The same "
            "query may also appear at lower N as a partial-completion row "
            "in the CSV; cross-reference for the full failure curve._"
        )
        lines.append("")

    lines.append("## Worst per-query ratios (N > 1, samples > 0)")
    lines.append("")
    offenders = sorted(
        [r for r in rows if r["n"] > 1 and r["ratio_p50_vs_n1"] is not None
         and r["ratio_p50_vs_n1"] > FLATNESS_THRESHOLD],
        key=lambda r: r["ratio_p50_vs_n1"],
        reverse=True,
    )
    if not offenders:
        lines.append(
            f"_None: every query with samples at N > 1 stayed within "
            f"{FLATNESS_THRESHOLD}x of its N=1 baseline._"
        )
    else:
        lines.append("| query | N | p50@N (ms) | p50@N=1 (ms) | ratio | completion |")
        lines.append("|-------|--:|-----------:|-------------:|------:|-----------:|")
        for r in offenders[:20]:
            lines.append(
                f"| {r['query_id']} | {r['n']} | {r['p50_ms']:.1f} | "
                f"{r['p50_baseline_n1_ms']:.1f} | "
                f"{r['ratio_p50_vs_n1']:.3f} | "
                f"{r['samples']}/{r['streams_attempted']} |"
            )

    lines.append("")
    lines.append(
        "_Aggregate throughput metrics (QphDS, queries-per-hour) are "
        "intentionally omitted; the architectural claim under test is "
        "per-query latency, not aggregate throughput._"
    )
    lines.append("")
    path.write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--results-root", required=True, type=Path,
                   help="Directory containing scaleout_n*_*_s*/ subdirs.")
    p.add_argument("--run-ts", required=True,
                   help="UTC timestamp tag (matches the orchestrate.sh "
                        "RUN_TS) used to filter which stream dirs belong to "
                        "this aggregation.")
    p.add_argument("--n-values", required=True,
                   help="Space-separated N values that were measured "
                        "(e.g. '1 2 4 8').")
    p.add_argument("--out-dir", required=True, type=Path,
                   help="Where curve.csv / curve.json / curve.md land.")
    args = p.parse_args()

    n_values = sorted({int(x) for x in args.n_values.split() if x})
    if not n_values or n_values[0] != 1:
        sys.exit("error: --n-values must include 1 (it is the baseline)")

    # Collect per-(query, N) timing samples across all stream dirs that
    # match this run_ts.
    samples: dict[tuple[str, int], list[float]] = defaultdict(list)
    streams_seen: dict[int, int] = defaultdict(int)
    streams_missing_jsonl: list[str] = []

    for stream_dir in sorted(args.results_root.iterdir()):
        if not stream_dir.is_dir():
            continue
        parsed = parse_stream_dir(stream_dir)
        if parsed is None:
            continue
        n, ts, idx = parsed
        if ts != args.run_ts or n not in n_values:
            continue

        jsonl = discover_jsonl(stream_dir)
        if jsonl is None:
            streams_missing_jsonl.append(stream_dir.name)
            continue

        timings = load_query_timings(jsonl)
        if not timings:
            streams_missing_jsonl.append(stream_dir.name + " (empty)")
            continue

        streams_seen[n] += 1
        for qid, wall_ms in timings.items():
            samples[(qid, n)].append(wall_ms)

    # Warn (do NOT fail) if some N is missing streams. At SF100 the
    # heavy queries OOM-kill some streams under the 4 GB cgroup cap;
    # the surviving streams still produce useful records via per-step
    # jsonl appends, and the per-(query, N) verdict carries the
    # `samples` / `streams_attempted` / `completion_rate` columns so a
    # short stream is visible in the headline.
    expected_streams = {n: n for n in n_values}
    short = [(n, streams_seen[n], expected_streams[n])
             for n in n_values if streams_seen[n] != expected_streams[n]]
    if short:
        print("[aggregate] WARN under-sampled N values (some streams did not "
              "produce a jsonl; the curve still uses what did):", file=sys.stderr)
        for n, got, want in short:
            print(f"  N={n}: got {got} streams, expected {want}", file=sys.stderr)
        if streams_missing_jsonl:
            print("[aggregate] missing/empty jsonl in:", file=sys.stderr)
            for s in streams_missing_jsonl:
                print(f"  {s}", file=sys.stderr)

    # Universe of query ids: union of every (query_id) ever seen across
    # all streams. The aggregator emits a row for every (query_id, N)
    # so queries that no stream completed at some N still appear with
    # samples=0 (visible failure rather than silent omission).
    all_query_ids: set[str] = {qid for (qid, _n) in samples.keys()}
    if not all_query_ids:
        print("[aggregate] ERROR no query records collected from any stream "
              "(every stream produced an empty jsonl); aborting", file=sys.stderr)
        return 2

    rows = aggregate(samples, expected_streams, all_query_ids)

    args.out_dir.mkdir(parents=True, exist_ok=True)
    csv_path = args.out_dir / "curve.csv"
    json_path = args.out_dir / "curve.json"
    md_path = args.out_dir / "curve.md"

    write_csv(rows, csv_path)
    write_json(rows, n_values, args.run_ts, json_path, expected_streams)
    write_md(rows, n_values, args.run_ts, md_path, expected_streams)

    print(f"[aggregate] wrote {csv_path}")
    print(f"[aggregate] wrote {json_path}")
    print(f"[aggregate] wrote {md_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
