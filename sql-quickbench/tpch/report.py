"""Generate a TPC-H comparison report (Markdown + PNG chart) from results.csv.

Reads results.csv (one row per engine/query/run), aggregates per (engine,
query) into mean / median / p95 / stdev / range, writes:

    report.md   - human-readable tables + interpretation
    report.png  - paired-bar chart (DF vs DuckDB) with error bars,
                  plus a ratio panel below

Usage:
    python report.py
    python report.py --note "post-morsel"
    python report.py --scale-factor 10 --timestamp 2026-05-02T03:00:00
    python report.py --out-dir /tmp/tpch-report

By default reports the most recent timestamp in the CSV. Filters narrow that
selection rather than aggregating across runs from different timestamps,
which would be misleading.
"""
from __future__ import annotations

import argparse
import csv
import statistics
import sys
from collections import defaultdict
from pathlib import Path

try:
    import matplotlib.pyplot as plt
    import matplotlib.ticker as mtick
except ImportError:
    print("ERROR: matplotlib is required. Install via: pip install matplotlib", file=sys.stderr)
    sys.exit(2)


def load_rows(path: Path) -> list[dict]:
    out: list[dict] = []
    with path.open(newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                row["seconds_f"] = float(row["seconds"])
            except (ValueError, TypeError, KeyError):
                row["seconds_f"] = None
            out.append(row)
    return out


def filter_rows(rows, *, scale_factor=None, note=None, timestamp=None) -> list[dict]:
    out = []
    for r in rows:
        if r.get("seconds_f") is None:
            continue
        if scale_factor is not None and r.get("scale_factor") != str(scale_factor):
            continue
        if note is not None and note not in (r.get("note") or ""):
            continue
        if timestamp is not None and r["timestamp"] != timestamp:
            continue
        out.append(r)
    return out


def percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    s = sorted(values)
    if len(s) == 1:
        return s[0]
    idx = max(0, min(len(s) - 1, int(round(pct * (len(s) - 1)))))
    return s[idx]


def aggregate(rows: list[dict]) -> dict:
    bucket: dict[tuple[str, str], list[float]] = defaultdict(list)
    for r in rows:
        bucket[(r["engine"], r["query"])].append(r["seconds_f"])

    out: dict[tuple[str, str], dict] = {}
    for key, times in bucket.items():
        n = len(times)
        out[key] = {
            "n": n,
            "min": min(times),
            "max": max(times),
            "mean": statistics.mean(times),
            "median": statistics.median(times),
            "p95": percentile(times, 0.95),
            "stdev": statistics.stdev(times) if n > 1 else 0.0,
            "samples": times,
        }
    return out


def render_markdown(agg, *, scale_factor, note, sha, ts, out_path: Path) -> Path:
    queries = sorted({q for (_, q) in agg})
    engines = ["df", "duck"]

    lines: list[str] = []
    lines.append("# TPC-H benchmark report")
    lines.append("")
    lines.append(f"- scale factor: SF{scale_factor}")
    lines.append(f"- timestamp:    {ts}")
    lines.append(f"- git sha:      {sha}")
    if note:
        lines.append(f"- note:         {note}")
    lines.append("")

    lines.append("## Summary (mean wall time, warm cache)")
    lines.append("")
    lines.append("| Query | DF mean (s) | DuckDB mean (s) | DF / DuckDB | DF p95 (s) | DuckDB p95 (s) | DF n | DuckDB n |")
    lines.append("|---|---:|---:|---:|---:|---:|---:|---:|")
    for q in queries:
        df = agg.get(("df", q))
        dk = agg.get(("duck", q))
        if df and dk and dk["mean"] > 0:
            ratio_s = f"{df['mean'] / dk['mean']:.2f}x"
        else:
            ratio_s = "n/a"
        df_mean = f"{df['mean']:.3f}" if df else "FAIL"
        dk_mean = f"{dk['mean']:.3f}" if dk else "FAIL"
        df_p95  = f"{df['p95']:.3f}"  if df else "FAIL"
        dk_p95  = f"{dk['p95']:.3f}"  if dk else "FAIL"
        df_n = df["n"] if df else 0
        dk_n = dk["n"] if dk else 0
        lines.append(f"| {q} | {df_mean} | {dk_mean} | {ratio_s} | {df_p95} | {dk_p95} | {df_n} | {dk_n} |")

    lines.append("")
    lines.append("## Per-engine detail")
    lines.append("")
    lines.append("| Query | Engine | n | min | mean | median | p95 | max | stdev |")
    lines.append("|---|---|---:|---:|---:|---:|---:|---:|---:|")
    for q in queries:
        for eng in engines:
            x = agg.get((eng, q))
            if not x:
                lines.append(f"| {q} | {eng} | 0 | - | - | - | - | - | - |")
                continue
            lines.append(
                f"| {q} | {eng} | {x['n']} | {x['min']:.3f} | {x['mean']:.3f} | "
                f"{x['median']:.3f} | {x['p95']:.3f} | {x['max']:.3f} | {x['stdev']:.3f} |"
            )

    lines.append("")
    lines.append("## Ratios at a glance")
    lines.append("")
    ratios = []
    for q in queries:
        df = agg.get(("df", q))
        dk = agg.get(("duck", q))
        if df and dk and dk["mean"] > 0:
            ratios.append(df["mean"] / dk["mean"])
    if ratios:
        lines.append(f"- min ratio:    {min(ratios):.2f}x")
        lines.append(f"- max ratio:    {max(ratios):.2f}x")
        lines.append(f"- mean ratio:   {statistics.mean(ratios):.2f}x")
        lines.append(f"- median ratio: {statistics.median(ratios):.2f}x")

    out_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return out_path


def render_png(agg, *, scale_factor, ts, out_path: Path) -> Path:
    queries = sorted({q for (_, q) in agg})
    n_q = len(queries)

    df_means, duck_means = [], []
    df_errs,  duck_errs  = [], []
    ratios = []
    for q in queries:
        df = agg.get(("df", q))
        dk = agg.get(("duck", q))
        df_means.append(df["mean"]  if df else 0.0)
        duck_means.append(dk["mean"] if dk else 0.0)
        df_errs.append(df["stdev"]  if df else 0.0)
        duck_errs.append(dk["stdev"] if dk else 0.0)
        if df and dk and dk["mean"] > 0:
            ratios.append(df["mean"] / dk["mean"])
        else:
            ratios.append(0.0)

    plt.style.use("default")
    fig, (ax_top, ax_bot) = plt.subplots(
        2, 1, figsize=(11, 8), gridspec_kw={"height_ratios": [3, 1]}
    )

    x = list(range(n_q))
    width = 0.38

    df_color   = "#c84630"  # DeltaForge accent (copper/red)
    duck_color = "#fff100"  # DuckDB yellow

    bars_df = ax_top.bar(
        [i - width / 2 for i in x], df_means, width,
        yerr=df_errs, label="DeltaForge", color=df_color,
        edgecolor="#5c1a10", capsize=4,
        error_kw={"elinewidth": 1.2, "ecolor": "#5c1a10"},
    )
    bars_dk = ax_top.bar(
        [i + width / 2 for i in x], duck_means, width,
        yerr=duck_errs, label="DuckDB", color=duck_color,
        edgecolor="#9a8a00", capsize=4,
        error_kw={"elinewidth": 1.2, "ecolor": "#9a8a00"},
    )

    ax_top.set_ylabel("Wall time (s, warm cache, lower is better)")
    ax_top.set_title(f"TPC-H SF{scale_factor}: DeltaForge vs DuckDB    ({ts})")
    ax_top.set_xticks(x)
    ax_top.set_xticklabels([q.upper() for q in queries])
    ax_top.legend(loc="upper left")
    ax_top.grid(axis="y", linestyle="--", alpha=0.4)
    ax_top.yaxis.set_major_formatter(mtick.FormatStrFormatter("%.2f"))

    for bar, val in zip(bars_df, df_means):
        ax_top.annotate(
            f"{val:.2f}s",
            xy=(bar.get_x() + bar.get_width() / 2, bar.get_height()),
            xytext=(0, 4), textcoords="offset points", ha="center", fontsize=8,
        )
    for bar, val in zip(bars_dk, duck_means):
        ax_top.annotate(
            f"{val:.2f}s",
            xy=(bar.get_x() + bar.get_width() / 2, bar.get_height()),
            xytext=(0, 4), textcoords="offset points", ha="center", fontsize=8,
        )

    ax_bot.bar(x, ratios, width=0.6, color="#3a3a3a", edgecolor="black")
    ax_bot.axhline(y=1, color="#1f8b4c", linestyle="--", linewidth=1.2, alpha=0.8,
                   label="parity")
    ax_bot.set_ylabel("DF / DuckDB ratio")
    ax_bot.set_xticks(x)
    ax_bot.set_xticklabels([q.upper() for q in queries])
    ax_bot.grid(axis="y", linestyle="--", alpha=0.4)
    ax_bot.legend(loc="upper right", fontsize=8)
    for i, val in enumerate(ratios):
        if val > 0:
            ax_bot.annotate(
                f"{val:.2f}x",
                xy=(i, val), xytext=(0, 3), textcoords="offset points",
                ha="center", fontsize=8,
            )

    plt.tight_layout()
    plt.savefig(out_path, dpi=140, bbox_inches="tight")
    plt.close(fig)
    return out_path


def main():
    parser = argparse.ArgumentParser(description="TPC-H benchmark report generator")
    parser.add_argument("--results",      default=None,
                        help="path to results.csv (default: ./results.csv next to this script)")
    parser.add_argument("--scale-factor", type=int, default=None,
                        help="filter to a specific scale factor")
    parser.add_argument("--note",         default=None,
                        help="filter to runs whose note contains this substring")
    parser.add_argument("--timestamp",    default=None,
                        help="filter to a specific timestamp (default: latest matching)")
    parser.add_argument("--out-dir",      default=None,
                        help="report output directory (default: alongside results.csv)")
    args = parser.parse_args()

    here = Path(__file__).parent
    results_path = Path(args.results) if args.results else here / "results.csv"
    if not results_path.exists():
        print(f"ERROR: results.csv not found at {results_path}", file=sys.stderr)
        sys.exit(2)

    rows = load_rows(results_path)
    rows = [r for r in rows if r.get("seconds_f") is not None]
    if not rows:
        print("ERROR: results.csv has no successful runs", file=sys.stderr)
        sys.exit(2)

    candidates = rows
    if args.scale_factor is not None:
        candidates = [r for r in candidates if r.get("scale_factor") == str(args.scale_factor)]
    if args.note is not None:
        candidates = [r for r in candidates if args.note in (r.get("note") or "")]
    if not candidates:
        print("ERROR: no rows match the given filters", file=sys.stderr)
        sys.exit(2)

    timestamp = args.timestamp or max(r["timestamp"] for r in candidates)
    selected = filter_rows(rows, scale_factor=args.scale_factor, note=args.note,
                           timestamp=timestamp)
    if not selected:
        print("ERROR: no rows match the resolved filters", file=sys.stderr)
        sys.exit(2)

    sha  = selected[0].get("git_sha", "unknown")
    sf   = selected[0].get("scale_factor", "?")
    note = selected[0].get("note") or None

    out_dir = Path(args.out_dir) if args.out_dir else results_path.parent
    out_dir.mkdir(parents=True, exist_ok=True)

    agg = aggregate(selected)
    md_path  = render_markdown(agg, scale_factor=sf, note=note, sha=sha, ts=timestamp,
                               out_path=out_dir / "report.md")
    png_path = render_png(agg, scale_factor=sf, ts=timestamp,
                          out_path=out_dir / "report.png")

    print(f"wrote {md_path}")
    print(f"wrote {png_path}")
    print(f"  {len(selected)} rows from timestamp {timestamp} (sha={sha}, sf={sf})")


if __name__ == "__main__":
    main()
