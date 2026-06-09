#!/usr/bin/env python3
"""Per-query warm-median summary for a bench run.

Compares every engine that wrote a raw/<engine>.jsonl file in the run dir.
Emits a side-by-side warm-median table plus per-engine aggregates.
"""
from __future__ import annotations
import json
import statistics
import sys
from pathlib import Path


def _load(jsonl: Path) -> dict[str, list[tuple[bool, float]]]:
    """Pick the fairest timing per record: engine-reported when present,
    wall otherwise. df's wall includes the CLI spawn + auth + HTTP round
    trip; engine_reported_ms is the server-side execution time and is the
    comparable figure against Spark/DuckDB (both in-process)."""
    by_q: dict[str, list[tuple[bool, float]]] = {}
    for line in jsonl.open():
        r = json.loads(line)
        if r.get("error") or r.get("exit_code", 0) != 0:
            continue
        if r.get("step_kind") != "sql_query":
            continue
        elapsed = r.get("engine_reported_ms")
        if elapsed is None:
            elapsed = r["wall_ms"]
        by_q.setdefault(r["step_id"], []).append((r["cold"], float(elapsed)))
    return by_q


def _warm_median(runs: list[tuple[bool, float]]) -> float | None:
    warm = [e for c, e in runs if not c]
    return statistics.median(warm) if warm else None


def main(run_dir: str) -> None:
    raw = Path(run_dir) / "raw"
    files = sorted(raw.glob("*.jsonl"))
    if not files:
        sys.exit(f"no *.jsonl under {raw}")

    engines = [f.stem for f in files]
    per_engine = {f.stem: _load(f) for f in files}

    all_queries = sorted({q for d in per_engine.values() for q in d})

    name_col = max(8, max(len(e) for e in engines) + 2)
    col = max(12, name_col)
    hdr = f"{'query':<10}" + "".join(f"{e:>{col}}" for e in engines)
    print("Warm-median execution time (ms), per engine — server-side for df, wall for in-process engines")
    print(hdr)
    print("-" * len(hdr))
    totals: dict[str, list[float]] = {e: [] for e in engines}
    for q in all_queries:
        row = f"{q:<10}"
        for e in engines:
            runs = per_engine[e].get(q, [])
            med = _warm_median(runs)
            if med is None:
                row += f"{'-':>{col}}"
            else:
                row += f"{med:>{col}.2f}"
                totals[e].extend(x for c, x in runs if not c)
        print(row)
    print("-" * len(hdr))

    # Aggregate row across all warm runs.
    agg = f"{'median':<10}"
    for e in engines:
        v = statistics.median(totals[e]) if totals[e] else None
        agg += f"{'-' if v is None else f'{v:.2f}':>{col}}"
    print(agg)

    # Speedup vs df.
    if "df" in engines:
        print()
        print("Speedup of warm median vs df (higher = faster than df):")
        df_med = {q: _warm_median(per_engine["df"].get(q, [])) for q in all_queries}
        hdr2 = f"{'query':<10}" + "".join(f"{e:>{col}}" for e in engines if e != "df")
        print(hdr2)
        print("-" * len(hdr2))
        for q in all_queries:
            base = df_med.get(q)
            row = f"{q:<10}"
            for e in engines:
                if e == "df":
                    continue
                other = _warm_median(per_engine[e].get(q, []))
                if base is None or other is None or other == 0:
                    row += f"{'-':>{col}}"
                else:
                    row += f"{base / other:>{col}.2f}x"
            print(row)


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "/workspace/results")
