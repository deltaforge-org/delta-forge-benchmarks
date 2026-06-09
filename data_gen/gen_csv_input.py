"""One-time CSV input generator for the csv_to_delta write bench.

Converts the existing TPC-H parquet (lineitem, the largest table at any
scale factor) into a single CSV file the three engines can each ingest.

CSV is the deliberately neutral source format: no engine has a structural
advantage over another, and the write step is the realistic "land raw
external data into a lakehouse table" operation.

Usage:
    python data_gen/gen_csv_input.py --scale 1 [--table lineitem]
"""
from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scale", type=int, default=1,
                        help="TPC-H scale factor (matches data/tpch_sf<N>/).")
    parser.add_argument("--table", default="lineitem",
                        help="TPC-H table to convert (default: lineitem).")
    parser.add_argument("--out-dir", default="/workspace/data/csv_input",
                        help="Where the .csv lands.")
    args = parser.parse_args()

    src = Path(f"/workspace/data/tpch_sf{args.scale}/{args.table}.parquet")
    if not src.exists():
        print(f"ERROR: source parquet missing: {src}", file=sys.stderr)
        return 1

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    out = out_dir / f"{args.table}.csv"

    import duckdb
    con = duckdb.connect(":memory:")
    t0 = time.perf_counter()
    # DuckDB's COPY ... TO does not accept parameterized paths; inline.
    src_sql = str(src).replace("'", "''")
    out_sql = str(out).replace("'", "''")
    con.execute(
        f"COPY (SELECT * FROM read_parquet('{src_sql}')) "
        f"TO '{out_sql}' (FORMAT CSV, HEADER TRUE)"
    )
    dt = time.perf_counter() - t0
    size = out.stat().st_size
    rows = con.execute(f"SELECT count(*) FROM read_parquet('{src_sql}')").fetchone()[0]
    print(f"wrote {out} rows={rows:,} bytes={size:,} in {dt:.2f}s")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
