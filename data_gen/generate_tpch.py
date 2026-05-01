#!/usr/bin/env python3
"""Generate TPC-H data deterministically using DuckDB's `tpch` extension.

Why DuckDB and not the official TPC dbgen:
- DuckDB's tpch extension is MIT-licensed and ships its own reproducible
  port of dbgen. No registration with TPC required.
- It produces canonical TPC-H tables at any scale factor with a fixed seed.
- DuckDB is also our Parquet writer, so the data path is one tool.

The 8 tables (`lineitem`, `orders`, `customer`, `supplier`, `part`,
`partsupp`, `nation`, `region`) are generated in-process and exported to
`<out>/tpch_sf<scale>/<table>.parquet`. Both engines read the same Parquet
bytes; SHA-256 of every file lands in `manifest.json` so reviewers can
verify their copy matches.

This is the only ingest path the benchmark uses. There is no streaming
input, no CDC source, no time-varying data.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent

TPCH_TABLES = [
    "lineitem", "orders", "customer", "supplier",
    "part", "partsupp", "nation", "region",
]


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def generate(scale: int, out_dir: Path) -> dict:
    """Generate the 8 TPC-H tables at `scale` into `out_dir` as Parquet.
    Returns a manifest dict with row counts, byte sizes, and SHA-256s."""
    import duckdb  # local import: not needed for --help

    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"[generate_tpch] scale={scale} out={out_dir}")
    started = time.perf_counter()

    con = duckdb.connect(database=":memory:")
    con.execute("INSTALL tpch")
    con.execute("LOAD tpch")
    con.execute(f"CALL dbgen(sf = {scale})")

    manifest = {
        "scale": scale,
        "out_dir": str(out_dir),
        "duckdb_version": duckdb.__version__,
        "tables": {},
    }

    for table in TPCH_TABLES:
        path = out_dir / f"{table}.parquet"
        # COMPRESSION SNAPPY is DuckDB's default for parquet, set explicitly
        # so the bytes are reproducible across DuckDB versions where the
        # default could shift.
        con.execute(
            f"COPY {table} TO '{path}' "
            f"(FORMAT PARQUET, COMPRESSION SNAPPY)"
        )
        rows = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        size = path.stat().st_size
        sha = sha256_file(path)
        manifest["tables"][table] = {
            "path": str(path),
            "rows": rows,
            "bytes": size,
            "sha256": sha,
        }
        print(f"[generate_tpch] {table:9s} rows={rows:>12,d} bytes={size:>12,d}  {sha[:12]}")

    con.close()

    elapsed = time.perf_counter() - started
    manifest["elapsed_seconds"] = round(elapsed, 3)
    print(f"[generate_tpch] done in {elapsed:.1f}s")

    manifest_path = out_dir / "manifest.json"
    with manifest_path.open("w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, sort_keys=True)
        f.write("\n")
    print(f"[generate_tpch] manifest: {manifest_path}")

    return manifest


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--scale", type=int, default=1,
                    help="TPC-H scale factor (1, 10, 100). Larger needs more disk.")
    ap.add_argument("--out", default=str(REPO_ROOT / "data"),
                    help="Output root. Files land under <out>/tpch_sf<scale>/.")
    ap.add_argument("--force", action="store_true",
                    help="Regenerate even if the target directory already has parquet files.")
    args = ap.parse_args()

    out_dir = Path(args.out) / f"tpch_sf{args.scale}"

    if not args.force and out_dir.exists():
        existing = list(out_dir.glob("*.parquet"))
        if len(existing) >= len(TPCH_TABLES):
            print(f"[generate_tpch] {out_dir} already has {len(existing)} parquet files; pass --force to regenerate")
            return 0

    generate(args.scale, out_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
