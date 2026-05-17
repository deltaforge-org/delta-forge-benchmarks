"""Fetch the ClickBench `hits` parquet (~14 GB) + canonical query set.

Source: https://github.com/ClickHouse/ClickBench
        https://datasets.clickhouse.com/hits_compatible/hits.parquet

Run once per host. Bench-runner re-uses the downloaded files.
"""
from __future__ import annotations

import argparse
import sys
import time
import urllib.request
from pathlib import Path

PARQUET_URL = "https://datasets.clickhouse.com/hits_compatible/hits.parquet"
QUERIES_URL = "https://raw.githubusercontent.com/ClickHouse/ClickBench/main/clickhouse/queries.sql"
PARQUET_SHA256 = None  # ClickBench does not publish a stable hash; size-check only.
PARQUET_EXPECTED_BYTES = 14_779_976_446  # current size as of mid-2026.


def fetch(url: str, dest: Path, expected_bytes: int | None = None) -> None:
    if dest.exists() and (expected_bytes is None or dest.stat().st_size == expected_bytes):
        print(f"[skip] {dest} already present ({dest.stat().st_size:,} bytes)")
        return
    print(f"[fetch] {url}\n      -> {dest}")
    t0 = time.perf_counter()
    tmp = dest.with_suffix(dest.suffix + ".part")
    # S3 rejects urllib's default User-Agent with 403. Use a normal one.
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "Mozilla/5.0 (compatible; delta-forge-bench/1.0)"},
    )
    with urllib.request.urlopen(req) as resp, tmp.open("wb") as f:
        total = 0
        while True:
            chunk = resp.read(1 << 20)  # 1 MiB
            if not chunk:
                break
            f.write(chunk)
            total += len(chunk)
            if total % (256 * (1 << 20)) < (1 << 20):
                rate_mb = total / (1024 * 1024) / max(time.perf_counter() - t0, 1e-6)
                print(f"  ...{total:,} bytes  ({rate_mb:.0f} MB/s)")
    tmp.rename(dest)
    dt = time.perf_counter() - t0
    print(f"[done] {dest.stat().st_size:,} bytes in {dt:.1f}s")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", default="/workspace/data/clickbench",
                        help="Where hits.parquet lands.")
    parser.add_argument("--queries-dir",
                        default="/workspace/workloads/clickbench/queries",
                        help="Where the per-query .sql files land.")
    args = parser.parse_args()

    data_dir = Path(args.data_dir)
    data_dir.mkdir(parents=True, exist_ok=True)
    fetch(PARQUET_URL, data_dir / "hits.parquet", PARQUET_EXPECTED_BYTES)

    # Split the upstream queries.sql (one query per line, terminated by ;)
    # into qNN.sql files so the bench-runner can pick them up the same way
    # it picks up TPC-H queries.
    queries_dir = Path(args.queries_dir)
    queries_dir.mkdir(parents=True, exist_ok=True)
    print(f"[fetch] {QUERIES_URL}")
    req = urllib.request.Request(
        QUERIES_URL,
        headers={"User-Agent": "Mozilla/5.0 (compatible; delta-forge-bench/1.0)"},
    )
    raw = urllib.request.urlopen(req).read().decode("utf-8")
    queries = [q.strip() for q in raw.split(";") if q.strip()]
    if len(queries) != 43:
        print(f"[warn] expected 43 queries, got {len(queries)} — writing anyway",
              file=sys.stderr)
    for i, q in enumerate(queries):
        out = queries_dir / f"q{i:02d}.sql"
        out.write_text(q.rstrip() + "\n", encoding="utf-8")
    print(f"[done] {len(queries)} queries written to {queries_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
