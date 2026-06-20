"""Generate the TPC-DS fixture as plain Delta tables.

Run once per (host, scale) tuple; the output persists across bench runs.
Output directory layout:

    /workspace/data/tpcds_sf{scale}_delta/
        call_center/           (Delta dir)
        catalog_page/
        catalog_returns/
        ... 24 tables total ...

Same "plain Delta" protocol as generate_tpch_delta.py: deletion vectors,
column mapping, and row tracking are all explicitly disabled, so DuckDB's
read-only `delta` extension can read these tables alongside df and Spark.

Pipeline (avoids needing a separate TPC-DS tooling install on the host):

  1. DuckDB's `tpcds` extension runs `dsdgen` in-process, populating 24
     temp tables in a memory DuckDB instance.
  2. Each table is exported to a parquet file under
     /workspace/data/tpcds_sf{scale}/<table>.parquet.
  3. Spark reads each parquet file and rewrites it as a plain Delta
     directory at /workspace/data/tpcds_sf{scale}_delta/<table>/.

The two-stage pipeline (DuckDB -> parquet -> Spark -> Delta) matches the
TPC-H path so the published numbers across the two benchmarks are
directly comparable. DuckDB's tpcds extension ships with the upstream
TPC-DS templates instantiated at seed=0; we also extract those 99
queries (workloads/tpcds/queries/q01.sql through q99.sql) so the
workload module sees them at import time.

Usage (inside the bench container):
    python data_gen/generate_tpcds_delta.py --scale 1
    python data_gen/generate_tpcds_delta.py --scale 10 --overwrite
    python data_gen/generate_tpcds_delta.py --scale 100 \\
        --memory-limit 8GB --temp-dir /workspace/data/duckdb_tmp

At SF100, `CALL dsdgen(sf = 100)` materialises ~100 GB of tables before
parquet export. DuckDB will spill any excess over --memory-limit to
--temp-dir, so the host RAM does not need to hold the full dataset; the
temp dir does need ~80-120 GB free during the dsdgen + COPY phase.
"""
from __future__ import annotations

import argparse
import shutil
import sys
import time
from pathlib import Path

# Repo root resolved from this file so the benchmark runs on any host (not just
# the container's /workspace WORKDIR). data_gen/<file>.py -> parent.parent.
_REPO_ROOT = Path(__file__).resolve().parent.parent


TPCDS_TABLES = [
    "call_center", "catalog_page", "catalog_returns", "catalog_sales",
    "customer", "customer_address", "customer_demographics", "date_dim",
    "household_demographics", "income_band", "inventory", "item",
    "promotion", "reason", "ship_mode", "store", "store_returns",
    "store_sales", "time_dim", "warehouse", "web_page", "web_returns",
    "web_sales", "web_site",
]


def stage_parquet(
    scale: int,
    data_root: Path,
    overwrite: bool,
    memory_limit: str,
    temp_dir: Path,
) -> Path:
    """Run dsdgen via DuckDB and dump each table to a parquet file.

    `memory_limit` is passed verbatim to DuckDB's `SET memory_limit`
    (accepts the standard 'NGB', 'NMB', 'NKB' suffixes). `temp_dir` is
    where DuckDB will spill anything that exceeds `memory_limit`; at
    SF100 the spill volume reaches tens of GB so the dir must live on
    a partition with the headroom documented in the module docstring.

    Returns the directory holding the staged parquet files."""
    import duckdb

    parquet_dir = data_root / f"tpcds_sf{scale}"
    parquet_dir.mkdir(parents=True, exist_ok=True)

    needs_gen = overwrite or not all(
        (parquet_dir / f"{t}.parquet").exists() for t in TPCDS_TABLES
    )
    if not needs_gen:
        print(f"[skip] parquet stage already complete at {parquet_dir}")
        return parquet_dir

    temp_dir.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(":memory:")
    con.execute(f"SET memory_limit = '{memory_limit}'")
    con.execute(f"SET temp_directory = '{temp_dir}'")
    # preserve_insertion_order=false lets DuckDB spill aggregation /
    # sort state to temp_directory under memory pressure. At SF100 this
    # is the difference between completing and OOM-ing.
    con.execute("SET preserve_insertion_order = false")
    print(f"[duckdb] memory_limit={memory_limit} temp_directory={temp_dir}")
    con.execute("INSTALL tpcds")
    con.execute("LOAD tpcds")
    t0 = time.perf_counter()
    print(f"[dsdgen] populating in-memory TPC-DS schema at SF={scale}...")
    con.execute(f"CALL dsdgen(sf = {scale})")
    print(f"[dsdgen] done in {time.perf_counter() - t0:.1f}s")

    for t in TPCDS_TABLES:
        dst = parquet_dir / f"{t}.parquet"
        if dst.exists() and not overwrite:
            continue
        t0 = time.perf_counter()
        con.execute(
            f"COPY (SELECT * FROM {t}) TO '{dst}' "
            f"(FORMAT 'parquet', COMPRESSION 'snappy')"
        )
        n = con.execute(f"SELECT count(*) FROM {t}").fetchone()[0]
        print(f"[parquet] {dst.name:<25} rows={n:>13,} elapsed={time.perf_counter() - t0:.2f}s")

    con.close()
    return parquet_dir


def stage_delta(parquet_dir: Path, delta_dir: Path, overwrite: bool) -> None:
    """Rewrite each parquet file as a plain Delta directory."""
    sys.path.insert(0, str(_REPO_ROOT))
    from engines._spark_session import get_spark  # type: ignore

    spark = get_spark()
    spark.conf.set("spark.databricks.delta.properties.defaults.enableDeletionVectors", "false")
    spark.conf.set("spark.databricks.delta.properties.defaults.columnMapping.mode", "none")
    spark.conf.set("spark.databricks.delta.properties.defaults.enableRowTracking", "false")

    delta_dir.mkdir(parents=True, exist_ok=True)
    plain_props = (
        "TBLPROPERTIES ("
        "'delta.enableDeletionVectors' = 'false',"
        "'delta.columnMapping.mode'    = 'none',"
        "'delta.enableRowTracking'     = 'false'"
        ")"
    )

    for t in TPCDS_TABLES:
        src = parquet_dir / f"{t}.parquet"
        dst = delta_dir / t
        if dst.exists():
            if overwrite:
                shutil.rmtree(dst)
            else:
                print(f"[skip] {dst} already exists; pass --overwrite to recreate")
                continue

        view = f"src_{t}"
        spark.read.parquet(str(src)).createOrReplaceTempView(view)
        t0 = time.perf_counter()
        spark.sql(
            f"CREATE OR REPLACE TABLE delta.`{dst}` USING DELTA {plain_props} "
            f"AS SELECT * FROM {view}"
        )
        n = spark.read.format("delta").load(str(dst)).count()
        print(f"[delta]   {t:<25} rows={n:>13,} elapsed={time.perf_counter() - t0:.2f}s")

    spark.stop()


def extract_queries(out_dir: Path) -> None:
    """Dump DuckDB's instantiated TPC-DS query set into workloads/tpcds/queries/.

    DuckDB's tpcds extension ships the official TPC-DS query templates
    instantiated at seed=0. Writing them to disk so the workload module
    (workloads/tpcds_read_delta.py) sees them at import time, just like
    the TPC-H workload sees its committed q01.sql .. q22.sql."""
    import duckdb

    if out_dir.exists() and any(out_dir.glob("q*.sql")):
        n = len(list(out_dir.glob("q*.sql")))
        print(f"[skip] {out_dir} already has {n} query files")
        return

    out_dir.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(":memory:")
    con.execute("INSTALL tpcds")
    con.execute("LOAD tpcds")
    rows = con.execute(
        "SELECT query_nr, query FROM tpcds_queries() ORDER BY query_nr"
    ).fetchall()
    for nr, q in rows:
        (out_dir / f"q{nr:02d}.sql").write_text(q.rstrip() + "\n", encoding="utf-8")
    con.close()
    print(f"[queries] wrote {len(rows)} TPC-DS query files to {out_dir}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scale", type=int, default=1,
                        help="TPC-DS scale factor.")
    parser.add_argument("--data-dir", default=str(_REPO_ROOT / "data"),
                        help="Bench data root.")
    parser.add_argument("--overwrite", action="store_true",
                        help="Delete and recreate parquet/Delta outputs if they exist.")
    parser.add_argument("--memory-limit", default="8GB",
                        help="DuckDB SET memory_limit during dsdgen + COPY. "
                             "DuckDB spills anything over this to --temp-dir. "
                             "Bigger = fewer spills (faster) at the cost of "
                             "host RAM. Default 8GB is safe on a 32 GB box "
                             "while leaving room for the Spark JVM in the "
                             "Delta-rewrite stage that follows.")
    parser.add_argument("--temp-dir", default=str(_REPO_ROOT / "data" / "duckdb_tmp"),
                        help="DuckDB SET temp_directory. Holds spilled "
                             "intermediate state during dsdgen. At SF100 "
                             "this can reach tens of GB; put it on a "
                             "partition with at least 120 GB free.")
    args = parser.parse_args()

    data_root = Path(args.data_dir)
    delta_dir = data_root / f"tpcds_sf{args.scale}_delta"
    temp_dir = Path(args.temp_dir)
    parquet_dir = stage_parquet(
        args.scale, data_root, args.overwrite,
        memory_limit=args.memory_limit, temp_dir=temp_dir,
    )
    stage_delta(parquet_dir, delta_dir, args.overwrite)

    queries_dir = _REPO_ROOT / "workloads" / "tpcds" / "queries"
    extract_queries(queries_dir)

    print(f"\n[done] TPC-DS plain Delta tables under {delta_dir}")
    print(f"       Queries committed under {queries_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
