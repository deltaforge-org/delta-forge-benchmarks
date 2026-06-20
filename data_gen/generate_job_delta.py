"""Generate the Join Order Benchmark (JOB) fixture as plain Delta tables.

JOB (Leis et al., VLDB 2015) is a 21-table snapshot of the IMDB database
with 113 real queries designed to stress query-optimizer cardinality
estimation. The dataset is fixed-size (no scale factor); a single
fixture serves every run.

Output: /workspace/data/job_delta/<table>/. Same plain Delta protocol as
generate_tpch_delta.py: no DV, no column mapping, no row tracking. The
21 tables come from the JOB authors' canonical IMDB CSV snapshot.

Pipeline:
  1. Download the IMDB CSV bundle (~1.2 GB compressed, ~3.6 GB unpacked)
     from the JOB authors' CWI mirror.
  2. Use DuckDB to apply the JOB schema (workloads/job/schema.sql) and
     COPY each CSV into a typed table.
  3. Export each table to parquet at /workspace/data/job/<table>.parquet.
  4. Have Spark rewrite each parquet as a plain Delta directory at
     /workspace/data/job_delta/<table>/.

Why two-stage (DuckDB -> parquet -> Spark -> Delta): the CSVs are in
postgres-COPY format with a backslash escape character, which DuckDB
handles natively. Going through parquet first keeps the Spark step
identical to the TPC-H and TPC-DS data-gen paths so all three benchmarks
share the same Delta-write code.

Usage (inside the bench container):
    python data_gen/generate_job_delta.py
    python data_gen/generate_job_delta.py --imdb-url <alt-mirror>
    python data_gen/generate_job_delta.py --overwrite
"""
from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
import time
from pathlib import Path

# Repo root resolved from this file so the benchmark runs on any host (not just
# the container's /workspace WORKDIR). data_gen/<file>.py -> parent.parent.
_REPO_ROOT = Path(__file__).resolve().parent.parent


IMDB_TABLES = [
    "aka_name", "aka_title", "cast_info", "char_name", "comp_cast_type",
    "company_name", "company_type", "complete_cast", "info_type",
    "keyword", "kind_type", "link_type", "movie_companies", "movie_info",
    "movie_info_idx", "movie_keyword", "movie_link", "name",
    "person_info", "role_type", "title",
]

DEFAULT_IMDB_URL = "https://event.cwi.nl/da/job/imdb.tgz"


def download_imdb(url: str, dest: Path) -> Path:
    """Download imdb.tgz to dest/imdb.tgz if not already present."""
    dest.mkdir(parents=True, exist_ok=True)
    tarball = dest / "imdb.tgz"
    if tarball.exists() and tarball.stat().st_size > 100 * 1024 * 1024:
        print(f"[skip] {tarball} already present ({tarball.stat().st_size / 1e9:.1f} GB)")
        return tarball
    print(f"[fetch] {url} -> {tarball}")
    subprocess.run(["curl", "-fSL", "-o", str(tarball), url], check=True)
    return tarball


def extract_imdb(tarball: Path, csv_dir: Path) -> None:
    """Untar the IMDB bundle into csv_dir. The CWI tarball lays out CSV
    files at the archive root (no parent directory), so no strip-components."""
    csv_dir.mkdir(parents=True, exist_ok=True)
    sample = csv_dir / "title.csv"
    if sample.exists() and sample.stat().st_size > 1024 * 1024:
        print(f"[skip] {csv_dir} already populated")
        return
    print(f"[extract] {tarball} -> {csv_dir}")
    subprocess.run(
        ["tar", "-xzf", str(tarball), "-C", str(csv_dir)],
        check=True,
    )


def stage_parquet(csv_dir: Path, parquet_dir: Path, schema_sql: Path,
                  overwrite: bool) -> None:
    """Apply the JOB schema in DuckDB, COPY each CSV in, dump to parquet."""
    import duckdb

    parquet_dir.mkdir(parents=True, exist_ok=True)
    needs_gen = overwrite or not all(
        (parquet_dir / f"{t}.parquet").exists() for t in IMDB_TABLES
    )
    if not needs_gen:
        print(f"[skip] parquet stage already complete at {parquet_dir}")
        return

    con = duckdb.connect(":memory:")
    schema_text = schema_sql.read_text(encoding="utf-8")
    # DuckDB accepts most postgres DDL verbatim; the schema uses
    # `integer NOT NULL PRIMARY KEY`, `character varying(N)`, and `text`,
    # all of which DuckDB understands.
    con.execute(schema_text)

    for t in IMDB_TABLES:
        csv = csv_dir / f"{t}.csv"
        dst = parquet_dir / f"{t}.parquet"
        if dst.exists() and not overwrite:
            continue
        if not csv.exists():
            print(f"ERROR: missing CSV: {csv}", file=sys.stderr)
            raise SystemExit(2)
        t0 = time.perf_counter()
        # JOB CSVs use postgres COPY format: comma-separated, no header,
        # backslash escape on embedded delimiters / newlines.
        con.execute(
            f"COPY {t} FROM '{csv}' "
            f"(FORMAT CSV, HEADER FALSE, DELIMITER ',', "
            f"ESCAPE '\\', QUOTE '\"', NULL '')"
        )
        con.execute(
            f"COPY (SELECT * FROM {t}) TO '{dst}' "
            f"(FORMAT 'parquet', COMPRESSION 'snappy')"
        )
        n = con.execute(f"SELECT count(*) FROM {t}").fetchone()[0]
        print(f"[parquet] {t:<20} rows={n:>13,} elapsed={time.perf_counter() - t0:.2f}s")

    con.close()


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

    for t in IMDB_TABLES:
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
        print(f"[delta]   {t:<20} rows={n:>13,} elapsed={time.perf_counter() - t0:.2f}s")

    spark.stop()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", default=str(_REPO_ROOT / "data"),
                        help="Bench data root.")
    parser.add_argument("--imdb-url", default=DEFAULT_IMDB_URL,
                        help="Source URL for imdb.tgz; override for alt mirror.")
    parser.add_argument("--overwrite", action="store_true",
                        help="Delete and recreate parquet/Delta outputs if they exist.")
    args = parser.parse_args()

    data_root = Path(args.data_dir)
    csv_dir = data_root / "job_csv"
    parquet_dir = data_root / "job"
    delta_dir = data_root / "job_delta"

    schema_sql = _REPO_ROOT / "workloads" / "job" / "schema.sql"
    if not schema_sql.exists():
        print(f"ERROR: missing JOB schema: {schema_sql}", file=sys.stderr)
        return 1

    tarball = download_imdb(args.imdb_url, data_root)
    extract_imdb(tarball, csv_dir)
    stage_parquet(csv_dir, parquet_dir, schema_sql, args.overwrite)
    stage_delta(parquet_dir, delta_dir, args.overwrite)

    print(f"\n[done] JOB plain Delta tables under {delta_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
