"""Verify whether Spark's 125ms write of 10M rows is real or a measurement bug.

Runs CTAS on df and Spark with the same schema, times them externally
(wall-clock around the whole SQL invocation INCLUDING result drain),
then counts on-disk row counts via DuckDB's delta extension as an
independent reader. If Spark really wrote 10M rows in ~125ms it's real;
if the on-disk count is 0 or the timing is hiding deferred work the
bug is exposed."""
import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

ROWS = 10_000_000

DF_OUT = "/workspace/data/synth_write/df_probe/synth_fact"
SD_OUT = "/workspace/data/synth_write/sd_probe/synth_fact"


def df_ctas() -> tuple[float, int, int]:
    """Run df CTAS, return (wall_ms, on_disk_row_count, on_disk_bytes)."""
    # Ensure clean output dir
    shutil.rmtree("/workspace/data/synth_write/df_probe", ignore_errors=True)
    # Drop any prior catalog entry
    subprocess.run([
        "delta-forge-cli", "--format", "json",
        "--control-url", "http://127.0.0.1:3000",
        "query", "--node", "bench-local",
        "DROP DELTA TABLE IF EXISTS write_zone.bench.synth_probe WITH FILES",
    ], capture_output=True)
    # Ensure zone exists
    subprocess.run([
        "delta-forge-cli", "--format", "json",
        "--control-url", "http://127.0.0.1:3000",
        "query", "--node", "bench-local",
        "CREATE ZONE IF NOT EXISTS write_zone TYPE DELTA "
        "STORAGE_ROOT '/workspace/data/synth_write'",
    ], capture_output=True)

    sql = (
        f"CREATE DELTA TABLE write_zone.bench.synth_probe LOCATION '{DF_OUT}' AS "
        "SELECT "
        " i AS id, "
        " CAST(((i * 13) % 10000) AS INT) AS customer_id, "
        " date_add(DATE '2024-01-01', CAST(i % 365 AS INT)) AS order_date, "
        " CAST((i % 100) + 1 AS INT) AS quantity, "
        " CAST(((i % 9999) / 100.0) AS DECIMAL(10, 2)) AS unit_price, "
        " CAST(((i % 30) / 100.0) AS DOUBLE) AS discount, "
        " CASE WHEN (i % 5) = 0 THEN 'NORTH' "
        " WHEN (i % 5) = 1 THEN 'SOUTH' "
        " WHEN (i % 5) = 2 THEN 'EAST' "
        " WHEN (i % 5) = 3 THEN 'WEST' "
        " ELSE 'CENTRAL' END AS region, "
        " (i % 7) = 0 AS is_priority, "
        " concat('order_', lpad(CAST((i % 10000) AS VARCHAR), 6, '0')) AS notes "
        f"FROM generate_series(0, {ROWS - 1}) AS t(i)"
    )
    t0 = time.perf_counter()
    p = subprocess.run([
        "delta-forge-cli", "--format", "json",
        "--control-url", "http://127.0.0.1:3000",
        "query", "--node", "bench-local",
        sql,
    ], capture_output=True, text=True)
    wall_ms = (time.perf_counter() - t0) * 1000.0
    print(f"  df CLI stdout tail: {p.stdout.strip().splitlines()[-1] if p.stdout.strip() else '(empty)'}")
    return wall_ms, *count_on_disk(DF_OUT)


def spark_ctas() -> tuple[float, int, int]:
    """Run Spark CTAS, return (wall_ms, on_disk_row_count, on_disk_bytes)."""
    shutil.rmtree("/workspace/data/synth_write/sd_probe", ignore_errors=True)
    sys.path.insert(0, "/workspace")
    from engines._spark_session import get_spark
    s = get_spark()
    # Warm up Spark with a no-op so the JVM / py4j is JITed
    s.sql("SELECT 1").collect()

    # Spark 4.0 in ANSI mode requires STRING (or VARCHAR(n) with length).
    sql = (
        f"CREATE OR REPLACE TABLE delta.`{SD_OUT}` USING DELTA AS "
        "SELECT "
        " i AS id, "
        " CAST(((i * 13) % 10000) AS INT) AS customer_id, "
        " date_add(DATE '2024-01-01', CAST(i % 365 AS INT)) AS order_date, "
        " CAST((i % 100) + 1 AS INT) AS quantity, "
        " CAST(((i % 9999) / 100.0) AS DECIMAL(10, 2)) AS unit_price, "
        " CAST(((i % 30) / 100.0) AS DOUBLE) AS discount, "
        " CASE WHEN (i % 5) = 0 THEN 'NORTH' "
        " WHEN (i % 5) = 1 THEN 'SOUTH' "
        " WHEN (i % 5) = 2 THEN 'EAST' "
        " WHEN (i % 5) = 3 THEN 'WEST' "
        " ELSE 'CENTRAL' END AS region, "
        " (i % 7) = 0 AS is_priority, "
        " concat('order_', lpad(CAST((i % 10000) AS STRING), 6, '0')) AS notes "
        f"FROM range(0, {ROWS}) AS t(i)"
    )
    # Time the SQL execution (including any deferred materialization)
    t0 = time.perf_counter()
    s.sql(sql).collect()
    wall_ms = (time.perf_counter() - t0) * 1000.0

    # Force Spark to actually finalize any pending writes by querying the table
    n_rows_via_spark = s.sql(f"SELECT count(*) FROM delta.`{SD_OUT}`").collect()[0][0]
    print(f"  Spark sees {n_rows_via_spark:,} rows in its own Delta reader")
    s.stop()
    return wall_ms, *count_on_disk(SD_OUT)


def count_on_disk(path: str) -> tuple[int, int]:
    """Count rows in the Delta table via DuckDB's delta extension as an
    independent reader, plus on-disk bytes."""
    p = Path(path)
    if not p.exists():
        return (0, 0)
    bytes_total = sum(f.stat().st_size for f in p.rglob("*") if f.is_file())

    import duckdb
    c = duckdb.connect(":memory:")
    c.execute("INSTALL delta")
    c.execute("LOAD delta")
    try:
        n = c.execute(f"SELECT count(*) FROM delta_scan('{path}')").fetchone()[0]
    except Exception as e:
        print(f"  duckdb read FAILED: {e}")
        n = -1
    return (n, bytes_total)


def main() -> int:
    print(f"=== Writing {ROWS:,} rows on each engine; measuring externally ===\n")

    print("DeltaForge:")
    df_ms, df_rows, df_bytes = df_ctas()
    print(f"  external wall:    {df_ms:>10.2f} ms")
    print(f"  rows on disk:     {df_rows:>10,}  (DuckDB delta_scan)")
    print(f"  bytes on disk:    {df_bytes:>10,} B  ({df_bytes / 1024 / 1024:.1f} MiB)")
    print(f"  inferred rows/s:  {df_rows / (df_ms / 1000.0):>10,.0f}")

    print("\nSpark (default-config session):")
    sk_ms, sk_rows, sk_bytes = spark_ctas()
    print(f"  external wall:    {sk_ms:>10.2f} ms")
    print(f"  rows on disk:     {sk_rows:>10,}  (DuckDB delta_scan)")
    print(f"  bytes on disk:    {sk_bytes:>10,} B  ({sk_bytes / 1024 / 1024:.1f} MiB)")
    print(f"  inferred rows/s:  {sk_rows / (sk_ms / 1000.0):>10,.0f}")

    if sk_rows != ROWS or df_rows != ROWS:
        print(f"\n*** ROW COUNT MISMATCH — expected {ROWS:,} per engine ***")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
