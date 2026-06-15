"""TPC-DS read against PLAIN Delta tables (no DV, no column-mapping).

99 canonical TPC-DS queries against the 24-table snowflake schema. Same
fixture / engine / measurement pattern as tpch_read_delta.py:

Fixture (one-time, outside the bench):
    data_gen/generate_tpcds_delta.py writes the 24 TPC-DS tables as
    plain Delta into /workspace/data/tpcds_sf{scale}_delta/<table>/.
    Plain protocol so DuckDB's read-only delta extension can read them.

Per-engine read paths (no engine pays a catalog-registration tax inside
the measured query):

  df:
    Each measured query carries `OPEN DELTA TABLE '<path>' AS <name>`
    preambles for every TPC-DS table, then the SELECT. The df engine
    adapter (engines/df_engine.py) splits the multi-statement script,
    runs the OPENs in the same session as a SHOW STATS ACTUAL wrap
    around the final SELECT. SHOW STATS' `total_time_ms` covers only
    the SELECT; the OPEN preamble lands in wall_ms.

  DuckDB:
    INSTALL delta; LOAD delta (one-time, in setup). Then per-table
    `CREATE OR REPLACE VIEW <t> AS SELECT * FROM delta_scan('<path>')`
    (also setup). Measured queries reference unqualified names.

  Spark:
    `CREATE OR REPLACE TEMPORARY VIEW <t> USING delta OPTIONS
    (path '<path>')` (setup). Measured queries reference unqualified
    names.

Query source: DuckDB's `tpcds` extension ships the TPC-DS query
templates instantiated at seed=0; data_gen/generate_tpcds_delta.py
extracts those 99 queries into workloads/tpcds/queries/q01.sql through
q99.sql so this module sees them at import time.
"""
from __future__ import annotations

from pathlib import Path

from engines.base import STEP_SQL_DDL, STEP_SQL_QUERY, WorkloadStep
from .spec import Workload
from ._df_catalog import df_register_setup, df_qualify

QUERIES_DIR = Path(__file__).parent / "tpcds" / "queries"

_TPCDS_TABLES = [
    "call_center", "catalog_page", "catalog_returns", "catalog_sales",
    "customer", "customer_address", "customer_demographics", "date_dim",
    "household_demographics", "income_band", "inventory", "item",
    "promotion", "reason", "ship_mode", "store", "store_returns",
    "store_sales", "time_dim", "warehouse", "web_page", "web_returns",
    "web_sales", "web_site",
]

_DELTA_ROOT = "{data_dir}_delta"

_ZONE = "tpcds"
_SCHEMA = "rd"


def _df_setup() -> str:
    return df_register_setup(_ZONE, _SCHEMA, _DELTA_ROOT, _TPCDS_TABLES)


def _duckdb_setup() -> str:
    parts = ["INSTALL delta", "LOAD delta"]
    for t in _TPCDS_TABLES:
        parts.append(f"DROP VIEW IF EXISTS {t}")
        parts.append(
            f"CREATE OR REPLACE VIEW {t} AS "
            f"SELECT * FROM delta_scan('{_DELTA_ROOT}/{t}')"
        )
    return ";\n".join(parts)


def _duckdb_cleanup() -> str:
    return ";\n".join(f"DROP VIEW IF EXISTS {t}" for t in _TPCDS_TABLES)


def _spark_setup() -> str:
    parts = []
    for t in _TPCDS_TABLES:
        parts.append(
            f"CREATE OR REPLACE TEMPORARY VIEW {t} "
            f"USING delta OPTIONS (path '{_DELTA_ROOT}/{t}')"
        )
    return ";\n".join(parts)


def _spark_cleanup() -> str:
    return ";\n".join(f"DROP VIEW IF EXISTS {t}" for t in _TPCDS_TABLES)


def _df_open_preamble() -> str:
    """24 OPEN DELTA TABLE statements. Cost is bounded and ~30 ms per table
    on warm metadata cache; df_engine.py treats every statement before the
    final SELECT as untimed preamble."""
    return ";\n".join(
        f"OPEN DELTA TABLE '{_DELTA_ROOT}/{t}' AS {t}"
        for t in _TPCDS_TABLES
    )


def _catalog_setup_steps() -> list[WorkloadStep]:
    # df only: REGISTER DELTA TABLE is catalog-persistent, so it runs ONCE per
    # session (bench_runner's catalog phase), tied to dataset creation, never
    # per query and never torn down. sql=None means the path-reading engines
    # (DuckDB/Spark) skip it; their session-local views live in _setup_steps.
    return [
        WorkloadStep(
            id="register_delta_catalog",
            kind=STEP_SQL_DDL,
            sql=None,
            per_engine_sql={"df": _df_setup()},
            description="Register TPC-DS Delta tables in the DeltaForge catalog (once)",
            measured=False,
        )
    ]


def _setup_steps() -> list[WorkloadStep]:
    return [
        WorkloadStep(
            id="register_delta_tables",
            kind=STEP_SQL_DDL,
            sql=None,
            per_engine_sql={
                "duckdb": _duckdb_setup(),
                "spark-default": _spark_setup(),
                "spark-tuned": _spark_setup(),
            },
            description="Per-engine Delta mount (DuckDB: load delta+views; Spark: views)",
            measured=False,
        )
    ]


def _cleanup_steps() -> list[WorkloadStep]:
    return [
        WorkloadStep(
            id="unregister_delta_tables",
            kind=STEP_SQL_DDL,
            sql=None,
            per_engine_sql={
                "duckdb": _duckdb_cleanup(),
                "spark-default": _spark_cleanup(),
                "spark-tuned": _spark_cleanup(),
            },
            description="Drop per-engine Delta mounts",
            measured=False,
        )
    ]


def _query_paths_in_stream_order() -> list[Path]:
    """Return the 99 TPC-DS query files in the order this stream should
    execute them.

    Default (env `DF_BENCH_STREAM_SEED` unset): lexicographic order, i.e.
    q01, q02, ..., q99. This matches the Power-run protocol and preserves
    every existing single-stream result exactly.

    With `DF_BENCH_STREAM_SEED` set to an integer N: the same 99 paths
    are shuffled with `random.Random(N).shuffle`. The scale-out
    orchestrator assigns each concurrent stream a distinct seed so the
    streams hit the dataset in different orders, matching the TPC-DS
    Throughput Test methodology where the per-stream ordering tables
    decorrelate cache + buffer-pool effects across streams. Seed=0
    yields a deterministic permutation; do not use it as a "no shuffle"
    sentinel, use the env-unset case for that.
    """
    import os
    import random

    paths = sorted(QUERIES_DIR.glob("q*.sql"))
    seed_str = os.environ.get("DF_BENCH_STREAM_SEED")
    if seed_str is None:
        return paths
    try:
        seed = int(seed_str)
    except ValueError as exc:
        raise ValueError(
            f"DF_BENCH_STREAM_SEED must be an integer, got {seed_str!r}"
        ) from exc
    random.Random(seed).shuffle(paths)
    return paths


def _load_query_steps() -> list[WorkloadStep]:
    steps: list[WorkloadStep] = []
    for sql_path in _query_paths_in_stream_order():
        sql = sql_path.read_text(encoding="utf-8").strip().rstrip(";").rstrip()
        steps.append(
            WorkloadStep(
                id=sql_path.stem,
                kind=STEP_SQL_QUERY,
                sql=sql,
                per_engine_sql={"df": df_qualify(sql, _TPCDS_TABLES, _ZONE, _SCHEMA)},
                description=f"TPC-DS {sql_path.stem.upper()} (Delta read)",
                expects_rows=True,
            )
        )
    return steps


WORKLOAD = Workload(
    name="tpcds_read_delta",
    description="99 canonical TPC-DS read queries against plain Delta tables (no DV).",
    catalog_setup_steps=_catalog_setup_steps(),
    setup_steps=_setup_steps(),
    measured_steps=_load_query_steps(),
    cleanup_steps=_cleanup_steps(),
    requires_data_at_scale=None,
    # Pre-flight checks for parquet files in the staging dir; the Delta
    # tables live alongside under tpcds_sf{scale}_delta/. Matches the
    # tpch_read_delta pattern.
    data_subdir="tpcds_sf{scale}",
)
