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

QUERIES_DIR = Path(__file__).parent / "tpcds" / "queries"

_TPCDS_TABLES = [
    "call_center", "catalog_page", "catalog_returns", "catalog_sales",
    "customer", "customer_address", "customer_demographics", "date_dim",
    "household_demographics", "income_band", "inventory", "item",
    "promotion", "reason", "ship_mode", "store", "store_returns",
    "store_sales", "time_dim", "warehouse", "web_page", "web_returns",
    "web_sales", "web_site",
]

_DELTA_ROOT = "/workspace/data/tpcds_sf1_delta"


def _df_setup() -> str:
    return "SELECT 1"


def _df_cleanup() -> str:
    return "SELECT 1"


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


def _setup_steps() -> list[WorkloadStep]:
    return [
        WorkloadStep(
            id="register_delta_tables",
            kind=STEP_SQL_DDL,
            sql=_spark_setup(),
            per_engine_sql={
                "df": _df_setup(),
                "duckdb": _duckdb_setup(),
                "spark-default": _spark_setup(),
                "spark-tuned": _spark_setup(),
            },
            description="Per-engine Delta mount (df: no-op; DuckDB: load delta+views; Spark: views)",
            measured=False,
        )
    ]


def _cleanup_steps() -> list[WorkloadStep]:
    return [
        WorkloadStep(
            id="unregister_delta_tables",
            kind=STEP_SQL_DDL,
            sql=_spark_cleanup(),
            per_engine_sql={
                "df": _df_cleanup(),
                "duckdb": _duckdb_cleanup(),
                "spark-default": _spark_cleanup(),
                "spark-tuned": _spark_cleanup(),
            },
            description="Drop per-engine Delta mounts",
            measured=False,
        )
    ]


def _load_query_steps() -> list[WorkloadStep]:
    preamble = _df_open_preamble()
    steps: list[WorkloadStep] = []
    for sql_path in sorted(QUERIES_DIR.glob("q*.sql")):
        sql = sql_path.read_text(encoding="utf-8").strip().rstrip(";").rstrip()
        df_sql = preamble + ";\n" + sql
        steps.append(
            WorkloadStep(
                id=sql_path.stem,
                kind=STEP_SQL_QUERY,
                sql=sql,
                per_engine_sql={"df": df_sql},
                description=f"TPC-DS {sql_path.stem.upper()} (Delta read)",
                expects_rows=True,
            )
        )
    return steps


WORKLOAD = Workload(
    name="tpcds_read_delta",
    description="99 canonical TPC-DS read queries against plain Delta tables (no DV).",
    setup_steps=_setup_steps(),
    measured_steps=_load_query_steps(),
    cleanup_steps=_cleanup_steps(),
    requires_data_at_scale=None,
    # Pre-flight checks for parquet files in the staging dir; the Delta
    # tables live alongside under tpcds_sf{scale}_delta/. Matches the
    # tpch_read_delta pattern.
    data_subdir="tpcds_sf{scale}",
)
