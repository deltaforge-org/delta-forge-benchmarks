"""Star Schema Benchmark (SSB) read against PLAIN Delta tables.

13 canonical SSB queries (O'Neil et al., 2009) across the 5-table
star: lineorder fact + date, part, supplier, customer dimensions.

Fixture (one-time, outside the bench):
    data_gen/generate_ssb_delta.py derives the 5 SSB tables from the
    plain-Delta TPC-H tables produced by generate_tpch_delta.py.
    Output: /workspace/data/ssb_sf{scale}_delta/<table>/. Plain Delta
    protocol so DuckDB's read-only delta extension can read them.

Per-engine read paths (same shape as tpch_read_delta.py): df uses an
OPEN DELTA TABLE preamble per query; DuckDB and Spark register views
in untimed setup. SHOW STATS ACTUAL wraps the final SELECT only on df,
so the published number is plan + compile + execute + drain for the
SELECT alone, with the OPEN attach excluded.
"""
from __future__ import annotations

from pathlib import Path

from engines.base import STEP_SQL_DDL, STEP_SQL_QUERY, WorkloadStep
from .spec import Workload

QUERIES_DIR = Path(__file__).parent / "ssb" / "queries"

_SSB_TABLES = ["date", "part", "supplier", "customer", "lineorder"]

_DELTA_ROOT = "/workspace/data/ssb_sf1_delta"


def _df_setup() -> str:
    return "SELECT 1"


def _df_cleanup() -> str:
    return "SELECT 1"


def _duckdb_setup() -> str:
    parts = ["INSTALL delta", "LOAD delta"]
    for t in _SSB_TABLES:
        parts.append(f"DROP VIEW IF EXISTS {t}")
        parts.append(
            f"CREATE OR REPLACE VIEW {t} AS "
            f"SELECT * FROM delta_scan('{_DELTA_ROOT}/{t}')"
        )
    return ";\n".join(parts)


def _duckdb_cleanup() -> str:
    return ";\n".join(f"DROP VIEW IF EXISTS {t}" for t in _SSB_TABLES)


def _spark_setup() -> str:
    parts = []
    for t in _SSB_TABLES:
        parts.append(
            f"CREATE OR REPLACE TEMPORARY VIEW {t} "
            f"USING delta OPTIONS (path '{_DELTA_ROOT}/{t}')"
        )
    return ";\n".join(parts)


def _spark_cleanup() -> str:
    return ";\n".join(f"DROP VIEW IF EXISTS {t}" for t in _SSB_TABLES)


def _df_open_preamble() -> str:
    return ";\n".join(
        f"OPEN DELTA TABLE '{_DELTA_ROOT}/{t}' AS {t}"
        for t in _SSB_TABLES
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
            description="Per-engine SSB Delta mount",
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
            description="Drop per-engine SSB Delta mounts",
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
                description=f"SSB {sql_path.stem.upper()} (Delta read)",
                expects_rows=True,
            )
        )
    return steps


WORKLOAD = Workload(
    name="ssb_read_delta",
    description="13 canonical SSB queries against a 5-table plain Delta star schema.",
    setup_steps=_setup_steps(),
    measured_steps=_load_query_steps(),
    cleanup_steps=_cleanup_steps(),
    requires_data_at_scale=None,
    data_subdir="ssb_sf{scale}",
)
