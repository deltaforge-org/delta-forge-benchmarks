"""TPC-H read against PLAIN Delta tables (no DV, no column-mapping).

Fixture (one-time, outside the bench):
    data_gen/generate_tpch_delta.py writes the 8 TPC-H tables as plain
    Delta into /workspace/data/tpch_sf1_delta/<table>/. Plain protocol
    so DuckDB's read-only delta extension can read them too.

Per-engine read paths (no engine pays a catalog-registration tax inside
the measured query):

  df:
    Each measured query carries an `OPEN DELTA TABLE '<path>' AS <name>`
    preamble for every table the query touches, then the SELECT. The
    df engine adapter (engines/df_engine.py) splits the multi-statement
    script, runs the OPENs in the same session as a SHOW STATS ACTUAL
    wrap around the final SELECT. SHOW STATS' `total_time_ms` covers
    only the SELECT; the OPEN preamble lands in wall_ms.

    Why per-query OPENs and not a one-shot setup? `OPEN DELTA TABLE` is
    session-scoped, and each CLI invocation opens a fresh session. Per-
    query OPENs guarantee the table is visible to the planner of THIS
    query. The cost is bounded -- OPEN is ~30 ms per table on warm
    metadata cache -- and lives in wall_ms, not in the published timing.

  DuckDB:
    INSTALL delta; LOAD delta (one-time, in setup). Then per-table
    `CREATE VIEW <t> AS SELECT * FROM delta_scan('<path>')` (also
    setup). Measured queries reference unqualified names.

  Spark:
    `CREATE OR REPLACE TEMPORARY VIEW <t> USING delta OPTIONS
    (path '<path>')` (setup). Measured queries reference unqualified
    names.

Note: this workload replaces tpch_read.py (parquet via external table).
The Delta path is what DeltaForge is differentiated on; the parquet
path measured three engines' parquet readers, not three engines' Delta
readers.
"""
from __future__ import annotations

from pathlib import Path

from engines.base import STEP_SQL_DDL, STEP_SQL_QUERY, WorkloadStep
from .spec import Workload

QUERIES_DIR = Path(__file__).parent / "tpch" / "queries"

_TPCH_TABLES = [
    "lineitem", "orders", "customer", "supplier",
    "part", "partsupp", "nation", "region",
]

# Where the Delta fixture lives. data_gen/generate_tpch_delta.py writes here.
_DELTA_ROOT = "/workspace/data/tpch_sf1_delta"


# ----- per-engine setup -----------------------------------------------------

def _df_setup() -> str:
    """df setup is a true no-op: OPEN DELTA TABLE is session-scoped, so the
    measured queries carry their own OPENs. Returning a harmless SELECT 1
    keeps the bench framework happy (a workload always runs at least one
    setup statement)."""
    return "SELECT 1"


def _df_cleanup() -> str:
    return "SELECT 1"


def _duckdb_setup() -> str:
    parts = ["INSTALL delta", "LOAD delta"]
    for t in _TPCH_TABLES:
        parts.append(f"DROP VIEW IF EXISTS {t}")
        parts.append(
            f"CREATE OR REPLACE VIEW {t} AS "
            f"SELECT * FROM delta_scan('{_DELTA_ROOT}/{t}')"
        )
    return ";\n".join(parts)


def _duckdb_cleanup() -> str:
    return ";\n".join(f"DROP VIEW IF EXISTS {t}" for t in _TPCH_TABLES)


def _spark_setup() -> str:
    parts = []
    for t in _TPCH_TABLES:
        parts.append(
            f"CREATE OR REPLACE TEMPORARY VIEW {t} "
            f"USING delta OPTIONS (path '{_DELTA_ROOT}/{t}')"
        )
    return ";\n".join(parts)


def _spark_cleanup() -> str:
    return ";\n".join(f"DROP VIEW IF EXISTS {t}" for t in _TPCH_TABLES)


# ----- df per-query preamble ------------------------------------------------

def _df_open_preamble() -> str:
    """One `OPEN DELTA TABLE` per TPC-H table. Issuing all 8 unconditionally
    is simpler than figuring out which the query touches, and the cost is
    bounded -- ~30 ms per table on warm metadata cache. df_engine.py
    treats every statement before the final SELECT as untimed preamble."""
    return ";\n".join(
        f"OPEN DELTA TABLE '{_DELTA_ROOT}/{t}' AS {t}"
        for t in _TPCH_TABLES
    )


# ----- workload definition --------------------------------------------------

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
        # df script: OPEN preambles then the SELECT, all in one CLI call.
        # df_engine.py wraps only the last statement with SHOW STATS ACTUAL.
        df_sql = preamble + ";\n" + sql
        steps.append(
            WorkloadStep(
                id=sql_path.stem,
                kind=STEP_SQL_QUERY,
                sql=sql,
                per_engine_sql={"df": df_sql},
                description=f"TPC-H {sql_path.stem.upper()} (Delta read)",
                expects_rows=True,
            )
        )
    return steps


WORKLOAD = Workload(
    name="tpch_read_delta",
    description="22 canonical TPC-H read queries against plain Delta tables (no DV).",
    setup_steps=_setup_steps(),
    measured_steps=_load_query_steps(),
    cleanup_steps=_cleanup_steps(),
    requires_data_at_scale=None,
)
