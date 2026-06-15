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
from ._df_catalog import df_register_setup, df_qualify

QUERIES_DIR = Path(__file__).parent / "tpch" / "queries"

_TPCH_TABLES = [
    "lineitem", "orders", "customer", "supplier",
    "part", "partsupp", "nation", "region",
]

# Where the Delta fixture lives. data_gen/generate_tpch_delta.py writes the
# per-scale dirs (tpch_sf{N}_delta); bench_runner substitutes {data_dir} to
# /workspace/data/tpch_sf{scale} for the active --scale, so this template
# resolves to /workspace/data/tpch_sf{scale}_delta at run time.
_DELTA_ROOT = "{data_dir}_delta"


# df registers the Delta tables in the catalog (REGISTER DELTA TABLE) ONCE per
# session, then references them by their qualified name. Registration is a
# one-time catalog DDL tied to dataset creation, not per-query work, so it lives
# in catalog_setup_steps (no per-run register/unregister). See _df_catalog.py.
_ZONE = "tpch"
_SCHEMA = "rd"


# ----- per-engine setup -----------------------------------------------------

def _df_setup() -> str:
    return df_register_setup(_ZONE, _SCHEMA, _DELTA_ROOT, _TPCH_TABLES)


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
            description="Register TPC-H Delta tables in the DeltaForge catalog (once)",
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


def _load_query_steps() -> list[WorkloadStep]:
    steps: list[WorkloadStep] = []
    for sql_path in sorted(QUERIES_DIR.glob("q*.sql")):
        sql = sql_path.read_text(encoding="utf-8").strip().rstrip(";").rstrip()
        # df reads the tables registered in the catalog by _df_setup (3-part
        # CREATE DELTA TABLE); qualify the bare TPC-H names to bench.tpch.* so
        # they resolve (df has no USE). No per-query OPEN.
        steps.append(
            WorkloadStep(
                id=sql_path.stem,
                kind=STEP_SQL_QUERY,
                sql=sql,
                per_engine_sql={"df": df_qualify(sql, _TPCH_TABLES, _ZONE, _SCHEMA)},
                description=f"TPC-H {sql_path.stem.upper()} (Delta read)",
                expects_rows=True,
            )
        )
    return steps


WORKLOAD = Workload(
    name="tpch_read_delta",
    description="22 canonical TPC-H read queries against plain Delta tables (no DV).",
    catalog_setup_steps=_catalog_setup_steps(),
    setup_steps=_setup_steps(),
    measured_steps=_load_query_steps(),
    cleanup_steps=_cleanup_steps(),
    requires_data_at_scale=None,
)
