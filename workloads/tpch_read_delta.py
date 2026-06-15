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

import re
from pathlib import Path

from engines.base import STEP_SQL_DDL, STEP_SQL_QUERY, WorkloadStep
from .spec import Workload

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


# df has no SQL USE / search path, so a bare table name resolves to the default
# schema (datafusion.public), not the catalog. The Delta tables are registered
# under bench.tpch (see _df_setup), so df's copy of each query references them
# qualified. TPC-H column names are all prefixed (l_, o_, ...) and the queries
# carry no `table.column` qualifiers, so a whole-word substitution of the 8 table
# names is unambiguous (longest-first guards the partsupp/part overlap).
_DF_QUALIFY_RE = re.compile(
    r"\b(" + "|".join(sorted(_TPCH_TABLES, key=len, reverse=True)) + r")\b"
)


def _df_qualify(sql: str) -> str:
    return _DF_QUALIFY_RE.sub(r"bench.tpch.\1", sql)


# ----- per-engine setup -----------------------------------------------------

def _df_setup() -> str:
    """Register the existing Delta tables PERMANENTLY in the catalog. df runs
    each statement as its own ``delta-forge-cli query`` session, so a
    session-scoped OPEN DELTA TABLE cannot carry across the per-query sessions.
    REGISTER DELTA TABLE writes a persistent catalog row (3-part
    zone.schema.table) for an existing Delta directory without copying or
    rewriting data, so every later query session sees the table by name. It is
    idempotent (a repeat REGISTER on an existing name is a no-op, never an
    error), and LOCATION is RELATIVE to the zone's storage_root."""
    parts = [
        f"CREATE ZONE IF NOT EXISTS bench STORAGE_ROOT = '{_DELTA_ROOT}'",
        "CREATE SCHEMA IF NOT EXISTS bench.tpch",
    ]
    for t in _TPCH_TABLES:
        parts.append(f"REGISTER DELTA TABLE bench.tpch.{t} LOCATION '{t}'")
    return ";\n".join(parts)


def _df_cleanup() -> str:
    """Leave a clean catalog: UNREGISTER removes each catalog row WITHOUT
    deleting the underlying Delta data, then drop the now-empty schema and zone.
    (In the container each run already starts from a fresh bootstrapped catalog;
    this keeps a re-used catalog free of residue too.)"""
    parts = [f"UNREGISTER TABLE IF EXISTS bench.tpch.{t}" for t in _TPCH_TABLES]
    parts.append("DROP SCHEMA IF EXISTS bench.tpch")
    parts.append("DROP ZONE IF EXISTS bench")
    return ";\n".join(parts)


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
                per_engine_sql={"df": _df_qualify(sql)},
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
