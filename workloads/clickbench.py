"""ClickBench: 43 canonical analytical queries on a 100M-row web-log table.

Source: https://github.com/ClickHouse/ClickBench

Read-only by construction (same pattern as tpch_read): setup mounts the
ClickBench `hits.parquet` as a view / external table; the 43 queries scan
that same on-disk parquet from every engine through its own reader.

Run prerequisites:
    python data_gen/get_clickbench.py    # downloads hits.parquet + queries.sql

Per-query results: warm-median wall (or engine-reported for df) in the
results JSON, comparable to ClickBench's public leaderboard:
    https://benchmark.clickhouse.com/
"""
from __future__ import annotations

import re
from pathlib import Path

from engines.base import STEP_SQL_DDL, STEP_SQL_QUERY, WorkloadStep

from .spec import Workload

QUERIES_DIR = Path(__file__).parent / "clickbench" / "queries"

# ClickBench has one wide table, `hits`, with ~105 columns.
_CB_TABLE = "hits"

# df mounts the parquet in an EXTERNAL zone whose storage_root is the
# clickbench data dir. The zone is shared with the existing bench_ext but
# scoped to its own schema so other workloads' externals don't collide.
_DF_CB_ZONE = "bench_ext"
_DF_CB_SCHEMA = "bench_ext.clickbench"
_DF_CB_ZONE_ROOT = "/workspace/data"

# Spark / DuckDB read the file directly.
_CB_PARQUET_HOST_PATH = "/workspace/data/clickbench/hits.parquet"

# Word-boundary regex for the single hits table.
_TABLE_RE = re.compile(r"\bhits\b", re.IGNORECASE)


def _qualify_for_df(sql: str) -> str:
    """Rewrite `hits` -> `bench_ext.clickbench.hits` so df can resolve it."""
    return _TABLE_RE.sub(f"{_DF_CB_SCHEMA}.{_CB_TABLE}", sql)


def _df_setup_sql() -> str:
    return (
        f"CREATE ZONE IF NOT EXISTS {_DF_CB_ZONE} TYPE EXTERNAL "
        f"STORAGE_ROOT = '{_DF_CB_ZONE_ROOT}' "
        f"COMMENT 'Bench external zone';\n"
        f"CREATE SCHEMA IF NOT EXISTS {_DF_CB_SCHEMA};\n"
        f"DROP EXTERNAL TABLE IF EXISTS {_DF_CB_SCHEMA}.{_CB_TABLE};\n"
        f"CREATE EXTERNAL TABLE {_DF_CB_SCHEMA}.{_CB_TABLE} "
        f"USING PARQUET LOCATION 'clickbench/hits.parquet'"
    )


def _df_drop_sql() -> str:
    return f"DROP EXTERNAL TABLE IF EXISTS {_DF_CB_SCHEMA}.{_CB_TABLE}"


def _duckdb_setup_sql() -> str:
    return (
        f"CREATE OR REPLACE VIEW {_CB_TABLE} AS "
        f"SELECT * FROM read_parquet('{_CB_PARQUET_HOST_PATH}')"
    )


def _duckdb_drop_sql() -> str:
    return f"DROP VIEW IF EXISTS {_CB_TABLE}"


def _spark_setup_sql() -> str:
    return (
        f"CREATE OR REPLACE TEMPORARY VIEW {_CB_TABLE} "
        f"USING parquet OPTIONS (path '{_CB_PARQUET_HOST_PATH}')"
    )


def _spark_drop_sql() -> str:
    return f"DROP VIEW IF EXISTS {_CB_TABLE}"


def _setup_steps() -> list[WorkloadStep]:
    return [
        WorkloadStep(
            id="mount_hits",
            kind=STEP_SQL_DDL,
            sql=_spark_setup_sql(),
            per_engine_sql={
                "df": _df_setup_sql(),
                "duckdb": _duckdb_setup_sql(),
                "spark-default": _spark_setup_sql(),
                "spark-tuned": _spark_setup_sql(),
            },
            description="Mount ClickBench hits.parquet as a view / external table",
            measured=False,
        )
    ]


def _drop_steps() -> list[WorkloadStep]:
    return [
        WorkloadStep(
            id="unmount_hits",
            kind=STEP_SQL_DDL,
            sql=_spark_drop_sql(),
            per_engine_sql={
                "df": _df_drop_sql(),
                "duckdb": _duckdb_drop_sql(),
                "spark-default": _spark_drop_sql(),
                "spark-tuned": _spark_drop_sql(),
            },
            description="Drop ClickBench hits view",
            measured=False,
        )
    ]


def _load_query_steps() -> list[WorkloadStep]:
    steps: list[WorkloadStep] = []
    if not QUERIES_DIR.exists():
        # Queries not downloaded yet; emit no measured steps. The workload
        # is still discoverable but a run will print a clear note.
        return steps
    for sql_path in sorted(QUERIES_DIR.glob("q*.sql")):
        sql = sql_path.read_text(encoding="utf-8").strip().rstrip(";").rstrip()
        if not sql:
            continue
        steps.append(
            WorkloadStep(
                id=sql_path.stem,
                kind=STEP_SQL_QUERY,
                sql=sql,
                per_engine_sql={"df": _qualify_for_df(sql)},
                description=f"ClickBench {sql_path.stem.upper()}",
                expects_rows=True,
            )
        )
    return steps


WORKLOAD = Workload(
    name="clickbench",
    description="ClickBench 43 analytical queries on hits.parquet (~100M rows).",
    setup_steps=_setup_steps(),
    measured_steps=_load_query_steps(),
    cleanup_steps=_drop_steps(),
    requires_data_at_scale=None,
    data_subdir="clickbench",
)
