"""Join Order Benchmark (JOB) read against PLAIN Delta tables.

113 queries from Leis et al. (VLDB 2015) "How Good Are Query
Optimizers, Really?" against the IMDB snapshot. Purpose-built to stress
cardinality estimation; the query plans on this dataset are where
optimizer quality shows up most cleanly.

JOB is **fixed size** (no scale factor). The IMDB snapshot is ~3.6 GB
unpacked, ~1 GB as plain Delta.

Fixture (one-time, outside the bench):
    data_gen/generate_job_delta.py downloads imdb.tgz, applies the
    21-table JOB schema via DuckDB, exports each table to parquet,
    then writes plain Delta at /workspace/data/job_delta/<table>/.

Per-engine read paths follow the same shape as tpch_read_delta and
ssb_read_delta: df uses an OPEN DELTA TABLE preamble per query;
DuckDB and Spark register views in untimed setup. SHOW STATS ACTUAL
wraps only the final SELECT on df so the published number excludes
the per-query attach.
"""
from __future__ import annotations

from pathlib import Path

from engines.base import STEP_SQL_DDL, STEP_SQL_QUERY, WorkloadStep
from .spec import Workload
from ._df_catalog import df_register_setup, df_unregister_cleanup, df_qualify

QUERIES_DIR = Path(__file__).parent / "job" / "queries"

_JOB_TABLES = [
    "aka_name", "aka_title", "cast_info", "char_name", "comp_cast_type",
    "company_name", "company_type", "complete_cast", "info_type",
    "keyword", "kind_type", "link_type", "movie_companies", "movie_info",
    "movie_info_idx", "movie_keyword", "movie_link", "name",
    "person_info", "role_type", "title",
]

# JOB is fixed-size; data_subdir is "job", and bench_runner substitutes
# {data_dir} to /workspace/data/job, so this template resolves to
# /workspace/data/job_delta at run time.
_DELTA_ROOT = "{data_dir}_delta"

_ZONE = "job"
_SCHEMA = "rd"


def _df_setup() -> str:
    return df_register_setup(_ZONE, _SCHEMA, _DELTA_ROOT, _JOB_TABLES)


def _df_cleanup() -> str:
    return df_unregister_cleanup(_ZONE, _SCHEMA, _JOB_TABLES)


def _duckdb_setup() -> str:
    parts = ["INSTALL delta", "LOAD delta"]
    for t in _JOB_TABLES:
        parts.append(f"DROP VIEW IF EXISTS {t}")
        parts.append(
            f"CREATE OR REPLACE VIEW {t} AS "
            f"SELECT * FROM delta_scan('{_DELTA_ROOT}/{t}')"
        )
    return ";\n".join(parts)


def _duckdb_cleanup() -> str:
    return ";\n".join(f"DROP VIEW IF EXISTS {t}" for t in _JOB_TABLES)


def _spark_setup() -> str:
    parts = []
    for t in _JOB_TABLES:
        parts.append(
            f"CREATE OR REPLACE TEMPORARY VIEW {t} "
            f"USING delta OPTIONS (path '{_DELTA_ROOT}/{t}')"
        )
    return ";\n".join(parts)


def _spark_cleanup() -> str:
    return ";\n".join(f"DROP VIEW IF EXISTS {t}" for t in _JOB_TABLES)


def _df_open_preamble() -> str:
    """21 OPEN DELTA TABLE statements. Cost is bounded; df_engine.py
    treats every statement before the final SELECT as untimed preamble."""
    return ";\n".join(
        f"OPEN DELTA TABLE '{_DELTA_ROOT}/{t}' AS {t}"
        for t in _JOB_TABLES
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
            description="Per-engine JOB Delta mount",
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
            description="Drop per-engine JOB Delta mounts",
            measured=False,
        )
    ]


def _load_query_steps() -> list[WorkloadStep]:
    steps: list[WorkloadStep] = []
    for sql_path in sorted(QUERIES_DIR.glob("q*.sql")):
        sql = sql_path.read_text(encoding="utf-8").strip().rstrip(";").rstrip()
        steps.append(
            WorkloadStep(
                id=sql_path.stem,
                kind=STEP_SQL_QUERY,
                sql=sql,
                per_engine_sql={"df": df_qualify(sql, _JOB_TABLES, _ZONE, _SCHEMA)},
                description=f"JOB {sql_path.stem.upper()} (Delta read)",
                expects_rows=True,
            )
        )
    return steps


WORKLOAD = Workload(
    name="job_read_delta",
    description="113 JOB queries against the IMDB snapshot as plain Delta tables.",
    setup_steps=_setup_steps(),
    measured_steps=_load_query_steps(),
    cleanup_steps=_cleanup_steps(),
    requires_data_at_scale=None,
    # JOB is fixed-size; no scale placeholder in the dir name.
    data_subdir="job",
)
