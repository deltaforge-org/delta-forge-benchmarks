"""TPC-H read workload: 22 canonical queries against the source parquet.

Read-only by construction: setup creates views (Spark/DuckDB) or external
tables (DeltaForge) over the same /workspace/data/tpch_sf{scale}/*.parquet
files. No data is copied during setup. Every engine therefore reads the
same on-disk bytes through its own parquet reader + planner + executor.

Cross-engine writes are measured separately by the `bulk_load` workload.

Each query carries `per_engine_sql` so df gets fully-qualified external
table names (bench_ext.tpch_read.lineitem) while Spark and DuckDB use the
unqualified names from the .sql files verbatim.
"""
from __future__ import annotations

import re
from pathlib import Path

from engines.base import STEP_SQL_QUERY, WorkloadStep

from ._fixtures import (
    _DF_READ_SCHEMA,
    make_parquet_view_drop_steps,
    make_parquet_view_steps,
)
from .spec import Workload

QUERIES_DIR = Path(__file__).parent / "tpch" / "queries"

_TPCH_TABLES = [
    "lineitem", "orders", "customer", "supplier",
    "part", "partsupp", "nation", "region",
]

# Word-boundary regex over the table names. Only applied at FROM-list-like
# positions in df qualification (see _qualify_for_df). Used by every engine
# that needs to know the bench's TPC-H table inventory.
_TABLE_RE = re.compile(
    r"\b(" + "|".join(_TPCH_TABLES) + r")\b",
    re.IGNORECASE,
)


def _qualify_for_df(sql: str) -> str:
    """Replace unqualified TPC-H table names with bench_ext.tpch_read.<name>.

    The regex still has the known limitation that it cannot distinguish a
    column alias named `nation` from the `nation` table reference. The two
    queries that exercise this (q08, q09) have been edited to use
    `nation_name` as the alias to sidestep the collision.
    """
    return _TABLE_RE.sub(lambda m: f"{_DF_READ_SCHEMA}.{m.group(0).lower()}", sql)


def _load_query_steps() -> list[WorkloadStep]:
    steps: list[WorkloadStep] = []
    for sql_path in sorted(QUERIES_DIR.glob("q*.sql")):
        sql = sql_path.read_text(encoding="utf-8").strip().rstrip(";").rstrip()
        df_sql = _qualify_for_df(sql)
        steps.append(
            WorkloadStep(
                id=sql_path.stem,
                kind=STEP_SQL_QUERY,
                sql=sql,
                per_engine_sql={"df": df_sql},
                description=f"TPC-H {sql_path.stem.upper()}",
                expects_rows=True,
            )
        )
    return steps


WORKLOAD = Workload(
    name="tpch_read",
    description="22 canonical TPC-H read queries over source parquet (no data copy at setup).",
    setup_steps=make_parquet_view_steps(measured=False),
    measured_steps=_load_query_steps(),
    cleanup_steps=make_parquet_view_drop_steps(),
    requires_data_at_scale=None,
)
