"""TPC-H read workload: all 22 canonical queries against Delta tables.

Each query carries `per_engine_sql` so DF gets fully-qualified table names
(pbi.bench_tpch.lineitem) while Spark+DuckDB use the unqualified names from
the query files verbatim.
"""
from __future__ import annotations

import re
from pathlib import Path

from engines.base import STEP_SQL_QUERY, WorkloadStep

from ._fixtures import _DF_SCHEMA, make_delta_load_steps, make_drop_steps
from .spec import Workload

QUERIES_DIR = Path(__file__).parent / "tpch" / "queries"

_TPCH_TABLES = [
    "lineitem", "orders", "customer", "supplier",
    "part", "partsupp", "nation", "region",
]

# Pre-built regex: matches a bare table name surrounded by word boundaries.
_TABLE_RE = re.compile(
    r"\b(" + "|".join(_TPCH_TABLES) + r")\b",
    re.IGNORECASE,
)


def _qualify_for_df(sql: str) -> str:
    """Replace unqualified TPC-H table names with pbi.bench_tpch.<name>."""
    return _TABLE_RE.sub(lambda m: f"{_DF_SCHEMA}.{m.group(0).lower()}", sql)


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
    description="22 canonical TPC-H read queries against Delta tables.",
    setup_steps=make_delta_load_steps(measured=False),
    measured_steps=_load_query_steps(),
    cleanup_steps=make_drop_steps(),
    requires_data_at_scale=None,
)
