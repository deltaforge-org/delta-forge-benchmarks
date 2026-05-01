"""TPC-H read workload: all 22 canonical queries against Delta tables.

The 22 queries are read from `workloads/tpch/queries/q01.sql` ... `q22.sql`
which were extracted verbatim from DuckDB's `tpch_queries()` function. This
keeps the queries version-controlled and reviewable as plain SQL.

Each query is a STEP_SQL_QUERY with `expects_rows=True`, so the runner
records `(row_count, sha256_of_sorted_result)` for cross-engine
correctness validation. A mismatch on any query is a release blocker.
"""
from __future__ import annotations

from pathlib import Path

from engines.base import STEP_SQL_QUERY, WorkloadStep

from ._fixtures import make_delta_load_steps, make_drop_steps
from .spec import Workload

QUERIES_DIR = Path(__file__).parent / "tpch" / "queries"


def _load_query_steps() -> list[WorkloadStep]:
    steps: list[WorkloadStep] = []
    for sql_path in sorted(QUERIES_DIR.glob("q*.sql")):
        sql = sql_path.read_text(encoding="utf-8").strip()
        # PySpark .sql() rejects trailing semicolons.
        sql = sql.rstrip(";").rstrip()
        steps.append(
            WorkloadStep(
                id=sql_path.stem,
                kind=STEP_SQL_QUERY,
                sql=sql,
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
    requires_data_at_scale=None,  # works at any scale; runner asserts data exists
)
