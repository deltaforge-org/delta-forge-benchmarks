"""Shared workload helpers.

Two things every TPC-H workload needs:
    1. A way to materialize the 8 Delta tables from the staged Parquet.
    2. A way to drop them on cleanup.

These helpers return WorkloadStep lists with `{data_dir}` placeholders that
the runner substitutes at execution time.
"""
from __future__ import annotations

from engines.base import STEP_SQL_DDL, WorkloadStep

# Loading order matters when using foreign-key-style joins later: parents
# (region, nation) before children (supplier, customer).
TPCH_LOAD_ORDER = [
    "region", "nation", "supplier", "customer",
    "part", "partsupp", "orders", "lineitem",
]


def make_delta_load_steps(measured: bool = False) -> list[WorkloadStep]:
    """CREATE OR REPLACE 8 Delta tables from staged Parquet. The
    `{data_dir}` placeholder is substituted by the runner.

    `measured=False` (the default) marks these as setup steps whose
    timings are recorded but excluded from the headline. The bulk_load
    workload flips this to True to put loading in the headline."""
    steps: list[WorkloadStep] = []
    for table in TPCH_LOAD_ORDER:
        steps.append(
            WorkloadStep(
                id=f"load_{table}",
                kind=STEP_SQL_DDL,
                sql=(
                    f"CREATE OR REPLACE TABLE {table} USING DELTA AS "
                    f"SELECT * FROM parquet.`{{data_dir}}/{table}.parquet`"
                ),
                description=f"Load {table} from Parquet -> Delta",
                measured=measured,
            )
        )
    return steps


def make_drop_steps() -> list[WorkloadStep]:
    """DROP all 8 tables. Reverse of load order so children go before parents."""
    return [
        WorkloadStep(
            id=f"drop_{table}",
            kind=STEP_SQL_DDL,
            sql=f"DROP TABLE IF EXISTS {table}",
            description=f"Drop {table}",
            measured=False,
        )
        for table in reversed(TPCH_LOAD_ORDER)
    ]
