"""MERGE workload: 1% updates + 1% deletes against `lineitem` (CDC shape).

This is what production Delta workloads spend a lot of time on:
applying a stream of changes to a fact table. The shape:

    target:  full lineitem (6.0M rows at SF=1, 60M at SF=10)
    source:  pre-built `cdc_changes` table containing
             - 1% of lineitem rows tagged as UPDATEs (l_quantity bumped)
             - 1% of lineitem rows tagged as DELETEs
             - 0 rows tagged as INSERTs (kept simple for v0.1)

The MERGE step is one statement that the engine plans and executes. We
measure that statement only. The 1% rows are deterministic (modulo of
l_orderkey + l_linenumber) so the result is reproducible across engines.

Engine portability note: Spark accepts the standard SQL MERGE INTO
syntax against Delta tables. DeltaForge is expected to as well; if
the dialects diverge, this workload picks up the difference and the
runner will report engine-specific errors rather than silently skipping.
"""
from __future__ import annotations

from engines.base import STEP_SQL_DDL, STEP_SQL_DML, WorkloadStep

from ._fixtures import make_delta_load_steps, make_drop_steps
from .spec import Workload


# The "changes" feed. Built once per workload run from the loaded `lineitem`.
# Tag column distinguishes UPDATEs (op='U') from DELETEs (op='D').
PREPARE_CHANGES_SQL = """
CREATE OR REPLACE TABLE cdc_changes USING DELTA AS
SELECT
    l_orderkey,
    l_linenumber,
    l_quantity + 1.0 AS l_quantity_new,
    CASE
        WHEN MOD(ABS(HASH(l_orderkey, l_linenumber)), 100) < 1 THEN 'U'
        WHEN MOD(ABS(HASH(l_orderkey, l_linenumber)), 100) < 2 THEN 'D'
        ELSE NULL
    END AS op
FROM lineitem
WHERE MOD(ABS(HASH(l_orderkey, l_linenumber)), 100) < 2
""".strip()

MERGE_SQL = """
MERGE INTO lineitem AS t
USING cdc_changes AS s
    ON t.l_orderkey = s.l_orderkey
   AND t.l_linenumber = s.l_linenumber
WHEN MATCHED AND s.op = 'U' THEN UPDATE SET t.l_quantity = s.l_quantity_new
WHEN MATCHED AND s.op = 'D' THEN DELETE
""".strip()


WORKLOAD = Workload(
    name="merge_cdc",
    description="MERGE applying ~1% updates + ~1% deletes to lineitem.",
    setup_steps=[
        *make_delta_load_steps(measured=False),
        WorkloadStep(
            id="prepare_changes",
            kind=STEP_SQL_DDL,
            sql=PREPARE_CHANGES_SQL,
            description="Build deterministic 2% changes feed from lineitem",
            measured=False,
        ),
    ],
    measured_steps=[
        WorkloadStep(
            id="merge_apply",
            kind=STEP_SQL_DML,
            sql=MERGE_SQL,
            description="Apply 1% UPDATE + 1% DELETE via MERGE",
        ),
    ],
    cleanup_steps=[
        WorkloadStep(
            id="drop_cdc_changes",
            kind=STEP_SQL_DDL,
            sql="DROP TABLE IF EXISTS cdc_changes",
            description="Drop changes feed",
            measured=False,
        ),
        *make_drop_steps(),
    ],
    # The MERGE accumulates state; warm runs after the first show different
    # cost (less to update). Cap warm to 3 so the report reflects realistic
    # repeated-merge cost rather than degenerating to a near-no-op.
    cold_runs=1,
    warm_runs=2,
)
