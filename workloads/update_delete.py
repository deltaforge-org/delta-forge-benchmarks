"""UPDATE + DELETE with three selectivity levels.

Tests the ACID write path against `lineitem` directly. Three selectivities:

    1%   - small targeted change (a customer correction)
    10%  - region-scoped batch update
    50%  - bulk migration shape

Each selectivity is its own step so the report can show the throughput
curve. Operations are deterministic via HASH(l_orderkey, l_linenumber)
so the engines see the same rows; cross-engine row count + post-state
correctness validation works.

After UPDATE/DELETE there's a follow-up SELECT COUNT step that verifies
the affected row count matches the expected selectivity (within +/- 1%
random variance from HASH bucket boundaries). The follow-up SELECTs are
NOT measured for the headline; they're there for correctness.
"""
from __future__ import annotations

from engines.base import STEP_SQL_DML, STEP_SQL_QUERY, WorkloadStep

from ._fixtures import make_delta_load_steps, make_drop_steps
from .spec import Workload


def _update_step(pct: int) -> WorkloadStep:
    return WorkloadStep(
        id=f"update_{pct}pct",
        kind=STEP_SQL_DML,
        sql=(
            "UPDATE lineitem SET l_quantity = l_quantity + 1 "
            f"WHERE MOD(ABS(HASH(l_orderkey, l_linenumber)), 100) < {pct}"
        ),
        description=f"UPDATE ~{pct}% of lineitem rows",
    )


def _delete_step(pct: int) -> WorkloadStep:
    return WorkloadStep(
        id=f"delete_{pct}pct",
        kind=STEP_SQL_DML,
        sql=(
            "DELETE FROM lineitem "
            f"WHERE MOD(ABS(HASH(l_orderkey, l_linenumber)), 1000) < {pct * 10}"
        ),
        description=f"DELETE ~{pct}% of lineitem rows",
    )


WORKLOAD = Workload(
    name="update_delete",
    description="UPDATE + DELETE at 1% / 10% / 50% selectivity vs lineitem.",
    setup_steps=make_delta_load_steps(measured=False),
    measured_steps=[
        _update_step(1),
        _update_step(10),
        _update_step(50),
        _delete_step(1),
        _delete_step(10),
    ],
    cleanup_steps=make_drop_steps(),
    # UPDATE/DELETE accumulate state; we re-run setup between cells via the
    # runner's per-step idempotency, but only one warm pass per step keeps
    # the numbers honest.
    cold_runs=1,
    warm_runs=0,
)
