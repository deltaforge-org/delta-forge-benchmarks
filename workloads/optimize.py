"""OPTIMIZE workload: file compaction throughput.

Real Delta workloads accumulate small files from streaming writes /
frequent appends; OPTIMIZE compacts them into bigger ones. We simulate
the small-file pile by writing `lineitem` 8 times into the target table
in append mode, then running OPTIMIZE.

Both engines should accept the canonical Delta `OPTIMIZE <table>` syntax.
If they do not, the engine adapter raises and the report records a
"workload not supported on engine X" failure rather than silently
skipping.
"""
from __future__ import annotations

from engines.base import STEP_MAINTENANCE, STEP_SQL_DDL, STEP_SQL_DML, WorkloadStep

from ._fixtures import make_delta_load_steps, make_drop_steps
from .spec import Workload


# 8 small appends so OPTIMIZE has visible work to compact.
_APPEND_SQL = """
INSERT INTO lineitem
SELECT * FROM parquet.`{data_dir}/lineitem.parquet`
""".strip()


WORKLOAD = Workload(
    name="optimize",
    description="OPTIMIZE compacts a lineitem table grown by 8 small appends.",
    setup_steps=[
        *make_delta_load_steps(measured=False),
        *(
            WorkloadStep(
                id=f"append_{i}",
                kind=STEP_SQL_DML,
                sql=_APPEND_SQL,
                description=f"Append #{i} (build small-file pile)",
                measured=False,
            )
            for i in range(1, 9)
        ),
    ],
    measured_steps=[
        WorkloadStep(
            id="optimize_lineitem",
            kind=STEP_MAINTENANCE,
            sql="OPTIMIZE lineitem",
            description="Compact small files for lineitem",
        ),
    ],
    cleanup_steps=make_drop_steps(),
    cold_runs=1,
    warm_runs=0,
)
