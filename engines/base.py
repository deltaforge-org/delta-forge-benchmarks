"""Engine adapter contract.

Every engine adapter implements the same shape so `bench_runner.py` can drive
them uniformly. Two principles:

1. Adapters never decide the protocol. They expose `start`, `stop`, `run_step`,
   `cold_start_metrics`, `version_info`. The cold/warm decision, drop-caches
   trigger, and run loop all live in the runner.
2. `run_step` returns a structured result, not raw timings. The runner is
   responsible for aggregating; the adapter is responsible for honestly
   reporting one execution including correctness fingerprints (row count +
   sha256 of the canonical-ordered result) so the report can detect
   cross-engine disagreements.

The abstraction is `WorkloadStep`, not `Query`, because real benchmarks
include writes, MERGE, OPTIMIZE, and other multi-statement work. A SELECT
is just a step that happens to return rows.
"""
from __future__ import annotations

import abc
import dataclasses
from pathlib import Path
from typing import Any, Callable


# Step kinds determine how the adapter executes the step and whether the
# runner expects a result set to fingerprint.
STEP_SQL_QUERY = "sql_query"      # SELECT-shaped; result rows fingerprinted
STEP_SQL_DML = "sql_dml"          # INSERT/UPDATE/DELETE/MERGE; no result rows
STEP_SQL_DDL = "sql_ddl"          # CREATE/DROP/ALTER; no result rows
STEP_MAINTENANCE = "maintenance"  # OPTIMIZE/VACUUM/ANALYZE; engine-specific
STEP_PYTHON = "python"            # arbitrary Python; non-portable, used for
                                   # data-prep steps that aren't SQL
STEP_CYPHER_QUERY = "cypher_query"  # MATCH/CALL ... YIELD ... RETURN ; rows fingerprinted
STEP_CYPHER_DML = "cypher_dml"      # CREATE/MERGE/DELETE/SET in Cypher; no rows

# All step kinds that carry textual SQL (substituted for {data_dir} placeholders
# by the runner). Cypher kinds use the same SQL field for the query text.
SQL_TEXT_KINDS = (
    STEP_SQL_QUERY, STEP_SQL_DML, STEP_SQL_DDL, STEP_MAINTENANCE,
    STEP_CYPHER_QUERY, STEP_CYPHER_DML,
)


@dataclasses.dataclass
class WorkloadStep:
    """One unit of measured (or unmeasured) work in a Workload."""

    id: str
    """Stable identifier within the workload, e.g. 'load_lineitem' or 'q05'.
    Used as a join key in the results table."""

    kind: str
    """One of STEP_SQL_QUERY / STEP_SQL_DML / STEP_SQL_DDL / STEP_MAINTENANCE
    / STEP_PYTHON."""

    sql: str | None = None
    """Query text. Required for SQL kinds and Cypher kinds. The same field
    carries Cypher when `kind` is STEP_CYPHER_*; the field is named `sql`
    only because the runner already calls it that. Adapters dispatch on
    `kind` to decide how to execute the text. The adapter is responsible
    for any engine-specific dialect translation, but should aim to run the
    text as-is when possible."""

    per_engine_sql: dict[str, str] | None = None
    """Optional per-engine override of `sql`. When the runner resolves a step
    for a specific engine and the engine's name appears as a key here, the
    adapter sees that variant in `step.sql` instead of the default. Used by
    workloads where the same logical step has divergent dialect (e.g. DF
    Cypher's `algo.pageRank(...)` vs Neo4j GDS's `gds.pageRank.stream(...)`)."""

    per_engine_kind: dict[str, str] | None = None
    """Optional per-engine override of `kind`. The graph_finance load step
    is SQL DDL on DF and Cypher DML on Neo4j; the workload sets this dict
    to ``{'df': STEP_SQL_DDL, 'neo4j': STEP_CYPHER_DML}`` so each adapter
    sees the kind it accepts."""

    fn: Callable[["Engine"], Any] | None = None
    """Python callable for STEP_PYTHON. Receives the engine adapter so it
    can run engine-specific data-prep (e.g. write a Parquet file from a
    DataFrame)."""

    description: str = ""
    """Human-readable description for the per-run report."""

    measured: bool = True
    """If False, the step runs but its timings are not part of the headline
    table. Setup and cleanup steps default to measured=False at the workload
    level, but individual steps can flip the flag."""

    expects_rows: bool = False
    """For SQL_QUERY only. When True, the runner fingerprints the result.
    Set False for SELECT statements whose row order is not deterministic
    and where you only care about row count."""


@dataclasses.dataclass
class StepResult:
    """One execution of one WorkloadStep on one engine."""

    step_id: str
    wall_ms: float
    """Wall-clock time the harness observed, end to end."""

    engine_reported_ms: float | None
    """Time the engine itself reported (e.g. delta-forge-cli's
    `execution_time_ms`, or Spark's queryExecution.executedPlan timing).
    None if the engine does not report one."""

    rss_peak_mb: float | None
    """Peak resident memory of the engine process tree during this step."""

    cpu_pct_avg: float | None
    """Average CPU percent of the engine process tree, sampled at 100ms."""

    rows_returned: int | None
    """Row count of the result set, when applicable."""

    result_sha256: str | None
    """Canonical hash of the result rows. The runner sets this for
    SQL_QUERY steps with expects_rows=True. Used by the report to assert
    cross-engine agreement; a mismatch is a release blocker."""

    exit_code: int
    """0 on success, non-zero otherwise. Non-zero results are still recorded
    so that 'X steps failed on engine Y' is part of the published table."""

    error: str | None = None
    """Truncated error message when exit_code != 0."""


@dataclasses.dataclass
class ColdStartMetrics:
    """Once-per-engine startup measurement, reported separately from query
    times. The 'JVM tax' for Spark, the process-spawn cost for DF.

    Measured from the first observable signal (Python `import` start,
    subprocess `Popen`) to the engine being able to execute a trivial
    `SELECT 1`. Captures the gap that ad-hoc/notebook users feel.
    """

    import_to_session_ready_ms: float
    """Time from engine.start() invocation to the engine being able to
    accept queries. For Spark: from get_spark() call to first sql() call
    completing on a trivial probe. For DF: from server start to /health 200."""

    session_to_first_query_ms: float
    """Time for the very first SELECT 1 after the engine is ready. Captures
    the JIT warm-up tail that survives setup."""


class Engine(abc.ABC):
    """Abstract base. Subclasses pick their own __init__ args."""

    name: str

    @abc.abstractmethod
    def version_info(self) -> dict[str, Any]:
        """Pinned versions and configuration flags. Goes into manifest.json."""

    @abc.abstractmethod
    def start(self) -> ColdStartMetrics:
        """Bring the engine up and measure cold-start. Returns the metric
        set even on failure (with NaN sentinels) so the manifest has a
        record."""

    @abc.abstractmethod
    def stop(self) -> None:
        """Stop processes, close sessions. Called before the next engine
        starts. Must leave no JVM heap, buffer pool, or background thread
        alive."""

    @abc.abstractmethod
    def run_step(self, step: WorkloadStep) -> StepResult:
        """Execute one step. Time it. Return a `StepResult`. Do not print to
        stdout, do not raise on engine error (capture into `error` and set
        a non-zero `exit_code`); the runner needs every result, including
        failures, in the published table.

        The adapter is responsible for:
        - Running the SQL exactly once.
        - Capturing wall-clock time via `time.perf_counter()`.
        - Capturing engine-reported time when available.
        - For SQL_QUERY with expects_rows=True: returning row count +
          deterministic SHA-256 of the result set in `result_sha256`.
        """
