"""DuckDB native-engine adapter.

Comparison shape: DuckDB loads the TPC-H parquet into native columnar storage
during the setup phase (CTAS from read_parquet), then runs Q01..Q22 against
those in-memory tables. This mirrors Spark's CREATE OR REPLACE TABLE pattern
and lets the measured-query times reflect DuckDB's execution engine without
the parquet-decode tail in every query.

The connection is process-local (`duckdb.connect(":memory:")`), so RSS is
sampled from the bench Python process. Cold-start is the import + first
trivial query.
"""
from __future__ import annotations

import os
import time
from typing import Any

from .base import (
    STEP_MAINTENANCE,
    STEP_PYTHON,
    STEP_SQL_DDL,
    STEP_SQL_DML,
    STEP_SQL_QUERY,
    ColdStartMetrics,
    Engine,
    StepResult,
    WorkloadStep,
)
from . import _metrics


class DuckDBEngine(Engine):
    name = "duckdb"

    def __init__(self) -> None:
        self._conn = None
        self._session_pid: int | None = None
        self._version: str | None = None

    def _build_connection(self):
        import duckdb
        self._version = duckdb.__version__
        conn = duckdb.connect(":memory:")
        # Match the container's cgroup budget so DuckDB cannot exceed what
        # the other engines see. BENCH_CPUS / BENCH_MEMORY are exported by
        # docker-compose so every engine reads the same governor values.
        cpus = os.environ.get("BENCH_CPUS")
        memory = os.environ.get("BENCH_MEMORY")
        if cpus:
            try:
                conn.execute(f"PRAGMA threads = {int(cpus)}")
            except Exception:
                pass
        if memory:
            try:
                conn.execute(f"PRAGMA memory_limit = '{memory}'")
            except Exception:
                pass
        return conn

    def start(self) -> ColdStartMetrics:
        t0 = time.perf_counter()
        self._conn = self._build_connection()
        ready_ms = (time.perf_counter() - t0) * 1000.0
        self._session_pid = os.getpid()

        t1 = time.perf_counter()
        self._conn.execute("SELECT 1 AS x").fetchall()
        first_q_ms = (time.perf_counter() - t1) * 1000.0

        return ColdStartMetrics(
            import_to_session_ready_ms=ready_ms,
            session_to_first_query_ms=first_q_ms,
        )

    def stop(self) -> None:
        try:
            if self._conn is not None:
                self._conn.close()
        finally:
            self._conn = None
            self._session_pid = None

    def version_info(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "duckdb_version": self._version,
            "threads_env": os.environ.get("BENCH_CPUS"),
            "memory_limit_env": os.environ.get("BENCH_MEMORY"),
        }

    def run_step(self, step: WorkloadStep) -> StepResult:
        if self._conn is None:
            return _failure(step.id, "engine not started")
        if self._session_pid is None:
            return _failure(step.id, "session pid not set")

        sampler = _metrics.MetricSampler(self._session_pid)
        sampler.start()

        t0 = time.perf_counter()
        rows_returned: int | None = None
        result_sha: str | None = None
        exit_code = 0
        error: str | None = None

        try:
            if step.kind == STEP_PYTHON:
                if step.fn is None:
                    raise ValueError("STEP_PYTHON requires fn")
                step.fn(self)

            elif step.kind in (STEP_SQL_DDL, STEP_SQL_DML, STEP_MAINTENANCE):
                if not step.sql:
                    raise ValueError(f"{step.kind} step requires sql")
                # DuckDB accepts multi-statement scripts on execute().
                self._conn.execute(step.sql)

            elif step.kind == STEP_SQL_QUERY:
                if not step.sql:
                    raise ValueError("SQL_QUERY requires sql")
                rows = self._conn.execute(step.sql).fetchall()
                rows_returned = len(rows)
                if step.expects_rows:
                    from workloads.spec import hash_result_rows
                    result_sha, _ = hash_result_rows(rows)

            else:
                raise ValueError(f"unknown step kind: {step.kind}")

        except Exception as e:
            exit_code = 1
            error = repr(e)[:2000]

        wall_ms = (time.perf_counter() - t0) * 1000.0
        sampler.stop()

        return StepResult(
            step_id=step.id,
            wall_ms=wall_ms,
            engine_reported_ms=None,
            rss_peak_mb=sampler.peak_rss_mb,
            cpu_pct_avg=sampler.avg_cpu_pct,
            rows_returned=rows_returned,
            result_sha256=result_sha,
            exit_code=exit_code,
            error=error,
        )


def _failure(step_id: str, message: str) -> StepResult:
    return StepResult(
        step_id=step_id,
        wall_ms=0.0,
        engine_reported_ms=None,
        rss_peak_mb=None,
        cpu_pct_avg=None,
        rows_returned=None,
        result_sha256=None,
        exit_code=2,
        error=message,
    )
