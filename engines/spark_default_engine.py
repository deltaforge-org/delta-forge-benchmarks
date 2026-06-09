"""Spark with stock-OSS defaults.

The config below mirrors `delta-forge/delta-forge-demos/verify_lib/spark_session.py`
verbatim: `local[*]`, 4 GB driver, no AQE override, default shuffle
partitions. This is what a user gets from `pip install pyspark==4.0.0
delta-spark==4.0.0` with no further tuning. Publishing this baseline alongside
the tuned baseline is the only defensible answer to the "you misconfigured
Spark" critique.

The adapter exposes:
- start():    builds the SparkSession, measures cold-start, returns metrics.
- stop():     stops the session.
- run_step(): executes one workload step, captures wall-clock + Spark plan
              time + RSS/CPU + (for SELECTs) row count + canonical hash.
- version_info(): returns the literal spark.* config keys the session was
              built with so the report can quote them verbatim.
"""
from __future__ import annotations

import os
import re
import time
from pathlib import Path
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
from . import _spark_session


class SparkDefaultEngine(Engine):
    """Stock-OSS-default Spark. Override only by SparkTunedEngine."""

    name = "spark-default"

    # Subclasses override; the literal builder config is in _spark_session.py.
    _config_keys: dict[str, str] = {
        "spark.sql.extensions":             "io.delta.sql.DeltaSparkSessionExtension",
        "spark.sql.catalog.spark_catalog":  "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        "spark.driver.memory":              "4g",
        "spark.ui.showConsoleProgress":     "false",
        "spark.log.level":                  "WARN",
        "spark.master":                     "local[*]",
    }

    def __init__(self) -> None:
        self._spark = None
        self._session_pid: int | None = None

    # ----- lifecycle ---------------------------------------------------------

    def _build_session(self):
        """Subclasses override to swap in their own builder config."""
        return _spark_session.get_spark()

    def start(self) -> ColdStartMetrics:
        t0 = time.perf_counter()
        self._spark = self._build_session()
        ready_ms = (time.perf_counter() - t0) * 1000.0
        self._session_pid = os.getpid()  # PySpark driver runs in this process

        t1 = time.perf_counter()
        # Trivial probe: forces classloading + Delta extension init.
        self._spark.sql("SELECT 1 AS x").collect()
        first_q_ms = (time.perf_counter() - t1) * 1000.0

        return ColdStartMetrics(
            import_to_session_ready_ms=ready_ms,
            session_to_first_query_ms=first_q_ms,
        )

    def stop(self) -> None:
        try:
            _spark_session.stop_spark()
        finally:
            self._spark = None
            self._session_pid = None

    # ----- introspection -----------------------------------------------------

    def version_info(self) -> dict[str, Any]:
        info: dict[str, Any] = {
            "name": self.name,
            "config_source": "engines/_spark_session.py",
            "config_keys": dict(self._config_keys),
        }
        if self._spark is not None:
            try:
                info["spark_version"] = self._spark.version
                info["spark_app_name"] = self._spark.sparkContext.appName
                info["spark_master"] = self._spark.sparkContext.master
                info["resolved_config"] = {
                    k: self._spark.conf.get(k, "<unset>") for k in self._config_keys
                }
            except Exception as e:
                info["introspection_error"] = repr(e)
        return info

    # ----- step execution ----------------------------------------------------

    def run_step(self, step: WorkloadStep) -> StepResult:
        if self._spark is None:
            return _failure(step.id, "engine not started")

        if self._session_pid is None:
            return _failure(step.id, "session pid not set")

        sampler = _metrics.MetricSampler(self._session_pid)
        sampler.start()

        t0 = time.perf_counter()
        rows_returned: int | None = None
        result_sha: str | None = None
        engine_reported_ms: float | None = None
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
                # Spark's .sql() accepts only one statement at a time. Split
                # on `;` so workloads can hand us drop-then-create-then-insert
                # scripts the same way df does. Engine-reported-ms is the sum
                # of per-statement queryExecution durations.
                stmts = _split_sql_statements(step.sql)
                if not stmts:
                    raise ValueError(f"{step.kind} step has no statements")
                summed_ns = 0
                saw_any_ns = False
                for stmt in stmts:
                    df = self._spark.sql(stmt)
                    _ = df.collect()
                    try:
                        qe = df._jdf.queryExecution()
                        phases = qe.tracker().phases()
                        it = phases.entrySet().iterator()
                        while it.hasNext():
                            e = it.next()
                            summed_ns += int(e.getValue().durationMs()) * 1_000_000
                            saw_any_ns = True
                    except Exception:
                        pass
                if saw_any_ns and summed_ns > 0:
                    engine_reported_ms = summed_ns / 1_000_000.0

            elif step.kind == STEP_SQL_QUERY:
                if not step.sql:
                    raise ValueError("SQL_QUERY requires sql")
                df = self._spark.sql(step.sql)
                rows = df.collect()
                rows_returned = len(rows)
                if step.expects_rows:
                    from workloads.spec import hash_result_rows
                    result_sha, _ = hash_result_rows(tuple(r) for r in rows)
                # Best-effort: pull Spark's reported planning + execution time.
                try:
                    qe = df._jdf.queryExecution()
                    metrics = qe.tracker().phases()
                    # Sum of phase durations in nanoseconds, when available.
                    total_ns = 0
                    it = metrics.entrySet().iterator()
                    while it.hasNext():
                        e = it.next()
                        total_ns += int(e.getValue().durationMs()) * 1_000_000
                    if total_ns > 0:
                        engine_reported_ms = total_ns / 1_000_000.0
                except Exception:
                    pass

            else:
                raise ValueError(f"unknown step kind: {step.kind}")

        except Exception as e:
            exit_code = 1
            error = (repr(e))[:2000]

        wall_ms = (time.perf_counter() - t0) * 1000.0
        sampler.stop()

        return StepResult(
            step_id=step.id,
            wall_ms=wall_ms,
            engine_reported_ms=engine_reported_ms,
            rss_peak_mb=sampler.peak_rss_mb,
            cpu_pct_avg=sampler.avg_cpu_pct,
            rows_returned=rows_returned,
            result_sha256=result_sha,
            exit_code=exit_code,
            error=error,
        )


_SEMI_RE = re.compile(r";")


def _split_sql_statements(text: str) -> list[str]:
    """Split a ``;``-terminated multi-statement script into individual
    statements. Whitespace-only / comment-only fragments are dropped.
    Mirrors engines/df_engine.py:_split_sql_statements so the two adapters
    accept the same multi-statement workload SQL."""
    out: list[str] = []
    last = 0
    for m in _SEMI_RE.finditer(text):
        chunk = text[last:m.start()].strip()
        last = m.end()
        if not chunk:
            continue
        non_comment = "\n".join(
            line for line in chunk.splitlines()
            if line.strip() and not line.strip().startswith("--")
        )
        if non_comment.strip():
            out.append(chunk)
    tail = text[last:].strip()
    if tail:
        non_comment = "\n".join(
            line for line in tail.splitlines()
            if line.strip() and not line.strip().startswith("--")
        )
        if non_comment.strip():
            out.append(tail)
    return out


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
