"""Neo4j engine adapter for the graph chapter of the bench.

Connects to a Neo4j Community 5.x instance (with the GDS Community plugin
loaded) over the Bolt protocol and executes Cypher against it. Used by the
graph_finance workload to produce a head-to-head comparison against the
DeltaForge graph runtime.

Lifecycle
---------

- ``start()`` opens a Bolt session, probes ``CALL db.ping()``, and records
  the ``ColdStartMetrics`` for the run. The Neo4j *server* itself is brought
  up by docker-compose (``services.neo4j`` in ``docker/docker-compose.yml``);
  this adapter is purely a client. We do not start or stop the server
  process from inside the bench container, because the Neo4j data directory
  must persist across step re-runs the way the bench expects.
- ``stop()`` closes the driver. The server keeps running; the next engine
  in the schedule does not need it stopped.
- ``run_step()`` dispatches on ``step.kind``: Cypher kinds run via the
  driver, the trio of SQL kinds raise (Neo4j is not a SQL engine), and
  ``STEP_PYTHON`` invokes the callable.

Cross-engine correctness
------------------------

For ``STEP_CYPHER_QUERY`` with ``expects_rows=True``, we hash the result
rows using the canonical hasher in ``workloads.spec.hash_result_rows``,
the same one the Spark and DF adapters use. The hash is order-insensitive
(rows are sorted before hashing), so two engines that return the same
multiset of rows produce the same digest. This is how the report flags
silent disagreement between engines.

For non-deterministic algorithms (Louvain, sampled betweenness), the
workload sets ``expects_rows=False`` on the step so we record row count
and timing but skip the cross-engine hash compare.

Memory metrics
--------------

The MetricSampler in ``engines/_metrics.py`` walks the process tree of a
PID. The Neo4j JVM runs in a *different container*, so its PID is invisible
from the bench container's view of /proc. Rather than ship a fragile
``docker stats`` shim, this adapter reports ``rss_peak_mb=None`` and
``cpu_pct_avg=None`` for Neo4j steps. The wall-clock and engine-reported
times are the comparable numbers; RSS for the Neo4j side, when needed, is
recoverable from ``docker stats neo4j`` during the run. This is documented
in the report and in the bench README.
"""
from __future__ import annotations

import os
import time
from typing import Any

from .base import (
    STEP_CYPHER_DML,
    STEP_CYPHER_QUERY,
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


# Process patterns the cold-run purge would target if the bench managed the
# JVM itself. The compose-managed Neo4j runs in its own container, so the
# bench host does not see these PIDs; left in place for environments where
# the user runs Neo4j directly on the bench host.
NEO4J_PROCESS_PATTERNS = [
    "java.*org.neo4j.server.CommunityEntryPoint",
    "neo4j.*server",
]


_DEFAULT_BOLT_URL = "bolt://localhost:7687"
_DEFAULT_USER = "neo4j"
_DEFAULT_DATABASE = "neo4j"


class Neo4jEngine(Engine):
    """Neo4j Community + GDS Community client adapter."""

    name = "neo4j"

    def __init__(
        self,
        bolt_url: str | None = None,
        user: str | None = None,
        password: str | None = None,
        database: str | None = None,
    ) -> None:
        self._bolt_url = bolt_url or os.environ.get("NEO4J_BOLT_URL", _DEFAULT_BOLT_URL)
        self._user = user or os.environ.get("NEO4J_USER", _DEFAULT_USER)
        # Password is required. Compose sets NEO4J_AUTH=neo4j/<password> on the
        # neo4j service and NEO4J_PASSWORD on the bench container; we read the
        # latter. There is no default credential because Neo4j 5+ refuses the
        # built-in default password until it has been changed.
        self._password = password or os.environ.get("NEO4J_PASSWORD")
        self._database = database or os.environ.get("NEO4J_DATABASE", _DEFAULT_DATABASE)
        self._driver: Any = None

    # ----- lifecycle ---------------------------------------------------------

    def _connect(self) -> Any:
        if not self._password:
            raise RuntimeError(
                "NEO4J_PASSWORD environment variable is not set. "
                "Set it (and NEO4J_AUTH=neo4j/<same> on the neo4j compose service) "
                "before starting the bench."
            )
        try:
            from neo4j import GraphDatabase  # type: ignore
        except ImportError as e:
            raise RuntimeError(
                "The 'neo4j' driver is not installed. "
                "Run `pip install -r requirements.txt` (it is pinned there)."
            ) from e

        return GraphDatabase.driver(
            self._bolt_url,
            auth=(self._user, self._password),
            # Connection-acquisition timeout is conservative because graph
            # algorithms can hold a session for minutes on a 10M graph.
            connection_acquisition_timeout=600.0,
            max_connection_lifetime=3600.0,
        )

    def start(self) -> ColdStartMetrics:
        t0 = time.perf_counter()
        self._driver = self._connect()
        # Neo4j 5+ exposes db.ping(); fall back to a trivial RETURN if the
        # procedure is unavailable on this build (e.g. older Community).
        ready_ms: float
        first_q_ms: float
        # Wait until the server accepts connections. Compose `depends_on`
        # does not wait for the bolt port to be reachable, only the container
        # to start, so this loop is the real readiness gate.
        deadline = time.perf_counter() + 120.0
        last_err: Exception | None = None
        while time.perf_counter() < deadline:
            try:
                with self._driver.session(database=self._database) as s:
                    s.run("RETURN 1 AS x").consume()
                break
            except Exception as e:  # pragma: no cover (timing-dependent)
                last_err = e
                time.sleep(1.0)
        else:
            raise RuntimeError(
                f"Neo4j at {self._bolt_url} not reachable within 120s: {last_err}"
            )
        ready_ms = (time.perf_counter() - t0) * 1000.0

        t1 = time.perf_counter()
        with self._driver.session(database=self._database) as s:
            s.run("RETURN 1 AS x").consume()
        first_q_ms = (time.perf_counter() - t1) * 1000.0

        return ColdStartMetrics(
            import_to_session_ready_ms=ready_ms,
            session_to_first_query_ms=first_q_ms,
        )

    def stop(self) -> None:
        if self._driver is not None:
            try:
                self._driver.close()
            finally:
                self._driver = None

    # ----- introspection -----------------------------------------------------

    def version_info(self) -> dict[str, Any]:
        info: dict[str, Any] = {
            "name": self.name,
            "bolt_url": self._bolt_url,
            "database": self._database,
            "config_source": "engines/neo4j_engine.py + docker/docker-compose.yml",
        }
        if self._driver is None:
            return info
        try:
            with self._driver.session(database=self._database) as s:
                rec = s.run(
                    "CALL dbms.components() YIELD name, versions, edition "
                    "RETURN name, versions, edition"
                ).single()
                if rec is not None:
                    info["neo4j_component"] = rec.get("name")
                    info["neo4j_versions"] = list(rec.get("versions") or ())
                    info["neo4j_edition"] = rec.get("edition")
                # GDS version, if the plugin is loaded.
                try:
                    gds = s.run("RETURN gds.version() AS v").single()
                    if gds is not None:
                        info["gds_version"] = gds["v"]
                except Exception as e:
                    info["gds_version"] = f"<unavailable: {e!r}>"
        except Exception as e:
            info["introspection_error"] = repr(e)
        return info

    # ----- step execution ----------------------------------------------------

    def run_step(self, step: WorkloadStep) -> StepResult:
        if self._driver is None:
            return _failure(step.id, "engine not started")

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

            elif step.kind in (STEP_SQL_DDL, STEP_SQL_DML, STEP_SQL_QUERY,
                               STEP_MAINTENANCE):
                raise ValueError(
                    f"Neo4j cannot execute {step.kind} steps. The workload "
                    f"either ran on the wrong engine or is missing a "
                    f"per_engine_sql override for 'neo4j'."
                )

            elif step.kind == STEP_CYPHER_DML:
                if not step.sql:
                    raise ValueError("CYPHER_DML requires sql/cypher text")
                with self._driver.session(database=self._database) as s:
                    # Run each statement separately when the workload glued
                    # several together with `;`. The Bolt protocol accepts a
                    # single statement per .run() call.
                    summary = None
                    for stmt in _split_cypher_statements(step.sql):
                        result = s.run(stmt)
                        summary = result.consume()
                    if summary is not None:
                        engine_reported_ms = _summary_total_ms(summary)

            elif step.kind == STEP_CYPHER_QUERY:
                if not step.sql:
                    raise ValueError("CYPHER_QUERY requires cypher text")
                with self._driver.session(database=self._database) as s:
                    stmts = _split_cypher_statements(step.sql)
                    if not stmts:
                        raise ValueError("CYPHER_QUERY has no statements")
                    # Only the LAST statement returns rows; earlier ones (e.g.
                    # MATCH-as-side-effect) are run for effect.
                    for stmt in stmts[:-1]:
                        s.run(stmt).consume()
                    result = s.run(stmts[-1])
                    raw_rows = list(result)
                    summary = result.consume()
                    engine_reported_ms = _summary_total_ms(summary)

                    # Render each Record into a tuple of cell values in
                    # declared key order. The Spark/DF adapters do the same
                    # thing (df.collect() yields Row objects we tuple()).
                    rows = [tuple(r.values()) for r in raw_rows]
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

        return StepResult(
            step_id=step.id,
            wall_ms=wall_ms,
            engine_reported_ms=engine_reported_ms,
            # See module docstring "Memory metrics": Neo4j JVM runs in a
            # different container; psutil cannot see its PID.
            rss_peak_mb=None,
            cpu_pct_avg=None,
            rows_returned=rows_returned,
            result_sha256=result_sha,
            exit_code=exit_code,
            error=error,
        )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _split_cypher_statements(text: str) -> list[str]:
    """Split a multi-statement Cypher block into individual statements.

    Bolt's session.run() accepts one statement at a time. We split on the
    semicolon at end-of-line. This is intentionally permissive: the bench
    is the only writer of these scripts and they do not contain semicolons
    inside strings or quoted identifiers.
    """
    out: list[str] = []
    cur: list[str] = []
    for line in text.splitlines():
        stripped = line.rstrip()
        # Skip cypher line comments and pure blank lines.
        s = stripped.lstrip()
        if s.startswith("//") or not s:
            cur.append(line)
            continue
        if stripped.endswith(";"):
            cur.append(stripped[:-1])
            stmt = "\n".join(cur).strip()
            if stmt:
                out.append(stmt)
            cur = []
        else:
            cur.append(line)
    tail = "\n".join(cur).strip()
    if tail:
        out.append(tail)
    return out


def _summary_total_ms(summary: Any) -> float | None:
    """Return total Neo4j-reported execution time in milliseconds.

    The driver exposes:
      summary.result_available_after  (planning + first-row latency)
      summary.result_consumed_after   (full result streamed back)
    Both are in milliseconds. We sum them so the engine_reported_ms field
    is comparable to Spark's queryExecution time and DF's
    execution_time_ms (both of which include planning + execution).
    """
    if summary is None:
        return None
    try:
        a = summary.result_available_after  # may be None on some drivers
        c = summary.result_consumed_after
    except AttributeError:
        return None
    if a is None and c is None:
        return None
    return float((a or 0) + (c or 0))


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
