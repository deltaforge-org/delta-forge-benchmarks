"""DeltaForge engine adapter.

Drives ``delta-forge-cli`` against an already-running control plane +
compute worker pair. The bench's docker-compose entrypoint
(``docker/bench-entrypoint.sh``) is responsible for starting Postgres,
delta-forge-server, and delta-forge-compute; this adapter is a *client*
to that stack.

This is the only adapter that handles both SQL and Cypher steps,
because DeltaForge is the only engine in the bench that runs both.
The dispatch is purely on ``step.kind`` because the server's
``execute_sql`` endpoint accepts either dialect and routes internally.

Run shape
---------

Every step is a single ``delta-forge-cli --format json query "<text>"``
invocation. The CLI auths via ``DF_USERNAME`` / ``DF_PASSWORD`` /
``DF_CONTROL_URL`` env vars (clap recognises these in
``delta-forge-cli/src/main.rs:33-46``). For ``STEP_*_QUERY`` we parse
the JSON shape produced by ``output::print_result_table``:

    {
      "columns":           [str, ...],
      "rows":              [{col: str_or_null}, ...],
      "row_count":         int,
      "execution_time_ms": int,
    }

For ``STEP_SQL_DDL`` / ``STEP_SQL_DML`` / ``STEP_CYPHER_DML`` /
``STEP_MAINTENANCE`` the CLI prints ``print_dml_result`` (no JSON), so
we treat exit-code 0 as success and skip the JSON parse.

Multi-statement scripts (e.g. the workload's load step) are split on
the same boundary the CLI's own ``run`` subcommand uses (statements
ending with `;`). Each statement is invoked independently so per-
statement timings are measurable.

RSS / CPU sampling
------------------

The DF compute worker is the heavy process; the CLI spawns once per
step and exits. We sample the worker's process tree via
``MetricSampler``, locating the worker PID via ``pgrep`` at adapter
startup. If pgrep does not find a worker (e.g. on a host where the
operator is running the bench against a remote control plane), RSS/CPU
are reported as ``None`` and the bench wall-clock + engine-reported
times remain valid.
"""
from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import time
from pathlib import Path
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
from . import _metrics


# Process-name patterns for the cold-run purge. The control plane and
# worker are kept alive across cold runs (entrypoint owns them); the
# OS page cache and the worker's in-process buffer pool are what cold
# actually invalidates here. To get a *fully* cold worker, the operator
# can `docker compose restart bench` between runs; the bench labels
# such runs explicitly.
DF_PROCESS_PATTERNS = [
    "delta-forge-cli",
    "delta-forge-server",
    "delta-forge-compute",
]


_DEFAULT_CONTROL_URL = "http://127.0.0.1:3000"
_DEFAULT_USERNAME = "admin"


class DeltaForgeEngine(Engine):
    """Client to a running delta-forge-server + delta-forge-compute pair."""

    name = "df"

    def __init__(
        self,
        cli_path: str | None = None,
        control_url: str | None = None,
        username: str | None = None,
        password: str | None = None,
    ) -> None:
        self._cli_path = (
            cli_path
            or os.environ.get("DF_CLI_PATH")
            or shutil.which("delta-forge-cli")
            or "delta-forge-cli"
        )
        self._control_url = (
            control_url
            or os.environ.get("DF_CONTROL_URL")
            or _DEFAULT_CONTROL_URL
        )
        self._username = username or os.environ.get("DF_USERNAME") or _DEFAULT_USERNAME
        # Two env-var sources for the password: DF_PASSWORD (the CLI's own
        # contract) and DELTA_FORGE_ADMIN_PASSWORD (the bench compose
        # contract). Whichever is set first wins.
        self._password = (
            password
            or os.environ.get("DF_PASSWORD")
            or os.environ.get("DELTA_FORGE_ADMIN_PASSWORD")
        )
        self._worker_pid: int | None = None
        self._cli_version: str | None = None

    # ----- lifecycle ---------------------------------------------------------

    def _cli_env(self) -> dict[str, str]:
        env = os.environ.copy()
        env["DF_CONTROL_URL"] = self._control_url
        env["DF_USERNAME"] = self._username
        if self._password:
            env["DF_PASSWORD"] = self._password
        return env

    def _locate_worker_pid(self) -> int | None:
        """Find delta-forge-compute's PID for RSS/CPU sampling.

        Falls back through `pgrep` (Linux/macOS) then a no-op on platforms
        without it. The PID is purely for the metric sampler; failing to
        find one is reported as RSS=None on every step, never an engine
        failure.
        """
        pgrep = shutil.which("pgrep")
        if not pgrep:
            return None
        try:
            r = subprocess.run(
                [pgrep, "-f", "delta-forge-compute"],
                capture_output=True, text=True, timeout=5,
            )
            if r.returncode != 0:
                return None
            for line in r.stdout.splitlines():
                line = line.strip()
                if line.isdigit():
                    return int(line)
        except (OSError, subprocess.TimeoutExpired):
            return None
        return None

    def start(self) -> ColdStartMetrics:
        if not self._password:
            raise RuntimeError(
                "DF_PASSWORD (or DELTA_FORGE_ADMIN_PASSWORD) is not set. "
                "The bench compose stack defines this; for ad-hoc runs, "
                "export it before invoking bench_runner.py."
            )

        # Wait for /health to come up. The compose entrypoint starts the
        # control plane in the background and may not be ready at the
        # instant the bench harness reaches this code.
        t0 = time.perf_counter()
        deadline = t0 + 120.0
        last_err: str = ""
        while time.perf_counter() < deadline:
            try:
                r = subprocess.run(
                    [self._cli_path, "health"],
                    env=self._cli_env(),
                    capture_output=True, text=True, timeout=10,
                )
                if r.returncode == 0:
                    break
                last_err = (r.stderr or r.stdout)[:500]
            except (OSError, subprocess.TimeoutExpired) as e:
                last_err = repr(e)
            time.sleep(1.0)
        else:
            raise RuntimeError(
                f"DeltaForge control plane at {self._control_url} not "
                f"reachable within 120s. last error: {last_err}"
            )
        ready_ms = (time.perf_counter() - t0) * 1000.0

        # First-query probe: SELECT 1.
        t1 = time.perf_counter()
        probe = subprocess.run(
            [self._cli_path, "--format", "json", "query", "SELECT 1 AS x"],
            env=self._cli_env(),
            capture_output=True, text=True, timeout=30,
        )
        first_q_ms = (time.perf_counter() - t1) * 1000.0
        if probe.returncode != 0:
            raise RuntimeError(
                f"DeltaForge SELECT 1 probe failed: {probe.stderr or probe.stdout!r}"
            )

        # Cache the worker PID once. If the worker restarts mid-run the
        # sampler will harmlessly observe a dead PID and report RSS=None
        # for affected steps; a fresh start() re-locates.
        self._worker_pid = self._locate_worker_pid()

        # Cache CLI version for version_info.
        try:
            v = subprocess.run(
                [self._cli_path, "--version"],
                capture_output=True, text=True, timeout=5,
            )
            self._cli_version = (v.stdout or v.stderr).strip() or "unknown"
        except (OSError, subprocess.TimeoutExpired):
            self._cli_version = "unknown"

        return ColdStartMetrics(
            import_to_session_ready_ms=ready_ms,
            session_to_first_query_ms=first_q_ms,
        )

    def stop(self) -> None:
        # The compose entrypoint owns server + worker. Nothing to stop on
        # the client side beyond clearing our own state.
        self._worker_pid = None

    # ----- introspection -----------------------------------------------------

    def version_info(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "control_url": self._control_url,
            "df_git_sha": os.environ.get("DF_GIT_SHA", "unset"),
            "cli_path": self._cli_path,
            "cli_version": self._cli_version or "<not started>",
            "worker_pid_at_start": self._worker_pid,
        }

    # ----- step execution ----------------------------------------------------

    def run_step(self, step: WorkloadStep) -> StepResult:
        sampler: _metrics.MetricSampler | None = None
        if self._worker_pid is not None:
            sampler = _metrics.MetricSampler(self._worker_pid)
            sampler.start()

        t0 = time.perf_counter()
        rows_returned: int | None = None
        result_sha: str | None = None
        engine_reported_ms: float | None = None
        exit_code = 0
        error: str | None = None
        extra: dict | None = None

        try:
            if step.kind == STEP_PYTHON:
                if step.fn is None:
                    raise ValueError("STEP_PYTHON requires fn")
                step.fn(self)

            elif step.kind in (STEP_SQL_DDL, STEP_SQL_DML, STEP_MAINTENANCE,
                               STEP_CYPHER_DML):
                if not step.sql:
                    raise ValueError(f"{step.kind} step requires sql")
                # Multi-statement scripts: run each separately.
                stmts = _split_sql_statements(step.sql)
                if not stmts:
                    raise ValueError(f"{step.kind} step has no statements")
                summed_engine_ms = 0.0
                saw_engine_ms = False
                for stmt in stmts:
                    proc = self._run_cli_query(stmt, timeout=3600.0)
                    if proc.returncode != 0:
                        raise RuntimeError(
                            f"delta-forge-cli failed (exit {proc.returncode}): "
                            f"{(proc.stderr or proc.stdout)[:1000]}"
                        )
                    parsed = _try_parse_cli_json(proc.stdout)
                    if parsed is not None and "execution_time_ms" in parsed:
                        summed_engine_ms += float(parsed["execution_time_ms"])
                        saw_engine_ms = True
                    else:
                        ms = _scrape_dml_ms(proc.stdout)
                        if ms is not None:
                            summed_engine_ms += ms
                            saw_engine_ms = True
                if saw_engine_ms:
                    engine_reported_ms = summed_engine_ms

            elif step.kind == STEP_CYPHER_QUERY:
                # Cypher graph queries (USE <graph> [ON GPU ...] CALL algo.* /
                # MATCH ... YIELD ... RETURN) now route natively under
                # `SHOW STATS ACTUAL`: the engine classifies the inner
                # statement and runs it through the native cypher executor
                # (the same path a bare cypher statement takes), then emits
                # per-phase metering rows (cypher.expand / cypher.property /
                # cypher.algorithm / cypher.gpu / cypher.csr) alongside the
                # standard time/rows rows. This makes the
                # cypher measurement uniform with the SQL branch (both report
                # `SHOW STATS ACTUAL.total_time_ms`) and surfaces the GPU
                # wall-time and per-phase breakdown the graph_gpu_vs_cpu
                # workload uses for the CPU-vs-GPU crossover. Cypher is a
                # single statement, so there is no `;`-preamble split.
                if not step.sql:
                    raise ValueError("cypher_query step requires query text")
                stats_sql = f"SHOW STATS ACTUAL {step.sql}"
                proc = self._run_cli_query(stats_sql, timeout=3600.0)
                if proc.returncode != 0:
                    raise RuntimeError(
                        f"delta-forge-cli failed (exit {proc.returncode}): "
                        f"{(proc.stderr or proc.stdout)[:1000]}"
                    )
                parsed = _try_parse_cli_json(proc.stdout)
                if parsed is None:
                    raise RuntimeError(
                        f"CLI did not return JSON for SHOW STATS (cypher); "
                        f"stdout head: {proc.stdout[:300]!r}"
                    )
                # Walk the long-form (category, metric, value, unit) rows.
                # total_time_ms is the headline engine time; the
                # expand/property/algorithm/gpu rows are captured into
                # `extra` so the crossover report can show device + the
                # per-phase split.
                phase_metrics: dict[str, Any] = {}
                inner_row_count: int | None = None
                for r in parsed.get("rows") or []:
                    cat = r.get("category")
                    met = r.get("metric")
                    val = r.get("value")
                    if cat == "time" and met == "total_time_ms":
                        engine_reported_ms = _stats_num(val)
                    elif cat == "time" and met == "total_engine_time_ms":
                        # Engine-only wall-clock (planning + execution +
                        # streaming, auth/control-plane excluded). The engine
                        # documents this as the apples-to-apples benchmark
                        # number (unified_executor::CompletionStats); capture
                        # it alongside total_time_ms so the report can show the
                        # auth-free compute time too.
                        v = _stats_num(val)
                        if v is not None:
                            phase_metrics["time.total_engine_time_ms"] = v
                    elif cat == "rows" and met == "rows_returned":
                        v = _stats_num(val)
                        if v is not None:
                            inner_row_count = int(v)
                    elif cat and cat.startswith("cypher.") and val not in (None, "None"):
                        # Per-phase native-Cypher breakdown. The engine
                        # namespaces every Cypher metering row under `cypher.`
                        # (cypher.expand / cypher.property / cypher.algorithm /
                        # cypher.gpu / cypher.csr; see
                        # unified_executor::actual_long_rows). Store the raw
                        # formatted cell; numeric consumers strip thousands
                        # separators via _stats_num. cypher.gpu.* is NULL (and
                        # thus skipped) on the CPU arm, so its presence also
                        # confirms the GPU path actually fired.
                        phase_metrics[f"{cat}.{met}"] = val
                rows_returned = inner_row_count if inner_row_count is not None else 0
                if phase_metrics:
                    extra = phase_metrics
                result_sha = None
                # A SHOW STATS that parsed but carried no time/total_time_ms row
                # is a contract break (a stats refactor moved or renamed the
                # row). Fail loudly here rather than letting the report silently
                # fall back to subprocess wall_ms, which would mix measurement
                # bases (CLI process wall-clock vs engine time) across the CPU
                # and GPU arms and quietly corrupt the crossover.
                if engine_reported_ms is None:
                    raise RuntimeError(
                        "SHOW STATS ACTUAL returned no time/total_time_ms row "
                        f"({len(parsed.get('rows') or [])} stats rows parsed); "
                        "refusing to report an untrusted measurement"
                    )

            elif step.kind == STEP_SQL_QUERY:
                if not step.sql:
                    raise ValueError(f"{step.kind} step requires query text")
                # Wrap the measured SELECT with `SHOW STATS ACTUAL` and report
                # SHOW STATS' `total_time_ms` (wall around the inner query
                # handler, plan + compile + execute + drain). The plain
                # streaming endpoint's `execution_time_ms` on this build
                # only covers planning/setup -- actual scan happens later
                # when partition tickets are drained -- which made our prior
                # df numbers measure planning, not execution.
                #
                # If the workload sent a multi-statement script (separated
                # by `;`), treat all statements EXCEPT the last as preamble
                # (e.g. `OPEN DELTA TABLE '...' AS lineitem`) and wrap only
                # the last with SHOW STATS. The preamble runs in the same
                # session as the SHOW STATS handler, so any OPENs are
                # visible when the inner query plans. Preamble time lands
                # in `wall_ms` but NOT in `engine_reported_ms`, matching
                # the DuckDB/Spark convention where setup is unmeasured.
                stmts = _split_sql_statements(step.sql)
                if not stmts:
                    raise ValueError(f"{step.kind} step has no SQL")
                preamble = stmts[:-1]
                select_stmt = stmts[-1]
                if preamble:
                    stats_sql = (
                        ";\n".join(preamble)
                        + ";\n"
                        + f"SHOW STATS ACTUAL {select_stmt}"
                    )
                else:
                    stats_sql = f"SHOW STATS ACTUAL {select_stmt}"
                proc = self._run_cli_query(stats_sql, timeout=3600.0)
                if proc.returncode != 0:
                    raise RuntimeError(
                        f"delta-forge-cli failed (exit {proc.returncode}): "
                        f"{(proc.stderr or proc.stdout)[:1000]}"
                    )
                parsed = _try_parse_cli_json(proc.stdout)
                if parsed is None:
                    raise RuntimeError(
                        f"CLI did not return JSON for SHOW STATS; "
                        f"stdout head: {proc.stdout[:300]!r}"
                    )
                # Walk the per-row stats records and pull total_time_ms and
                # the row count the underlying query produced. SHOW STATS
                # returns a "long" table of (category, metric, value, unit)
                # rows; we filter for the time/total_time_ms cell.
                engine_reported_ms = None
                inner_row_count = None
                for r in parsed.get("rows") or []:
                    if r.get("category") == "time" and r.get("metric") == "total_time_ms":
                        v = r.get("value")
                        if v not in (None, "None"):
                            try:
                                engine_reported_ms = float(v)
                            except (TypeError, ValueError):
                                pass
                    elif r.get("category") == "rows" and r.get("metric") == "rows_returned":
                        v = r.get("value")
                        if v not in (None, "None"):
                            try:
                                inner_row_count = int(float(v))
                            except (TypeError, ValueError):
                                pass
                rows_returned = inner_row_count if inner_row_count is not None else 0
                # SHOW STATS does not emit the underlying query's result rows
                # (it emits its own stats table instead). Result-set hashing
                # for cross-engine agreement therefore has to come from a
                # separate plain run. Skip the hash on df for now; the bench
                # framework still records row count, which catches the
                # gross-mismatch case.
                result_sha = None
                _ = step.expects_rows

            else:
                raise ValueError(f"unknown step kind: {step.kind}")

        except Exception as e:
            exit_code = 1
            error = repr(e)[:2000]

        wall_ms = (time.perf_counter() - t0) * 1000.0

        rss_peak: float | None = None
        cpu_pct: float | None = None
        if sampler is not None:
            sampler.stop()
            rss_peak = sampler.peak_rss_mb
            cpu_pct = sampler.avg_cpu_pct

        return StepResult(
            step_id=step.id,
            wall_ms=wall_ms,
            engine_reported_ms=engine_reported_ms,
            rss_peak_mb=rss_peak,
            cpu_pct_avg=cpu_pct,
            rows_returned=rows_returned,
            result_sha256=result_sha,
            exit_code=exit_code,
            error=error,
            extra=extra,
        )

    # ----- helpers -----------------------------------------------------------

    def _run_cli_query(self, sql: str, timeout: float) -> subprocess.CompletedProcess:
        """Invoke `delta-forge-cli --format json query <sql>`.

        We pass the SQL as a single positional argument; the CLI joins its
        positional args with spaces (`sql.join(" ")` in main.rs:227), which
        for a one-arg invocation reproduces the literal text including
        embedded newlines.
        """
        return subprocess.run(
            [self._cli_path, "--format", "json", "query", sql],
            env=self._cli_env(),
            capture_output=True, text=True, timeout=timeout,
        )


# ---------------------------------------------------------------------------
# Statement splitting + output parsing
# ---------------------------------------------------------------------------

# Match a top-level semicolon: a `;` followed by a newline OR end-of-string,
# *not* one that's inside a single-quoted string. The bench-authored scripts
# are deliberately simple (no dollar-quoting, no plpgsql blocks); a full
# tokenizer is overkill.
_SEMI_RE = re.compile(r";\s*(?:\n|$)")


def _split_sql_statements(text: str) -> list[str]:
    """Split a ``;``-terminated multi-statement script into individual
    statements. Whitespace-only / comment-only fragments are dropped.
    """
    out: list[str] = []
    last = 0
    for m in _SEMI_RE.finditer(text):
        chunk = text[last:m.start()].strip()
        last = m.end()
        if not chunk:
            continue
        # Drop pure-comment chunks.
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


_DML_MS_RE = re.compile(r"\((\d+)ms\)")


def _scrape_dml_ms(stdout: str) -> float | None:
    """When the CLI executes a DML/DDL it prints a one-liner like
    ``  ✓ Inserted 10000000 rows (8421ms)`` (see
    delta-forge-cli/src/output.rs:208). Recover the millisecond field
    so engine_reported_ms is populated for non-query steps too.
    """
    for line in stdout.splitlines():
        m = _DML_MS_RE.search(line)
        if m:
            try:
                return float(m.group(1))
            except ValueError:
                continue
    return None


def _stats_num(val: Any) -> float | None:
    """Parse a SHOW STATS ``value`` cell to a float.

    The stats table's ``value`` column is a *formatted* string: counts carry
    thousands separators (``"10,000,000"``) and times are plain decimals
    (``"85.452"``). Non-numeric cells (``"true"``, a cache tier name, an
    algorithm name) return None so callers can skip them. Stripping the
    separator is what keeps a multi-second ``total_time_ms`` (e.g.
    ``"5123.4"``) and a comma-grouped ``rows_returned`` both parseable.
    """
    if val in (None, "None"):
        return None
    try:
        return float(str(val).replace(",", ""))
    except (TypeError, ValueError):
        return None


def _try_parse_cli_json(stdout: str) -> dict | None:
    """Extract the JSON object from CLI stdout.

    The CLI may interleave informational lines (``Node: x → y``,
    ``Authenticating as ...``) before the JSON payload. The payload is
    pretty-printed, starts with a top-level `{`, and runs to the matching
    `}`. We scan for the first `{` at column 0 and decode from there.
    """
    if not stdout:
        return None
    # Fast path: stdout starts directly with the JSON.
    s = stdout.lstrip()
    if s.startswith("{"):
        try:
            return json.loads(s)
        except json.JSONDecodeError:
            pass

    # Slow path: find the first column-0 `{` and try to decode through to
    # the matching `}` by scanning braces.
    lines = stdout.splitlines()
    for i, line in enumerate(lines):
        if line.startswith("{"):
            depth = 0
            buf: list[str] = []
            for j in range(i, len(lines)):
                buf.append(lines[j])
                for ch in lines[j]:
                    if ch == "{":
                        depth += 1
                    elif ch == "}":
                        depth -= 1
                        if depth == 0:
                            try:
                                return json.loads("\n".join(buf))
                            except json.JSONDecodeError:
                                return None
            return None
    return None
