"""DeltaForge engine adapter (Phase 2 stub).

Phase 1 only declares the class so `bench_runner.py` can register it. The
real implementation lands in Phase 2: spawn `delta-forge-server` (control)
and `delta-forge-worker` (compute) as background processes inside the bench
container, wait for `/health`, then drive queries via `delta-forge-cli run
--format json` and parse the per-statement `execution_time_ms` field.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

from .base import Engine, RunResult


# Process-name patterns for the cold-run purge. These cover the CLI, the
# control plane, and the worker. `_purge.py` calls `pkill -f` with each
# pattern; matching nothing is fine, matching everything is fine.
DF_PROCESS_PATTERNS = [
    "delta-forge-cli",
    "delta-forge-server",
    "delta-forge-worker",
]


class DeltaForgeEngine(Engine):
    name = "df"

    def __init__(self) -> None:
        # Phase 2: store control_url, compute_url, credentials, paths.
        pass

    def version_info(self) -> dict[str, Any]:
        # Phase 2: invoke `delta-forge-cli --version`, capture build SHA from
        # env DF_GIT_SHA, record control + worker binary versions.
        return {
            "name": self.name,
            "df_git_sha": "unset",
            "phase1_stub": True,
        }

    def setup(self, scale: int, data_dir: Path) -> None:
        raise NotImplementedError("Phase 2: spawn server+worker, run schema.sql + load.sql")

    def teardown(self) -> None:
        raise NotImplementedError("Phase 2: SIGTERM the server+worker, wait, force-kill on timeout")

    def run_query(self, sql: str, query_id: str) -> RunResult:
        raise NotImplementedError(
            "Phase 2: subprocess delta-forge-cli run --format json, parse JSON, "
            "capture wall_ms via time.perf_counter() and engine_reported_ms from "
            "the JSON's execution_time_ms field"
        )
