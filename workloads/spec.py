"""Workload schema and the canonical result hasher.

Two facts that show up everywhere:

1. A workload has three phases: setup (idempotent prep), measured (the
   timed core), and cleanup. Only measured steps appear in the headline
   table; setup/cleanup wall-clock times are recorded for transparency
   but not for ranking.

2. Cross-engine correctness is validated by fingerprinting result rows
   with a deterministic hash. The hash function lives in this module so
   adapters that don't go through PySpark's collect() API can compute
   the same hash from their own row representation.
"""
from __future__ import annotations

import dataclasses
import hashlib
from pathlib import Path
from typing import Iterable

from engines.base import StepResult, WorkloadStep


@dataclasses.dataclass
class Workload:
    """A named, multi-step benchmark."""

    name: str
    """Stable identifier, e.g. 'tpch_read', 'bulk_load', 'merge_cdc'."""

    description: str
    """One-line human description for the report."""

    setup_steps: list[WorkloadStep] = dataclasses.field(default_factory=list)
    """Run once per engine spin-up. Always executed; timings recorded but
    not part of the headline. Includes things like CREATE TABLE + COPY
    from Parquet."""

    measured_steps: list[WorkloadStep] = dataclasses.field(default_factory=list)
    """The timed core. Each step runs once cold + N warm by default."""

    cleanup_steps: list[WorkloadStep] = dataclasses.field(default_factory=list)
    """Run once per engine teardown. DROP tables, reset state. Not measured."""

    # Optional knobs the runner reads when scheduling.
    cold_runs: int = 1
    warm_runs: int = 9
    requires_data_at_scale: int | None = None
    """If set, the runner refuses to schedule the workload unless TPC-H data
    at this scale factor is staged. None means the workload generates its
    own data in setup_steps."""


@dataclasses.dataclass
class StepRunRecord:
    """One execution of one measured step. Multiple records per (workload,
    engine, step) when warm_runs > 1."""

    workload: str
    engine: str
    step_id: str
    run_idx: int
    cold: bool
    purge_verified: bool
    result: StepResult


@dataclasses.dataclass
class WorkloadResult:
    """Aggregated result for one workload run on one engine."""

    workload: str
    engine: str
    setup_ms_total: float
    measured_records: list[StepRunRecord]
    cleanup_ms_total: float


# ---------------------------------------------------------------------------
# Canonical result hashing
# ---------------------------------------------------------------------------
#
# Cross-engine correctness validation hinges on this function returning the
# same hex digest given two engines' results, IFF the result sets are
# identical as multisets of rows.
#
# The contract:
#   - Each row is rendered to a tab-separated UTF-8 string.
#   - Cell rendering: None -> "NULL"; floats -> repr() with full precision;
#     bytes -> hex; everything else -> str().
#   - Rows are sorted lexicographically as their rendered strings before
#     hashing. This makes the hash order-insensitive.
#   - SHA-256 over the joined rendered rows separated by "\n".
#
# If two engines disagree on rounding, NULL representation, or string
# whitespace, the hash will differ. That's a feature: silent disagreement
# is the worst kind of bug. Rounding tolerance, when needed, lives in the
# workload's own normalize step, not here.

def _render_cell(v) -> str:
    if v is None:
        return "NULL"
    if isinstance(v, float):
        return repr(v)
    if isinstance(v, bytes):
        return v.hex()
    return str(v)


def _render_row(row: Iterable) -> str:
    return "\t".join(_render_cell(c) for c in row)


def hash_result_rows(rows: Iterable[Iterable]) -> tuple[str, int]:
    """Return (sha256_hex, row_count). Sort-then-hash so order does not
    affect the digest."""
    rendered = sorted(_render_row(r) for r in rows)
    h = hashlib.sha256()
    for line in rendered:
        h.update(line.encode("utf-8"))
        h.update(b"\n")
    return h.hexdigest(), len(rendered)


def discover(workloads_dir: Path) -> dict[str, Workload]:
    """Import every `*.py` under workloads_dir except `__init__` and `spec`,
    and collect the `WORKLOAD` symbol each module exports.

    Used by bench_runner.py so adding a new workload is a single new file."""
    import importlib

    out: dict[str, Workload] = {}
    for path in sorted(workloads_dir.glob("*.py")):
        if path.stem in ("__init__", "spec"):
            continue
        mod_name = f"workloads.{path.stem}"
        mod = importlib.import_module(mod_name)
        wl = getattr(mod, "WORKLOAD", None)
        if not isinstance(wl, Workload):
            continue
        if wl.name in out:
            raise ValueError(f"duplicate workload name {wl.name!r} in {path}")
        out[wl.name] = wl
    return out
