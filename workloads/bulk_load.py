"""Bulk-load workload: Parquet -> Delta for all 8 TPC-H tables.

This is the "first time you ingest" path. Real users hit it on day one.
We measure each table's load separately so the report can show which
tables dominate (lineitem usually does, by an order of magnitude).

No setup or cleanup outside the measured loads + drops, because the loads
ARE the measurement. Successive runs CREATE OR REPLACE, which is what a
real "re-ingest from scratch" does.
"""
from __future__ import annotations

from ._fixtures import make_delta_load_steps, make_drop_steps
from .spec import Workload

WORKLOAD = Workload(
    name="bulk_load",
    description="Parquet -> Delta load for the 8 TPC-H tables.",
    setup_steps=make_drop_steps(),  # idempotent reset
    measured_steps=make_delta_load_steps(measured=True),
    cleanup_steps=make_drop_steps(),
)
