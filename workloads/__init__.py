"""Workload definitions.

Each workload is a Python module exposing a `WORKLOAD: Workload` symbol.
The runner discovers them by name (e.g. `tpch_read`, `bulk_load`,
`merge_cdc`) and drives the same engine adapter through every step.

A Workload is structured as:
    - setup_steps: idempotent prep (CREATE TABLE, COPY INTO from Parquet).
      Timed but not part of the headline cell. Re-run when the engine
      restarts.
    - measured_steps: the core. 1 cold + 9 warm runs by default.
    - cleanup_steps: tear down (DROP TABLE) so the next workload starts
      from a known state. Not measured.
"""
from .spec import Workload, WorkloadResult, hash_result_rows

__all__ = ["Workload", "WorkloadResult", "hash_result_rows"]
