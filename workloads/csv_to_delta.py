"""Write benchmark: CSV input -> Delta (df, Spark) or parquet (DuckDB).

Source: /workspace/data/csv_input/<table>.csv generated once by
        data_gen/gen_csv_input.py from the TPC-H parquet. All three engines
        read the same CSV bytes.

Measured: one full drop-and-rewrite per iteration so cold/warm semantics
          stay clean (no append confusion across runs).

Target:
  - df:      Delta table at /workspace/bench_delta_w/<table>
  - Spark:   Delta table at /workspace/spark_write_out/<table>
  - DuckDB:  parquet file  at /workspace/duckdb_write_out/<table>.parquet

DuckDB writes raw parquet because the duckdb-delta extension in 1.x is
read-only. The Delta log overhead (a few KB of JSON per commit) is
milliseconds against multi-million-row parquet writes, so the comparison
remains apples-to-apples on the dominant work (parquet encode + I/O).
"""
from __future__ import annotations

from ._fixtures import make_csv_to_delta_drop_steps, make_csv_to_delta_steps
from .spec import Workload

# Default to lineitem only — the dominant write signal at any TPC-H scale.
# To widen, instantiate with a custom `tables` list.
_TABLES = ["lineitem"]


WORKLOAD = Workload(
    name="csv_to_delta",
    description="CSV -> Delta (df, Spark) / parquet (DuckDB) write benchmark.",
    setup_steps=make_csv_to_delta_drop_steps(_TABLES),
    measured_steps=make_csv_to_delta_steps(_TABLES, measured=True),
    cleanup_steps=make_csv_to_delta_drop_steps(_TABLES),
)
