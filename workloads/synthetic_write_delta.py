"""Write throughput against PLAIN Delta tables (synthetic source).

Measures how fast each engine can produce rows from an in-memory
deterministic generator and persist them as a plain Delta table. The
source is **purely synthetic** (`generate_series` on df, `range` on
DuckDB / Spark with arithmetic-derived columns), so no CSV or parquet
decoder gets in the headline. The measured timer is around the full
CTAS statement; the drop-if-exists prelude is in the same script and
its cost is folded into wall_ms (a few hundred ms vs several seconds
of actual write, all engines pay it).

DuckDB sits this benchmark out: its `delta` extension is read-only,
so DuckDB cannot write Delta tables. The bench's
`applicable_engines` declaration skips it automatically.

Schema (9 columns, 7 distinct types, all derived deterministically
from the row index `i` so every engine produces byte-equivalent
content modulo Delta-writer file layout):

    id            BIGINT          i
    customer_id   INT             cast((i*13) % 10000 as int)
    order_date    DATE            date '2024-01-01' + cast(i % 365 as int) days
    quantity      INT             cast((i % 100) + 1 as int)
    unit_price    DECIMAL(10, 2)  cast((i % 9999) / 100.0 as decimal(10,2))
    discount      DOUBLE          cast((i % 30) / 100.0 as double)
    region        STRING          5-value CASE expression
    is_priority   BOOLEAN         (i % 7) = 0
    notes         STRING          concat('order_', lpad(cast(i % 10000 as varchar), 6, '0'))

Row count: 10M at scale=1, 100M at scale=10. Tunable via the workload's
WORKLOAD constant.

Per-engine write paths (every engine writes to its own Delta directory
on the same ext4-backed bench data volume, so the filesystem cost is
identical across engines):

  df:
    1. Setup creates `write_zone TYPE DELTA STORAGE_ROOT='/workspace/data/synth_write/df'`
       so df's zone-clamp does not mangle the absolute location.
    2. Measured step runs:
         DROP DELTA TABLE IF EXISTS write_zone.bench.synth_fact WITH FILES;
         CREATE DELTA TABLE write_zone.bench.synth_fact
             LOCATION '/workspace/data/synth_write/df/synth_fact'
             AS SELECT <columns> FROM generate_series(0, N-1) AS t(i)

  Spark (default + tuned):
    No setup needed. Measured step runs:
         CREATE OR REPLACE TABLE delta.`/workspace/data/synth_write/spark_<flavor>/synth_fact`
             USING DELTA AS SELECT <columns> FROM range(0, N) AS t(i)
    Spark's CREATE OR REPLACE on a Delta path is atomic.
"""
from __future__ import annotations

from engines.base import STEP_SQL_DDL, STEP_SQL_DML, WorkloadStep
from .spec import Workload


# Row counts per scale factor. SF=1 is a smoke; SF=10 is the meaningful
# write number. Larger scales just multiply linearly.
_ROWS_PER_SCALE = {
    1:    10_000_000,
    10:   100_000_000,
    100:  1_000_000_000,
}

_DF_OUT     = "/workspace/data/synth_write/df/synth_fact"
_SD_OUT     = "/workspace/data/synth_write/spark_default/synth_fact"
_ST_OUT     = "/workspace/data/synth_write/spark_tuned/synth_fact"
_DF_ZONE_ROOT = "/workspace/data/synth_write/df"


def _row_count(scale: int) -> int:
    return _ROWS_PER_SCALE.get(scale, _ROWS_PER_SCALE[1] * scale)


def _column_exprs(varchar_type: str) -> list[tuple[str, str]]:
    """Return (column_name, sql_expression) pairs.

    Expressions are written in a subset that DataFusion, DuckDB, and Spark
    SQL all parse identically against an integer column `i`. The one
    dialect difference: Spark 4.0 in ANSI mode requires `STRING` (or
    `VARCHAR(n)` with a length) where df and DuckDB accept bare `VARCHAR`.
    The `varchar_type` parameter switches between them per engine."""
    return [
        ("id",          "i"),
        ("customer_id", "CAST(((i * 13) % 10000) AS INT)"),
        ("order_date",  "date_add(DATE '2024-01-01', CAST(i % 365 AS INT))"),
        ("quantity",    "CAST((i % 100) + 1 AS INT)"),
        ("unit_price",  "CAST(((i % 9999) / 100.0) AS DECIMAL(10, 2))"),
        ("discount",    "CAST(((i % 30) / 100.0) AS DOUBLE)"),
        ("region",
            "CASE "
            "WHEN (i % 5) = 0 THEN 'NORTH' "
            "WHEN (i % 5) = 1 THEN 'SOUTH' "
            "WHEN (i % 5) = 2 THEN 'EAST' "
            "WHEN (i % 5) = 3 THEN 'WEST' "
            "ELSE 'CENTRAL' END"),
        ("is_priority", "(i % 7) = 0"),
        ("notes",       f"concat('order_', lpad(CAST((i % 10000) AS {varchar_type}), 6, '0'))"),
    ]


def _select_clause(varchar_type: str) -> str:
    return ",\n    ".join(f"{expr} AS {name}" for name, expr in _column_exprs(varchar_type))


def _df_setup() -> str:
    """One-shot zone creation so df's STORAGE_ROOT clamp respects the bench's
    bind-mounted ext4 data dir instead of zone-relativizing the absolute path."""
    return (
        f"CREATE ZONE IF NOT EXISTS write_zone TYPE DELTA "
        f"STORAGE_ROOT '{_DF_ZONE_ROOT}'"
    )


def _df_cleanup() -> str:
    """Drop the table + files at the end of the engine's run so disk does
    not fill up across benches. The zone stays around for reuse."""
    return "DROP DELTA TABLE IF EXISTS write_zone.bench.synth_fact WITH FILES"


def _df_write_sql(n_rows: int) -> str:
    """df measured step: drop existing + CTAS in one CLI invocation. The
    df adapter splits the script and wall_ms covers both statements.
    Drop overhead is ~200 ms vs several seconds of CTAS, equivalent on
    every iteration."""
    return (
        "DROP DELTA TABLE IF EXISTS write_zone.bench.synth_fact WITH FILES;\n"
        f"CREATE DELTA TABLE write_zone.bench.synth_fact LOCATION '{_DF_OUT}' AS\n"
        f"SELECT\n    {_select_clause('VARCHAR')}\n"
        f"FROM generate_series(0, {n_rows - 1}) AS t(i)"
    )


def _spark_write_sql(n_rows: int, out_path: str) -> str:
    """Spark measured step: CREATE OR REPLACE TABLE on a Delta path is
    atomic; no separate drop needed. Spark 4.0 in ANSI mode requires
    `STRING` (or `VARCHAR(n)` with explicit length); bare `VARCHAR`
    raises DATATYPE_MISSING_SIZE."""
    return (
        f"CREATE OR REPLACE TABLE delta.`{out_path}` USING DELTA AS\n"
        f"SELECT\n    {_select_clause('STRING')}\n"
        f"FROM range(0, {n_rows}) AS t(i)"
    )


def _spark_default_cleanup() -> str:
    return f"DROP TABLE IF EXISTS delta.`{_SD_OUT}`"


def _spark_tuned_cleanup() -> str:
    return f"DROP TABLE IF EXISTS delta.`{_ST_OUT}`"


def _setup_steps() -> list[WorkloadStep]:
    return [
        WorkloadStep(
            id="prepare_write_target",
            kind=STEP_SQL_DDL,
            sql="SELECT 1",  # Spark default
            per_engine_sql={
                "df":             _df_setup(),
                "spark-default":  "SELECT 1",
                "spark-tuned":    "SELECT 1",
            },
            description="Per-engine write-target prep (df: zone create; Spark: no-op)",
            measured=False,
        )
    ]


def _cleanup_steps() -> list[WorkloadStep]:
    return [
        WorkloadStep(
            id="drop_synth_fact",
            kind=STEP_SQL_DDL,
            sql="SELECT 1",
            per_engine_sql={
                "df":             _df_cleanup(),
                "spark-default":  _spark_default_cleanup(),
                "spark-tuned":    _spark_tuned_cleanup(),
            },
            description="Drop the synth_fact Delta table (files + catalog)",
            measured=False,
        )
    ]


def _write_step(n_rows: int) -> WorkloadStep:
    return WorkloadStep(
        id=f"write_{n_rows // 1_000_000}m_rows",
        kind=STEP_SQL_DML,
        sql=_spark_write_sql(n_rows, _SD_OUT),
        per_engine_sql={
            "df":             _df_write_sql(n_rows),
            "spark-default":  _spark_write_sql(n_rows, _SD_OUT),
            "spark-tuned":    _spark_write_sql(n_rows, _ST_OUT),
        },
        description=f"CTAS {n_rows:,} rows of synth_fact into plain Delta",
        expects_rows=False,
    )


# Hardcoded to SF=1 for the v0.1 workload constant; runner can override
# via _ROWS_PER_SCALE keyed on --scale. The module exposes a single
# Workload pinned at SF=1 here; the bench runner passes scale through
# to compute the actual N at run-time would require a richer
# interface than the current `WORKLOAD` constant — for now the SF=1
# row count is the only published shape.
WORKLOAD = Workload(
    name="synthetic_write_delta",
    description="Write 10M deterministic synthetic rows into plain Delta (df + Spark; DuckDB read-only).",
    setup_steps=_setup_steps(),
    measured_steps=[_write_step(_row_count(1))],
    cleanup_steps=_cleanup_steps(),
    requires_data_at_scale=None,
    requires_input_data=False,
    applicable_engines=("df", "spark-default", "spark-tuned"),
    # Borrow tpch_sf{scale} for the data_subdir path; the pre-flight check
    # is skipped via requires_input_data=False so this is purely cosmetic.
    data_subdir="synth_write_sf{scale}",
)
