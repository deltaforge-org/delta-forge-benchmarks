"""Shared workload helpers: TPC-H Delta table load/drop steps.

The benchmark runs exclusively inside the Docker container where:
  - Bench repo lives at /workspace (WORKDIR)
  - Generated TPC-H parquet lands at /workspace/data/tpch_sf{scale}/
  - Delta tables are written to /workspace/bench_delta/tpch_sf{scale}/

DF load pattern (explicit schema + INSERT — avoids DECIMAL→Utf8 coercion):
  1. CREATE ZONE bench STORAGE_ROOT='/workspace/bench_delta'
  2. CREATE SCHEMA bench.tpch
  3. CREATE DELTA TABLE bench.tpch.<t> (<typed schema>) LOCATION 'tpch_sf{scale}/<t>'
  4. INSERT INTO bench.tpch.<t> SELECT * FROM read_parquet('file:///workspace/data/tpch_sf{scale}/<t>.parquet')

Spark+Delta pattern (single-statement CTAS — Spark infers types from parquet):
  CREATE OR REPLACE TABLE <t> USING DELTA AS
  SELECT * FROM parquet.`{data_dir}/{t}.parquet`
"""
from __future__ import annotations

from engines.base import STEP_SQL_DDL, WorkloadStep

TPCH_LOAD_ORDER = [
    "region", "nation", "supplier", "customer",
    "part", "partsupp", "orders", "lineitem",
]

_DF_ZONE = "bench"
_DF_SCHEMA = "bench.tpch"
_DF_DELTA_LOC = "tpch_sf{scale}"  # zone-relative; bench_runner substitutes {scale}

# Zone storage root is fixed to /workspace/bench_delta — always inside the container.
# No env-var override: local paths are workstation-specific and break portability.
_DF_ZONE_ROOT = "/workspace/bench_delta"

# External zone for staging raw parquet. DeltaForge does not expose a
# read_parquet() table function; the canonical pattern is to wrap source
# files in a TYPE EXTERNAL zone, then INSERT...SELECT into the managed Delta
# table. See demos/parquet/flight-delays/setup.sql.
#
# resolve_location_for_zone (delta-forge-sql command/ddl.rs) clamps every
# LOCATION to the zone's storage_root. To keep external table paths pointing
# at the on-disk parquet, anchor the external zone at /workspace/data and
# pass a zone-relative LOCATION (e.g. tpch_sf1/region.parquet).
_DF_EXT_ZONE = "bench_ext"
_DF_EXT_SCHEMA = "bench_ext.tpch_stage"
_DF_EXT_ZONE_ROOT = "/workspace/data"

# Read-only external zone for the pure-reader bench (tpch_read). External
# tables here point at the source parquet so every engine reads the same
# bytes; no data movement happens at setup time. Distinct from the staging
# schema used by bulk_load's INSERT path so the two workloads don't collide.
_DF_READ_SCHEMA = "bench_ext.tpch_read"

# ---------------------------------------------------------------------------
# Explicit TPC-H column definitions (types verified against DuckDB's output)
# ---------------------------------------------------------------------------

_TPCH_SCHEMAS: dict[str, str] = {
    "region": """(
    r_regionkey  INT        NOT NULL,
    r_name       STRING     NOT NULL,
    r_comment    STRING
)""",
    "nation": """(
    n_nationkey  INT        NOT NULL,
    n_name       STRING     NOT NULL,
    n_regionkey  INT        NOT NULL,
    n_comment    STRING
)""",
    "supplier": """(
    s_suppkey    BIGINT     NOT NULL,
    s_name       STRING     NOT NULL,
    s_address    STRING     NOT NULL,
    s_nationkey  INT        NOT NULL,
    s_phone      STRING     NOT NULL,
    s_acctbal    DECIMAL(15,2) NOT NULL,
    s_comment    STRING
)""",
    "customer": """(
    c_custkey    BIGINT     NOT NULL,
    c_name       STRING     NOT NULL,
    c_address    STRING     NOT NULL,
    c_nationkey  INT        NOT NULL,
    c_phone      STRING     NOT NULL,
    c_acctbal    DECIMAL(15,2) NOT NULL,
    c_mktsegment STRING     NOT NULL,
    c_comment    STRING
)""",
    "part": """(
    p_partkey    BIGINT     NOT NULL,
    p_name       STRING     NOT NULL,
    p_mfgr       STRING     NOT NULL,
    p_brand      STRING     NOT NULL,
    p_type       STRING     NOT NULL,
    p_size       INT        NOT NULL,
    p_container  STRING     NOT NULL,
    p_retailprice DECIMAL(15,2) NOT NULL,
    p_comment    STRING
)""",
    "partsupp": """(
    ps_partkey   BIGINT     NOT NULL,
    ps_suppkey   BIGINT     NOT NULL,
    ps_availqty  BIGINT     NOT NULL,
    ps_supplycost DECIMAL(15,2) NOT NULL,
    ps_comment   STRING
)""",
    "orders": """(
    o_orderkey    BIGINT     NOT NULL,
    o_custkey     BIGINT     NOT NULL,
    o_orderstatus STRING     NOT NULL,
    o_totalprice  DECIMAL(15,2) NOT NULL,
    o_orderdate   DATE       NOT NULL,
    o_orderpriority STRING   NOT NULL,
    o_clerk       STRING     NOT NULL,
    o_shippriority INT       NOT NULL,
    o_comment     STRING
)""",
    "lineitem": """(
    l_orderkey     BIGINT       NOT NULL,
    l_partkey      BIGINT       NOT NULL,
    l_suppkey      BIGINT       NOT NULL,
    l_linenumber   BIGINT       NOT NULL,
    l_quantity     DECIMAL(15,2) NOT NULL,
    l_extendedprice DECIMAL(15,2) NOT NULL,
    l_discount     DECIMAL(15,2) NOT NULL,
    l_tax          DECIMAL(15,2) NOT NULL,
    l_returnflag   STRING       NOT NULL,
    l_linestatus   STRING       NOT NULL,
    l_shipdate     DATE         NOT NULL,
    l_commitdate   DATE         NOT NULL,
    l_receiptdate  DATE         NOT NULL,
    l_shipinstruct STRING       NOT NULL,
    l_shipmode     STRING       NOT NULL,
    l_comment      STRING
)""",
}


def _df_zone_init() -> str:
    """Zone + schema creation (idempotent). Sets up both the managed zone
    that holds the Delta tables and the external zone used to stage the
    raw TPC-H parquet files for INSERT...SELECT."""
    return (
        f"CREATE ZONE IF NOT EXISTS {_DF_ZONE} "
        f"STORAGE_ROOT = '{_DF_ZONE_ROOT}' "
        f"COMMENT 'Bench-managed zone';\n"
        f"CREATE SCHEMA IF NOT EXISTS {_DF_SCHEMA};\n"
        f"CREATE ZONE IF NOT EXISTS {_DF_EXT_ZONE} TYPE EXTERNAL "
        f"STORAGE_ROOT = '{_DF_EXT_ZONE_ROOT}' "
        f"COMMENT 'Bench external zone (raw parquet sources)';\n"
        f"CREATE SCHEMA IF NOT EXISTS {_DF_EXT_SCHEMA}"
    )


def _df_load_sql(table: str) -> str:
    schema = _TPCH_SCHEMAS[table]
    dst = f"{_DF_SCHEMA}.{table}"
    loc = f"{_DF_DELTA_LOC}/{table}"
    stg = f"{_DF_EXT_SCHEMA}.stg_{table}"
    # {data_dir} is substituted by bench_runner to /workspace/data/tpch_sf{scale}.
    return (
        f"{_df_zone_init()};\n"
        f"DROP DELTA TABLE IF EXISTS {dst} WITH FILES;\n"
        f"CREATE DELTA TABLE {dst} {schema} LOCATION '{loc}';\n"
        f"DROP EXTERNAL TABLE IF EXISTS {stg};\n"
        f"CREATE EXTERNAL TABLE {stg} "
        f"USING PARQUET LOCATION 'tpch_sf{{scale}}/{table}.parquet';\n"
        f"INSERT INTO {dst} SELECT * FROM {stg};\n"
        f"DROP EXTERNAL TABLE {stg}"
    )


def _df_drop_sql(table: str) -> str:
    return f"DROP DELTA TABLE IF EXISTS {_DF_SCHEMA}.{table} WITH FILES"


def _duckdb_load_sql(table: str) -> str:
    # DuckDB queries reference unqualified TPC-H table names, matching the
    # text of workloads/tpch/queries/*.sql. CREATE OR REPLACE drops any state
    # from a previous run so the engine starts each setup phase clean.
    return (
        f"CREATE OR REPLACE TABLE {table} AS "
        f"SELECT * FROM read_parquet('{{data_dir}}/{table}.parquet')"
    )


def _duckdb_drop_sql(table: str) -> str:
    return f"DROP TABLE IF EXISTS {table}"


# ---------------------------------------------------------------------------
# Read-only views over source parquet. Used by tpch_read so all three engines
# scan the same on-disk parquet files. No data movement at setup time; the
# measured queries reflect only reader + planner + executor work.
# ---------------------------------------------------------------------------

def _df_read_init() -> str:
    """External zone + schema for the pure-reader bench."""
    return (
        f"CREATE ZONE IF NOT EXISTS {_DF_EXT_ZONE} TYPE EXTERNAL "
        f"STORAGE_ROOT = '{_DF_EXT_ZONE_ROOT}' "
        f"COMMENT 'Bench external zone (raw parquet sources)';\n"
        f"CREATE SCHEMA IF NOT EXISTS {_DF_READ_SCHEMA}"
    )


def _df_view_sql(table: str) -> str:
    dst = f"{_DF_READ_SCHEMA}.{table}"
    return (
        f"{_df_read_init()};\n"
        f"DROP EXTERNAL TABLE IF EXISTS {dst};\n"
        f"CREATE EXTERNAL TABLE {dst} "
        f"USING PARQUET LOCATION 'tpch_sf{{scale}}/{table}.parquet'"
    )


def _df_view_drop_sql(table: str) -> str:
    return f"DROP EXTERNAL TABLE IF EXISTS {_DF_READ_SCHEMA}.{table}"


def _duckdb_view_sql(table: str) -> str:
    # View, not table: no data copy. read_parquet runs each query.
    return (
        f"CREATE OR REPLACE VIEW {table} AS "
        f"SELECT * FROM read_parquet('{{data_dir}}/{table}.parquet')"
    )


def _duckdb_view_drop_sql(table: str) -> str:
    return f"DROP VIEW IF EXISTS {table}"


def _spark_view_sql(table: str) -> str:
    # Temporary view over the parquet datasource. No table materialization;
    # each query scans the parquet files directly.
    return (
        f"CREATE OR REPLACE TEMPORARY VIEW {table} "
        f"USING parquet OPTIONS (path '{{data_dir}}/{table}.parquet')"
    )


def _spark_view_drop_sql(table: str) -> str:
    return f"DROP VIEW IF EXISTS {table}"


def make_parquet_view_steps(measured: bool = False) -> list[WorkloadStep]:
    """Per-engine setup that mounts the source parquet as a view / external
    table. No data is copied. The measured queries then read the same on-disk
    parquet from all engines."""
    steps: list[WorkloadStep] = []
    for table in TPCH_LOAD_ORDER:
        # Spark default text — every engine has its own variant via per_engine_sql.
        default_sql = _spark_view_sql(table)
        steps.append(
            WorkloadStep(
                id=f"view_{table}",
                kind=STEP_SQL_DDL,
                sql=default_sql,
                per_engine_sql={
                    "df": _df_view_sql(table),
                    "duckdb": _duckdb_view_sql(table),
                    "spark-default": _spark_view_sql(table),
                    "spark-tuned": _spark_view_sql(table),
                },
                description=f"Mount {table}.parquet as view / external table",
                measured=measured,
            )
        )
    return steps


# ---------------------------------------------------------------------------
# Write workload: CSV -> Delta (df, Spark) or CSV -> parquet (DuckDB).
#
# All three engines write parquet on disk from the same CSV input. df and
# Spark wrap their parquet in a Delta log; DuckDB writes the parquet only
# (no Delta writer in duckdb 1.x). The log overhead is milliseconds on a
# multi-million-row write, so the comparison is fair on the dominant work.
# ---------------------------------------------------------------------------

# Managed zone reserved for the write bench. Kept separate from `bench` so
# tpch_read's read-only externals never collide with this workload's
# destination tables.
_DF_WRITE_ZONE = "bench_w"
_DF_WRITE_SCHEMA = "bench_w.tpch_w"
_DF_WRITE_ZONE_ROOT = "/workspace/bench_delta_w"

# External schema where the source CSV is mounted (read-only).
_DF_CSV_SRC_SCHEMA = "bench_ext.csv_input"

# Per-engine target dirs for the write artefacts. Each engine writes into
# its own subtree so successive iterations of cold/warm runs can drop and
# rewrite without cross-engine interference.
_DUCKDB_WRITE_DIR = "/workspace/duckdb_write_out"
_SPARK_WRITE_DIR = "/workspace/spark_write_out"

# Lineitem-shape CSV input. The gen_csv_input.py script materializes
# /workspace/data/csv_input/<table>.csv from the existing TPC-H parquet.
_CSV_INPUT_PATH = "/workspace/data/csv_input/{table}.csv"


def _df_csv_init() -> str:
    """One-shot zone + schema bootstrap for the write workload."""
    return (
        f"CREATE ZONE IF NOT EXISTS {_DF_EXT_ZONE} TYPE EXTERNAL "
        f"STORAGE_ROOT = '/workspace/data' "
        f"COMMENT 'Bench external zone';\n"
        f"CREATE SCHEMA IF NOT EXISTS {_DF_CSV_SRC_SCHEMA};\n"
        f"CREATE ZONE IF NOT EXISTS {_DF_WRITE_ZONE} TYPE DELTA "
        f"STORAGE_ROOT = '{_DF_WRITE_ZONE_ROOT}' "
        f"COMMENT 'Bench write-target zone';\n"
        f"CREATE SCHEMA IF NOT EXISTS {_DF_WRITE_SCHEMA}"
    )


def _df_parquet_ctas_sql(table: str) -> str:
    """CTAS-from-parquet: drop prior target, then one CREATE DELTA TABLE AS
    SELECT pulling typed rows from the source parquet through an external
    table mount. Source is already-decoded parquet so the measurement
    isolates write throughput (scan + encode + flush + commit), not CSV
    parser performance."""
    src = f"{_DF_CSV_SRC_SCHEMA}.{table}_pq"
    dst = f"{_DF_WRITE_SCHEMA}.{table}"
    return (
        f"{_df_csv_init()};\n"
        f"DROP EXTERNAL TABLE IF EXISTS {src};\n"
        f"CREATE EXTERNAL TABLE {src} "
        f"USING PARQUET LOCATION 'tpch_sf1/{table}.parquet';\n"
        f"DROP DELTA TABLE IF EXISTS {dst} WITH FILES;\n"
        f"CREATE DELTA TABLE {dst} LOCATION '{table}' "
        f"AS SELECT * FROM {src}"
    )


def _df_parquet_ctas_drop_sql(table: str) -> str:
    src = f"{_DF_CSV_SRC_SCHEMA}.{table}_pq"
    dst = f"{_DF_WRITE_SCHEMA}.{table}"
    return (
        f"DROP DELTA TABLE IF EXISTS {dst} WITH FILES;\n"
        f"DROP EXTERNAL TABLE IF EXISTS {src}"
    )


def _spark_parquet_ctas_sql(table: str) -> str:
    """Spark CREATE OR REPLACE TABLE ... USING DELTA AS SELECT from typed
    parquet. Single statement; OR REPLACE handles re-runs idempotently."""
    path = _SPARK_WRITE_DIR + f"/{table}"
    pq_path = f"/workspace/data/tpch_sf1/{table}.parquet"
    return (
        f"CREATE OR REPLACE TABLE {table} USING DELTA LOCATION '{path}' "
        f"AS SELECT * FROM parquet.`{pq_path}`"
    )


def _spark_parquet_ctas_drop_sql(table: str) -> str:
    return f"DROP TABLE IF EXISTS {table}"


def _duckdb_parquet_ctas_sql(table: str) -> str:
    """DuckDB has no Delta writer in 1.x, so the disk-CTAS equivalent is
    COPY ... TO ... (FORMAT PARQUET). Source = same typed parquet the
    other engines read. Output bytes are comparable; df and Spark
    additionally write a small JSON commit log."""
    pq_path = f"/workspace/data/tpch_sf1/{table}.parquet"
    parquet_path = f"{_DUCKDB_WRITE_DIR}/{table}.parquet"
    return (
        f"COPY (SELECT * FROM read_parquet('{pq_path}')) "
        f"TO '{parquet_path}' (FORMAT PARQUET, OVERWRITE_OR_IGNORE)"
    )


def _duckdb_parquet_ctas_drop_sql(table: str) -> str:
    return "SELECT 1"


def make_csv_to_delta_steps(tables: list[str] | None = None,
                            measured: bool = True) -> list[WorkloadStep]:
    """Per-engine 'load CSV into a lakehouse table' write benchmark.
    Defaults to just `lineitem` (6M rows at SF=1, the dominant write
    signal); pass `tables` to widen the suite."""
    if tables is None:
        tables = ["lineitem"]
    steps: list[WorkloadStep] = []
    for table in tables:
        steps.append(
            WorkloadStep(
                id=f"write_{table}",
                kind=STEP_SQL_DDL,
                sql=_spark_parquet_ctas_sql(table),  # Spark default
                per_engine_sql={
                    "df": _df_parquet_ctas_sql(table),
                    "duckdb": _duckdb_parquet_ctas_sql(table),
                    "spark-default": _spark_parquet_ctas_sql(table),
                    "spark-tuned": _spark_parquet_ctas_sql(table),
                },
                description=f"CTAS from parquet: {table}",
                measured=measured,
            )
        )
    return steps


def make_csv_to_delta_drop_steps(tables: list[str] | None = None) -> list[WorkloadStep]:
    if tables is None:
        tables = ["lineitem"]
    return [
        WorkloadStep(
            id=f"drop_write_{table}",
            kind=STEP_SQL_DDL,
            sql=_spark_parquet_ctas_drop_sql(table),
            per_engine_sql={
                "df": _df_parquet_ctas_drop_sql(table),
                "duckdb": _duckdb_parquet_ctas_drop_sql(table),
                "spark-default": _spark_parquet_ctas_drop_sql(table),
                "spark-tuned": _spark_parquet_ctas_drop_sql(table),
            },
            description=f"Drop write artefacts: {table}",
            measured=False,
        )
        for table in reversed(tables)
    ]


def make_parquet_view_drop_steps() -> list[WorkloadStep]:
    return [
        WorkloadStep(
            id=f"unmount_{table}",
            kind=STEP_SQL_DDL,
            sql=_spark_view_drop_sql(table),
            per_engine_sql={
                "df": _df_view_drop_sql(table),
                "duckdb": _duckdb_view_drop_sql(table),
                "spark-default": _spark_view_drop_sql(table),
                "spark-tuned": _spark_view_drop_sql(table),
            },
            description=f"Drop {table} view",
            measured=False,
        )
        for table in reversed(TPCH_LOAD_ORDER)
    ]


def make_delta_load_steps(measured: bool = False) -> list[WorkloadStep]:
    """CREATE 8 TPC-H Delta tables from staged Parquet."""
    steps: list[WorkloadStep] = []
    for table in TPCH_LOAD_ORDER:
        spark_sql = (
            f"CREATE OR REPLACE TABLE {table} USING DELTA "
            f"AS SELECT * FROM parquet.`{{data_dir}}/{table}.parquet`"
        )
        steps.append(
            WorkloadStep(
                id=f"load_{table}",
                kind=STEP_SQL_DDL,
                sql=spark_sql,
                per_engine_sql={
                    "df": _df_load_sql(table),
                    "duckdb": _duckdb_load_sql(table),
                },
                description=f"Load {table} from Parquet -> Delta (typed)",
                measured=measured,
            )
        )
    return steps


def make_drop_steps() -> list[WorkloadStep]:
    return [
        WorkloadStep(
            id=f"drop_{table}",
            kind=STEP_SQL_DDL,
            sql=f"DROP TABLE IF EXISTS {table}",
            per_engine_sql={
                "df": _df_drop_sql(table),
                "duckdb": _duckdb_drop_sql(table),
            },
            description=f"Drop {table}",
            measured=False,
        )
        for table in reversed(TPCH_LOAD_ORDER)
    ]


def make_single_table_load_steps(table: str, measured: bool = False) -> list[WorkloadStep]:
    spark_sql = (
        f"CREATE OR REPLACE TABLE {table} USING DELTA "
        f"AS SELECT * FROM parquet.`{{data_dir}}/{table}.parquet`"
    )
    return [
        WorkloadStep(
            id=f"load_{table}",
            kind=STEP_SQL_DDL,
            sql=spark_sql,
            per_engine_sql={
                "df": _df_load_sql(table),
                "duckdb": _duckdb_load_sql(table),
            },
            description=f"Load {table} from Parquet -> Delta (typed)",
            measured=measured,
        )
    ]


def make_single_table_drop_steps(table: str) -> list[WorkloadStep]:
    return [
        WorkloadStep(
            id=f"drop_{table}",
            kind=STEP_SQL_DDL,
            sql=f"DROP TABLE IF EXISTS {table}",
            per_engine_sql={
                "df": _df_drop_sql(table),
                "duckdb": _duckdb_drop_sql(table),
            },
            description=f"Drop {table}",
            measured=False,
        )
    ]
