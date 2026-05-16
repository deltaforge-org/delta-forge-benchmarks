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
    """Zone + schema creation (idempotent)."""
    return (
        f"CREATE ZONE IF NOT EXISTS {_DF_ZONE} "
        f"STORAGE_ROOT = '{_DF_ZONE_ROOT}' "
        f"COMMENT 'Bench-managed zone';\n"
        f"CREATE SCHEMA IF NOT EXISTS {_DF_SCHEMA}"
    )


def _df_load_sql(table: str) -> str:
    schema = _TPCH_SCHEMAS[table]
    dst = f"{_DF_SCHEMA}.{table}"
    loc = f"{_DF_DELTA_LOC}/{table}"
    # read_parquet path: file:///workspace/data/tpch_sf1/<table>.parquet
    # {data_dir} is substituted by bench_runner to /workspace/data/tpch_sf{scale}
    return (
        f"{_df_zone_init()};\n"
        f"DROP DELTA TABLE IF EXISTS {dst} WITH FILES;\n"
        f"CREATE DELTA TABLE {dst} {schema} LOCATION '{loc}';\n"
        f"INSERT INTO {dst} SELECT * FROM read_parquet('file://{{data_dir}}/{table}.parquet')"
    )


def _df_drop_sql(table: str) -> str:
    return f"DROP DELTA TABLE IF EXISTS {_DF_SCHEMA}.{table} WITH FILES"


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
                per_engine_sql={"df": _df_load_sql(table)},
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
            per_engine_sql={"df": _df_drop_sql(table)},
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
            per_engine_sql={"df": _df_load_sql(table)},
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
            per_engine_sql={"df": _df_drop_sql(table)},
            description=f"Drop {table}",
            measured=False,
        )
    ]
