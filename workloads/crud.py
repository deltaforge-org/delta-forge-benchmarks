"""CRUD workload: INSERT / UPDATE / DELETE / MERGE against orders.

Target table: `orders` (1.5M rows at SF=1, 15M at SF=10).
All operations use deterministic hash-based row selection so results
are reproducible and cross-engine correct counts can be asserted.

Operations measured (each is one WorkloadStep):

  insert_batch   -- INSERT ~50K new rows (1/30 sampling of existing rows
                    re-keyed to avoid PK collisions via +MAX offset).
  update_1pct    -- UPDATE o_orderstatus = 'X' on 1% of rows.
  delete_1pct    -- DELETE 1% of rows (different hash bucket from UPDATE).
  merge_cdc      -- MERGE applying 1% UPDATEs + ~3K INSERTs in one statement.

Each step runs cold=1, warm=2 (3 total) to show steady-state cost after
the first Delta commit warms the metadata cache.

Engine portability
------------------
Spark+Delta accepts standard SQL MERGE INTO / UPDATE / DELETE against Delta
tables. DeltaForge does too. The only dialect difference is the table name:
  Spark: unqualified `orders`  (lives in the session's default database)
  DF:    `bench.tpch.orders`

Every step that touches `orders` carries `per_engine_sql` with the DF-qualified
name. The setup / cleanup steps use the shared helpers from _fixtures.py which
already carry DF variants.
"""
from __future__ import annotations

from engines.base import STEP_SQL_DDL, STEP_SQL_DML, WorkloadStep

from ._fixtures import (
    _DF_DELTA_LOC,
    _DF_SCHEMA,
    make_single_table_drop_steps,
    make_single_table_load_steps,
)
from .spec import Workload

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_SPARK_T = "orders"
_DF_T = f"{_DF_SCHEMA}.orders"


def _spark(sql: str) -> str:
    return sql.replace("__T__", _SPARK_T)


def _df(sql: str) -> str:
    return sql.replace("__T__", _DF_T)


def _step(step_id: str, kind: str, sql_template: str, description: str,
          measured: bool = True) -> WorkloadStep:
    """Build a step using __T__ as the table placeholder."""
    return WorkloadStep(
        id=step_id,
        kind=kind,
        sql=_spark(sql_template),
        per_engine_sql={"df": _df(sql_template)},
        description=description,
        measured=measured,
    )


# ---------------------------------------------------------------------------
# Setup: build the `cdc_feed` staging table used by MERGE.
# Spark SQL: CREATE OR REPLACE TABLE cdc_feed USING DELTA AS ...
# DF SQL:    DROP + CREATE DELTA TABLE pbi.bench_tpch.cdc_feed LOCATION ... AS ...
# ---------------------------------------------------------------------------

_SPARK_CDC_FEED = """
CREATE OR REPLACE TABLE cdc_feed USING DELTA AS
SELECT
    o_orderkey + (SELECT MAX(o_orderkey) + 100000 FROM orders) AS o_orderkey,
    o_custkey,
    'F' AS o_orderstatus,
    o_totalprice * 1.05 AS o_totalprice,
    o_orderdate,
    o_orderpriority,
    o_clerk,
    o_shippriority,
    CONCAT('new:', o_comment) AS o_comment,
    'I' AS _op
FROM orders
WHERE MOD(ABS(HASH(o_orderkey)), 500) = 0
UNION ALL
SELECT
    o_orderkey,
    o_custkey,
    'X' AS o_orderstatus,
    o_totalprice,
    o_orderdate,
    o_orderpriority,
    o_clerk,
    o_shippriority,
    o_comment,
    'U' AS _op
FROM orders
WHERE MOD(ABS(HASH(o_orderkey)), 100) = 7
""".strip()

_DF_CDC_LOCATION = f"tpch_sf{{scale}}/cdc_feed"  # zone-relative; {scale} substituted by runner

_DF_CDC_FEED = f"""
CREATE SCHEMA IF NOT EXISTS {_DF_SCHEMA};
DROP DELTA TABLE IF EXISTS {_DF_SCHEMA}.cdc_feed WITH FILES;
CREATE DELTA TABLE {_DF_SCHEMA}.cdc_feed
LOCATION '{_DF_CDC_LOCATION}'
AS
SELECT
    o_orderkey + (SELECT MAX(o_orderkey) + 100000 FROM {_DF_T}) AS o_orderkey,
    o_custkey,
    'F' AS o_orderstatus,
    o_totalprice * 1.05 AS o_totalprice,
    o_orderdate,
    o_orderpriority,
    o_clerk,
    o_shippriority,
    CONCAT('new:', o_comment) AS o_comment,
    'I' AS _op
FROM {_DF_T}
WHERE MOD(ABS(HASH(o_orderkey)), 500) = 0
UNION ALL
SELECT
    o_orderkey,
    o_custkey,
    'X' AS o_orderstatus,
    o_totalprice,
    o_orderdate,
    o_orderpriority,
    o_clerk,
    o_shippriority,
    o_comment,
    'U' AS _op
FROM {_DF_T}
WHERE MOD(ABS(HASH(o_orderkey)), 100) = 7
""".strip()

# ---------------------------------------------------------------------------
# INSERT: ~50K new rows (1/30 of orders, re-keyed to avoid PK collision)
# ---------------------------------------------------------------------------

_INSERT_SQL = """
INSERT INTO __T__
SELECT
    o_orderkey + (SELECT MAX(o_orderkey) + 200000 FROM __T__) AS o_orderkey,
    o_custkey,
    'N' AS o_orderstatus,
    o_totalprice,
    o_orderdate,
    o_orderpriority,
    o_clerk,
    o_shippriority,
    o_comment
FROM __T__
WHERE MOD(ABS(HASH(o_orderkey)), 30) = 0
""".strip()

# ---------------------------------------------------------------------------
# UPDATE 1%: flip o_orderstatus to 'P' for ~15K rows
# ---------------------------------------------------------------------------

_UPDATE_SQL = (
    "UPDATE __T__ "
    "SET o_orderstatus = 'P' "
    "WHERE MOD(ABS(HASH(o_orderkey)), 100) = 3"
)

# ---------------------------------------------------------------------------
# DELETE 1%: remove ~15K rows (different hash bucket from UPDATE)
# ---------------------------------------------------------------------------

_DELETE_SQL = (
    "DELETE FROM __T__ "
    "WHERE MOD(ABS(HASH(o_orderkey)), 100) = 17"
)

# ---------------------------------------------------------------------------
# MERGE: apply the cdc_feed (inserts + updates)
# ---------------------------------------------------------------------------

_SPARK_MERGE_SQL = """
MERGE INTO orders AS t
USING cdc_feed AS s
    ON t.o_orderkey = s.o_orderkey
WHEN MATCHED AND s._op = 'U'
    THEN UPDATE SET
        t.o_orderstatus = s.o_orderstatus,
        t.o_totalprice  = s.o_totalprice
WHEN NOT MATCHED AND s._op = 'I'
    THEN INSERT (o_orderkey, o_custkey, o_orderstatus, o_totalprice,
                 o_orderdate, o_orderpriority, o_clerk,
                 o_shippriority, o_comment)
         VALUES (s.o_orderkey, s.o_custkey, s.o_orderstatus, s.o_totalprice,
                 s.o_orderdate, s.o_orderpriority, s.o_clerk,
                 s.o_shippriority, s.o_comment)
""".strip()

_DF_MERGE_SQL = _SPARK_MERGE_SQL.replace("INTO orders", f"INTO {_DF_T}").replace(
    "USING cdc_feed", f"USING {_DF_SCHEMA}.cdc_feed"
)

# ---------------------------------------------------------------------------
# Workload assembly
# ---------------------------------------------------------------------------

WORKLOAD = Workload(
    name="crud",
    description=(
        "INSERT / UPDATE / DELETE / MERGE against orders (~1.5M rows at SF=1). "
        "Each operation is measured independently. Models the incremental-ingest "
        "CDC loop that is the dominant Delta Lake write pattern in production."
    ),
    setup_steps=[
        *make_single_table_load_steps("orders", measured=False),
        WorkloadStep(
            id="prepare_cdc_feed",
            kind=STEP_SQL_DDL,
            sql=_SPARK_CDC_FEED,
            per_engine_sql={"df": _DF_CDC_FEED},
            description="Build deterministic INSERT+UPDATE CDC feed from orders",
            measured=False,
        ),
    ],
    measured_steps=[
        WorkloadStep(
            id="insert_batch",
            kind=STEP_SQL_DML,
            sql=_spark(_INSERT_SQL),
            per_engine_sql={"df": _df(_INSERT_SQL)},
            description="INSERT ~50K new rows (1/30 sample, re-keyed)",
        ),
        WorkloadStep(
            id="update_1pct",
            kind=STEP_SQL_DML,
            sql=_spark(_UPDATE_SQL),
            per_engine_sql={"df": _df(_UPDATE_SQL)},
            description="UPDATE ~1% of orders (o_orderstatus = 'P')",
        ),
        WorkloadStep(
            id="delete_1pct",
            kind=STEP_SQL_DML,
            sql=_spark(_DELETE_SQL),
            per_engine_sql={"df": _df(_DELETE_SQL)},
            description="DELETE ~1% of orders (different hash bucket from UPDATE)",
        ),
        WorkloadStep(
            id="merge_cdc",
            kind=STEP_SQL_DML,
            sql=_SPARK_MERGE_SQL,
            per_engine_sql={"df": _DF_MERGE_SQL},
            description="MERGE applying 1% UPDATEs + ~3K INSERTs from cdc_feed",
        ),
    ],
    cleanup_steps=[
        WorkloadStep(
            id="drop_cdc_feed",
            kind=STEP_SQL_DDL,
            sql="DROP TABLE IF EXISTS cdc_feed",
            per_engine_sql={"df": f"DROP DELTA TABLE IF EXISTS {_DF_SCHEMA}.cdc_feed WITH FILES"},
            description="Drop CDC staging table",
            measured=False,
        ),

        *make_single_table_drop_steps("orders"),
    ],
    cold_runs=1,
    warm_runs=2,
)
