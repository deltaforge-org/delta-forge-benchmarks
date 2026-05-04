#!/usr/bin/env python3
"""Deterministic graph-finance data generator.

Produces a global-banking-network synthetic graph in two parallel formats
so DeltaForge and Neo4j load *identical bytes*:

    data/graph_finance_sf<scale>/
      accounts.parquet           # for delta-forge / spark / duckdb readers
      transactions.parquet
      accounts.csv               # Neo4j bulk-import format (with :ID header)
      transactions.csv           # Neo4j bulk-import format (with :TYPE header)
      manifest.json              # row counts + sha256 of each file

The schema mirrors the ``graph-gpu-10m-finance`` demo exactly: same column
names, same column types, same per-id deterministic distributions, same 7
edge-batch topology. Only the deterministic-name source differs (we use a
bundled 100-name cyclic list rather than the engine's fake-rs corpus,
because fake-rs is not reproducible cross-language). Names are the only
column where the bench data and the DF demo data diverge; every other
column is bit-identical.

Scale tiers
-----------
The bench uses one ``--scale`` knob. For the graph chapter we map scale
factors to node counts as follows:

    scale=1     ->   100 000 nodes,    ~480 000 edges  (smoke)
    scale=10    -> 1 000 000 nodes,   ~4 800 000 edges  (laptop standard)
    scale=100   -> 10 000 000 nodes, ~48 099 998 edges  (demo-equivalent)

All seven topology batches scale proportionally with N, and the strides
are kept fixed so degree distribution at scale 10 looks like a 10x-shrunk
version of scale 100. The smoke tier exists for end-to-end CI testing;
the laptop standard is the v0.1 published headline; scale=100 reproduces
the original demo bit-for-bit.

Reproducibility
---------------
Run twice with the same ``--scale`` and the manifest sha256s match. The
generator is single-threaded (DuckDB uses internal parallelism) and uses
no random-number generator; every column is a closed-form function of the
row id.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import sys
import time
from pathlib import Path
from typing import Iterable

# DuckDB is the same dependency the TPC-H generator uses (requirements.txt
# pins duckdb==1.1.3). Keep the dependency surface narrow so a reviewer
# can run this script from a fresh `pip install -r requirements.txt`.
import duckdb  # type: ignore


REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_OUT_PARENT = REPO_ROOT / "data"


# Bank list mirrors graph-gpu-10m-finance/setup.sql verbatim. Keep the
# order: cyclic_lookup is 0-indexed mod 30, so reordering breaks per-id
# determinism.
BANKS: tuple[str, ...] = (
    "JPMorgan", "Goldman Sachs", "Morgan Stanley", "Citibank",
    "HSBC", "Barclays", "Deutsche Bank", "BNP Paribas",
    "Credit Suisse", "UBS", "Santander", "BBVA",
    "ING", "Rabobank", "Nordea", "DBS",
    "ANZ", "Westpac", "MUFG", "Sumitomo Mitsui",
    "Mizuho", "ICBC", "Bank of China", "Standard Chartered",
    "Itau", "Bradesco", "OCBC", "Siam Commercial",
    "SEB", "Danske Bank",
)
assert len(BANKS) == 30, "BANKS must be 30 entries to match the demo's bank schema."

CITIES: tuple[str, ...] = (
    "New York", "London", "Tokyo", "Shanghai", "Singapore",
    "Hong Kong", "Frankfurt", "Paris", "Zurich", "Sydney",
    "Toronto", "Mumbai", "Seoul", "Sao Paulo", "Dubai",
    "Amsterdam", "Stockholm", "Dublin", "Chicago", "San Francisco",
    "Boston", "Geneva", "Luxembourg", "Taipei", "Jakarta",
)
assert len(CITIES) == 25, "CITIES must be 25 entries to match the demo's city schema."

# 100 deterministic first names + 100 last names, sourced from the most
# common public-record name distributions. Cyclic by (id-1) % 100, so
# regenerating with the same scale produces the same names.
FIRST_NAMES: tuple[str, ...] = (
    "James", "Mary", "Robert", "Patricia", "John", "Jennifer", "Michael",
    "Linda", "William", "Elizabeth", "David", "Barbara", "Richard", "Susan",
    "Joseph", "Jessica", "Thomas", "Sarah", "Charles", "Karen", "Christopher",
    "Lisa", "Daniel", "Nancy", "Matthew", "Betty", "Anthony", "Sandra",
    "Mark", "Margaret", "Donald", "Ashley", "Steven", "Kimberly", "Paul",
    "Emily", "Andrew", "Donna", "Joshua", "Michelle", "Kenneth", "Carol",
    "Kevin", "Amanda", "Brian", "Melissa", "George", "Deborah", "Edward",
    "Stephanie", "Ronald", "Rebecca", "Timothy", "Sharon", "Jason", "Laura",
    "Jeffrey", "Cynthia", "Ryan", "Kathleen", "Jacob", "Amy", "Gary",
    "Shirley", "Nicholas", "Angela", "Eric", "Helen", "Jonathan", "Anna",
    "Stephen", "Brenda", "Larry", "Pamela", "Justin", "Nicole", "Scott",
    "Samantha", "Brandon", "Katherine", "Benjamin", "Christine", "Samuel",
    "Debra", "Gregory", "Rachel", "Frank", "Catherine", "Alexander",
    "Carolyn", "Raymond", "Janet", "Patrick", "Ruth", "Jack", "Maria",
    "Dennis", "Heather", "Jerry", "Diane", "Tyler", "Virginia",
)
assert len(FIRST_NAMES) == 100

LAST_NAMES: tuple[str, ...] = (
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller",
    "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez",
    "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson", "Martin",
    "Lee", "Perez", "Thompson", "White", "Harris", "Sanchez", "Clark",
    "Ramirez", "Lewis", "Robinson", "Walker", "Young", "Allen", "King",
    "Wright", "Scott", "Torres", "Nguyen", "Hill", "Flores", "Green",
    "Adams", "Nelson", "Baker", "Hall", "Rivera", "Campbell", "Mitchell",
    "Carter", "Roberts", "Gomez", "Phillips", "Evans", "Turner", "Diaz",
    "Parker", "Cruz", "Edwards", "Collins", "Reyes", "Stewart", "Morris",
    "Morales", "Murphy", "Cook", "Rogers", "Gutierrez", "Ortiz", "Morgan",
    "Cooper", "Peterson", "Bailey", "Reed", "Kelly", "Howard", "Ramos",
    "Kim", "Cox", "Ward", "Richardson", "Watson", "Brooks", "Chavez",
    "Wood", "James", "Bennett", "Gray", "Mendoza", "Ruiz", "Hughes",
    "Price", "Alvarez", "Castillo", "Sanders", "Patel", "Myers", "Long",
    "Ross", "Foster", "Jimenez", "Powell",
)
assert len(LAST_NAMES) == 100


def n_nodes_for_scale(scale: int) -> int:
    """Map the bench's integer ``--scale`` to a node count.

    The mapping intentionally matches the comment in the module docstring:
    smoke -> laptop standard -> demo scale.
    """
    if scale <= 1:
        return 100_000
    if scale <= 10:
        return 1_000_000
    if scale <= 100:
        return 10_000_000
    # For scales beyond 100 (future at-scale tier), grow linearly past 10M.
    return 10_000_000 * (scale // 100)


def edge_batch_sizes(n_nodes: int) -> dict[str, int]:
    """Edge-batch sizes scaled proportionally to N relative to the demo's 10M.

    The demo at N=10_000_000 produces:
        batch1 = 15_000_000  (10M stride-20 + 5M stride-40)
        batch2 = 10_000_000  (7M stride-200 + 3M stride-400)
        batch3 =  5_500_000  (4M stride-15 + 1.5M stride-45 after the
                              same-bank stride-30 sub-batch is filtered)
        batch4 =  5_500_000  (advisory hierarchy)
        batch5 =  4_000_000  (compliance, ~2% of accounts * 20 offsets)
        batch6 =  4_900_000  (institutional hubs)
        batch7 =  3_199_998  (P2P weak ties, minus 2 self-loops)

    Each batch scales to ``n_nodes / 10_000_000`` of the original size,
    rounded so the totals at scale=100 reproduce the demo exactly.
    """
    scale_ratio = n_nodes / 10_000_000

    def s(n: int) -> int:
        return max(1, int(round(n * scale_ratio)))

    return {
        "b1_stride20":   s(10_000_000),
        "b1_stride40":   s(5_000_000),
        "b2_stride200":  s(7_000_000),
        "b2_stride400":  s(3_000_000),
        "b3_stride15":   s(4_000_000),
        "b3_stride30":   s(2_500_000),  # filtered out by same-bank rule
        "b3_stride45":   s(1_500_000),
        "b4_managers":   s(200_000),
        "b5_bridges":    s(n_nodes // 100 * 2),  # ~2% of accounts
        "b6_hubs":       s(500_000),
        "b7_p2p":        s(3_200_000),
    }


def quote_sql_str(s: str) -> str:
    """Render a Python string as a single-quoted SQL literal, escaping
    embedded single quotes by doubling them."""
    return "'" + s.replace("'", "''") + "'"


def list_lookup_sql(values: Iterable[str], idx_expr: str) -> str:
    """Build a SQL ``CASE`` expression that maps ``idx_expr`` (an integer
    expression in the range 0..len(values)-1) to the corresponding string.

    DuckDB has ``list_extract`` which is more compact, but we use CASE for
    portability with explain plans and to keep the SQL diff'able against
    the original demo.
    """
    arms: list[str] = []
    for i, v in enumerate(values):
        arms.append(f"WHEN {i} THEN {quote_sql_str(v)}")
    return f"CASE ({idx_expr}) {' '.join(arms)} END"


# ---------------------------------------------------------------------------
# Account generation
# ---------------------------------------------------------------------------

def build_accounts_sql(n: int) -> str:
    """SQL that produces the full ``gfn_accounts`` projection for N rows."""
    bank_lookup = list_lookup_sql(BANKS, "((id - 1) % 30)")
    city_lookup = list_lookup_sql(CITIES, "((id - 1) % 25)")
    fname_lookup = list_lookup_sql(FIRST_NAMES, "((id - 1) % 100)")
    lname_lookup = list_lookup_sql(LAST_NAMES, "((id - 1 + 37) % 100)")

    return f"""
WITH ids AS (
    SELECT (gs + 1)::BIGINT AS id
    FROM range(0, {n}) AS t(gs)
)
SELECT
    id,
    ({fname_lookup}) || ' ' || ({lname_lookup})        AS name,
    {bank_lookup}                                       AS bank,
    {city_lookup}                                       AS city,
    CASE
        WHEN id % 20 <= 15 THEN 'retail'
        WHEN id % 20 <= 18 THEN 'corporate'
        ELSE                       'institutional'
    END                                                 AS account_type,
    CASE
        WHEN id % 1000 = 0 THEN 'critical'
        WHEN id % 200  = 0 THEN 'high'
        WHEN id % 50   = 0 THEN 'medium'
        ELSE                    'low'
    END                                                 AS risk_tier,
    CASE
        WHEN id % 1000 = 0 THEN 'Ultra-High'
        WHEN id % 500  = 0 THEN 'High'
        WHEN id % 100  = 0 THEN 'Premium'
        WHEN id % 50   = 0 THEN 'Standard-Plus'
        WHEN id % 10   = 0 THEN 'Standard'
        ELSE                    'Basic'
    END                                                 AS balance_band,
    CASE
        WHEN id % 1000 = 0 THEN 'Enhanced'
        WHEN id % 100  = 0 THEN 'Standard'
        ELSE                    'Basic'
    END                                                 AS kyc_level,
    CAST(2010 + (id % 16) AS INTEGER)                   AS open_year,
    (id % 21) <> 0                                       AS active
FROM ids
"""


# ---------------------------------------------------------------------------
# Transaction (edge) generation
# ---------------------------------------------------------------------------

def build_transactions_sql(n: int, sizes: dict[str, int]) -> str:
    """SQL producing the full edge multiset for the seven topology batches.

    Mirrors graph-gpu-10m-finance/setup.sql line for line, with the strides
    fixed and the per-batch row counts taken from ``sizes``. ROW_NUMBER is
    only used in batches 4-7 where the original SQL uses it; batches 1-3
    derive ids from per-batch offsets.
    """
    # Type-distribution arms reuse the demo's exact CASE expressions so the
    # per-type counts in the assert table (wire-transfer = 7.5M etc) hold.
    return f"""
WITH
b1 AS (
    SELECT
        id, src, dst,
        ROUND(0.6 + 0.4 * (((src * 7 + dst * 13)::DOUBLE * 0.618033988749895) % 1.0), 3)::DOUBLE AS weight,
        CASE ((src + dst) % 4)
            WHEN 0 THEN 'wire-transfer'  WHEN 1 THEN 'ach-payment'
            WHEN 2 THEN 'card-payment'   WHEN 3 THEN 'direct-debit'
        END AS transaction_type,
        CAST(2015 + ((src + dst) % 11) AS INTEGER) AS tx_year
    FROM (
        SELECT (gs + 1)::BIGINT AS id,
               ((gs % {n}) + 1)::BIGINT AS src,
               (((gs % {n} + 20) % {n}) + 1)::BIGINT AS dst
        FROM range(0, {sizes['b1_stride20']}) AS t(gs)
        UNION ALL
        SELECT ({sizes['b1_stride20']} + gs + 1)::BIGINT AS id,
               ((gs % {n}) + 1)::BIGINT AS src,
               (((gs % {n} + 40) % {n}) + 1)::BIGINT AS dst
        FROM range(0, {sizes['b1_stride40']}) AS t(gs)
    )
    WHERE src <> dst
),
b2 AS (
    SELECT
        id, src, dst,
        ROUND(0.5 + 0.4 * (((src * 11 + dst * 17)::DOUBLE * 0.618033988749895) % 1.0), 3)::DOUBLE AS weight,
        CASE ((src * 3 + dst) % 3)
            WHEN 0 THEN 'loan-payment'      WHEN 1 THEN 'mortgage-payment'
            WHEN 2 THEN 'investment-transfer'
        END AS transaction_type,
        CAST(2018 + ((src + dst) % 8) AS INTEGER) AS tx_year
    FROM (
        SELECT (100000000 + gs + 1)::BIGINT AS id,
               ((gs % {n}) + 1)::BIGINT AS src,
               (((gs % {n} + 200) % {n}) + 1)::BIGINT AS dst
        FROM range(0, {sizes['b2_stride200']}) AS t(gs)
        UNION ALL
        SELECT (100000000 + {sizes['b2_stride200']} + gs + 1)::BIGINT AS id,
               ((gs % {n}) + 1)::BIGINT AS src,
               (((gs % {n} + 400) % {n}) + 1)::BIGINT AS dst
        FROM range(0, {sizes['b2_stride400']}) AS t(gs)
    )
    WHERE src <> dst
),
b3 AS (
    SELECT
        id, src, dst,
        ROUND(0.2 + 0.3 * (((src * 23 + dst * 29)::DOUBLE * 0.618033988749895) % 1.0), 3)::DOUBLE AS weight,
        CASE ((src + dst * 2) % 4)
            WHEN 0 THEN 'interbank-clearing'    WHEN 1 THEN 'correspondent-banking'
            WHEN 2 THEN 'cross-border-transfer'  WHEN 3 THEN 'fx-settlement'
        END AS transaction_type,
        CAST(2019 + ((src + dst) % 7) AS INTEGER) AS tx_year
    FROM (
        SELECT (200000000 + gs + 1)::BIGINT AS id,
               ((gs % {n}) + 1)::BIGINT AS src,
               (((gs % {n} + 15) % {n}) + 1)::BIGINT AS dst
        FROM range(0, {sizes['b3_stride15']}) AS t(gs)
        UNION ALL
        SELECT (200000000 + {sizes['b3_stride15']} + gs + 1)::BIGINT AS id,
               ((gs % {n}) + 1)::BIGINT AS src,
               (((gs % {n} + 30) % {n}) + 1)::BIGINT AS dst
        FROM range(0, {sizes['b3_stride30']}) AS t(gs)
        UNION ALL
        SELECT (200000000 + {sizes['b3_stride15']} + {sizes['b3_stride30']} + gs + 1)::BIGINT AS id,
               ((gs % {n}) + 1)::BIGINT AS src,
               (((gs % {n} + 45) % {n}) + 1)::BIGINT AS dst
        FROM range(0, {sizes['b3_stride45']}) AS t(gs)
    )
    WHERE src <> dst AND ((src - 1) % 30) <> ((dst - 1) % 30)
),
b4_pairs AS (
    -- Advisory hierarchy. Manager fan-out is power-law: per the demo,
    -- senior managers (id divisible by 1000) advise 100 clients;
    -- mid-tier (id mod 500 == 0 but not mod 1000) advise 60; line
    -- managers (mod 100) advise 30; juniors advise 15.
    SELECT manager_id AS src, ((manager_id - 1 + k * 20) % {n}) + 1 AS dst
    FROM (
        SELECT m.manager_id, o.k
        FROM (SELECT ((gs + 1) * 50)::BIGINT AS manager_id
              FROM range(0, {sizes['b4_managers']}) AS t(gs)) m
        CROSS JOIN (SELECT (gs + 1)::BIGINT AS k FROM range(0, 100) AS t(gs)) o
        WHERE (m.manager_id % 1000 = 0 AND o.k <= 100)
           OR (m.manager_id % 1000 <> 0 AND m.manager_id % 500 = 0 AND o.k <= 60)
           OR (m.manager_id % 500  <> 0 AND m.manager_id % 100 = 0 AND o.k <= 30)
           OR (m.manager_id % 100  <> 0 AND o.k <= 15)
    ) pairs
),
b4 AS (
    SELECT
        300000000 + ROW_NUMBER() OVER (ORDER BY src, dst) AS id,
        src, dst,
        ROUND(0.6 + 0.4 * (((src * 3 + dst * 7)::DOUBLE * 0.618033988749895) % 1.0), 3)::DOUBLE AS weight,
        'advisory'::VARCHAR AS transaction_type,
        CAST(2016 + ((src + dst) % 10) AS INTEGER) AS tx_year
    FROM b4_pairs
    WHERE src <> dst AND dst BETWEEN 1 AND {n}
),
b5_pairs AS (
    -- Compliance bridge. ~2% of accounts (id % 100 < 2) fan out to 20
    -- nearby accounts each (offsets 1..21, skipping 20).
    SELECT bridge_id AS src, ((bridge_id - 1 + offset) % {n}) + 1 AS dst
    FROM (
        SELECT (gs + 1)::BIGINT AS bridge_id
        FROM range(0, {n}) AS t(gs) WHERE (gs + 1) % 100 < 2
    ) bridges
    CROSS JOIN (
        SELECT (gs + 1)::BIGINT AS offset
        FROM range(0, 21) AS t(gs) WHERE (gs + 1) <> 20
    ) offsets
),
b5 AS (
    SELECT
        400000000 + ROW_NUMBER() OVER (ORDER BY src, dst) AS id,
        src, dst,
        ROUND(0.3 + 0.3 * (((src * 19 + dst * 23)::DOUBLE * 0.618033988749895) % 1.0), 3)::DOUBLE AS weight,
        CASE ((src + dst) % 3)
            WHEN 0 THEN 'compliance-referral'
            WHEN 1 THEN 'aml-flag'
            WHEN 2 THEN 'regulatory-report'
        END AS transaction_type,
        CAST(2017 + ((src + dst) % 9) AS INTEGER) AS tx_year
    FROM b5_pairs
    WHERE src <> dst AND dst BETWEEN 1 AND {n}
),
b6_pairs AS (
    SELECT hub_id AS src, ((hub_id - 1 + k * 7) % {n}) + 1 AS dst
    FROM (
        SELECT m.hub_id, o.k
        FROM (SELECT ((gs + 1) * 20)::BIGINT AS hub_id
              FROM range(0, {sizes['b6_hubs']}) AS t(gs)) m
        CROSS JOIN (SELECT (gs + 1)::BIGINT AS k FROM range(0, 50) AS t(gs)) o
        WHERE (m.hub_id % 1000 = 0 AND o.k <= 50)
           OR (m.hub_id % 1000 <> 0 AND m.hub_id % 500 = 0 AND o.k <= 40)
           OR (m.hub_id % 500  <> 0 AND m.hub_id % 100 = 0 AND o.k <= 25)
           OR (m.hub_id % 100  <> 0 AND o.k <= 5)
    ) pairs
),
b6 AS (
    SELECT
        500000000 + ROW_NUMBER() OVER (ORDER BY src, dst) AS id,
        src, dst,
        ROUND(0.4 + 0.4 * (((src * 31 + dst * 37)::DOUBLE * 0.618033988749895) % 1.0), 3)::DOUBLE AS weight,
        CASE ((src + dst) % 3)
            WHEN 0 THEN 'institutional-transfer'
            WHEN 1 THEN 'syndicate-payment'
            WHEN 2 THEN 'treasury-sweep'
        END AS transaction_type,
        CAST(2014 + ((src + dst) % 12) AS INTEGER) AS tx_year
    FROM b6_pairs
    WHERE src <> dst AND dst BETWEEN 1 AND {n}
),
b7 AS (
    SELECT
        600000000 + ROW_NUMBER() OVER (ORDER BY src, dst) AS id,
        src, dst,
        ROUND(0.05 + 0.15 * (((src * 43 + dst * 47)::DOUBLE * 0.618033988749895) % 1.0), 3)::DOUBLE AS weight,
        CASE ((src * 7 + dst * 3) % 4)
            WHEN 0 THEN 'p2p-transfer'         WHEN 1 THEN 'merchant-settlement'
            WHEN 2 THEN 'atm-withdrawal'        WHEN 3 THEN 'pos-purchase'
        END AS transaction_type,
        CAST(2022 + ((src + dst) % 4) AS INTEGER) AS tx_year
    FROM (
        SELECT
            ((i * 104729 + 56891) % {n}) + 1 AS src,
            ((i * 224737 + 31547) % {n}) + 1 AS dst
        FROM range(0, {sizes['b7_p2p']}) AS t(i)
    ) sub
    WHERE src <> dst
)
SELECT id, src, dst, weight, transaction_type, tx_year FROM b1
UNION ALL SELECT id, src, dst, weight, transaction_type, tx_year FROM b2
UNION ALL SELECT id, src, dst, weight, transaction_type, tx_year FROM b3
UNION ALL SELECT id, src, dst, weight, transaction_type, tx_year FROM b4
UNION ALL SELECT id, src, dst, weight, transaction_type, tx_year FROM b5
UNION ALL SELECT id, src, dst, weight, transaction_type, tx_year FROM b6
UNION ALL SELECT id, src, dst, weight, transaction_type, tx_year FROM b7
"""


# ---------------------------------------------------------------------------
# Output helpers
# ---------------------------------------------------------------------------

def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


# Neo4j bulk-import CSV header rows. The headers below tell
# `neo4j-admin database import full` how to interpret each column:
#   id:ID(Account)            -> primary key, namespace 'Account'
#   name                       -> property of type string (default)
#   open_year:int              -> property typed as integer
#   active:boolean             -> property typed as boolean
#   :START_ID(Account)         -> source-node reference
#   :END_ID(Account)           -> target-node reference
#   :TYPE                      -> relationship type column
#
# https://neo4j.com/docs/operations-manual/current/tools/neo4j-admin/neo4j-admin-import/
ACCOUNTS_CSV_HEADER = (
    "id:ID(Account),name,bank,city,account_type,risk_tier,balance_band,"
    "kyc_level,open_year:int,active:boolean"
)
TRANSACTIONS_CSV_HEADER = (
    "id:long,:START_ID(Account),:END_ID(Account),weight:double,"
    "transaction_type,tx_year:int,:TYPE"
)
NEO4J_REL_TYPE = "TRANSACTED"


def _write_parquet(con, sql: str, parquet_path: Path) -> None:
    """Run SQL and write the result to Parquet via DuckDB's COPY TO.

    Parquet compression matches the TPC-H generator's default (snappy).
    """
    parquet_path.parent.mkdir(parents=True, exist_ok=True)
    con.execute(
        f"COPY ({sql}) TO '{parquet_path.as_posix()}' "
        f"(FORMAT PARQUET, COMPRESSION 'snappy')"
    )


def _write_neo4j_accounts_csv(con, sql: str, csv_path: Path) -> None:
    """Write accounts to a Neo4j-headered CSV in two stages:

      1. DuckDB COPY ... TO 'tmp.csv' (HEADER, DELIMITER) -- fast bulk write.
      2. Replace DuckDB's column-name header with the Neo4j typed header.

    DuckDB's first-row header carries the bare column names; Neo4j's
    bulk loader needs `id:ID(Account)` etc., so we strip-and-prepend.
    """
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = csv_path.with_suffix(".csv.tmp")
    con.execute(
        f"COPY ({sql}) TO '{tmp.as_posix()}' "
        f"(FORMAT CSV, HEADER FALSE, DELIMITER ',')"
    )
    with csv_path.open("w", encoding="utf-8", newline="\n") as out:
        out.write(ACCOUNTS_CSV_HEADER + "\n")
        with tmp.open("r", encoding="utf-8") as src:
            shutil.copyfileobj(src, out)
    tmp.unlink()


def _write_neo4j_transactions_csv(con, sql: str, csv_path: Path) -> None:
    """Same shape as accounts; we wrap the source SQL to append the
    constant ``:TYPE`` column so every row carries 'TRANSACTED' as the
    Neo4j relationship type."""
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = csv_path.with_suffix(".csv.tmp")
    wrapped = (
        f"SELECT id, src, dst, weight, transaction_type, tx_year, "
        f"'{NEO4J_REL_TYPE}' AS rel_type FROM ({sql})"
    )
    con.execute(
        f"COPY ({wrapped}) TO '{tmp.as_posix()}' "
        f"(FORMAT CSV, HEADER FALSE, DELIMITER ',')"
    )
    with csv_path.open("w", encoding="utf-8", newline="\n") as out:
        out.write(TRANSACTIONS_CSV_HEADER + "\n")
        with tmp.open("r", encoding="utf-8") as src:
            shutil.copyfileobj(src, out)
    tmp.unlink()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def generate(scale: int, out_parent: Path = DEFAULT_OUT_PARENT,
             keep_existing: bool = False) -> Path:
    """Generate the graph-finance dataset for ``scale``.

    Returns the directory containing the output files. Existing output
    is left in place unless ``keep_existing=False`` (the default), in
    which case the directory is wiped and rebuilt.
    """
    n = n_nodes_for_scale(scale)
    sizes = edge_batch_sizes(n)

    out_dir = out_parent / f"graph_finance_sf{scale}"
    if out_dir.exists() and not keep_existing:
        shutil.rmtree(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"  scale={scale}  n_nodes={n:,}  out={out_dir}")
    print(f"  edge batches: {sizes}")

    # In-memory DuckDB; the COPY TO calls write straight to disk so we
    # never materialise the full table set in memory.
    threads = max(1, (os.cpu_count() or 4))
    con = duckdb.connect(database=":memory:")
    con.execute(f"PRAGMA threads={threads}")
    con.execute("PRAGMA preserve_insertion_order=true")

    accounts_sql = build_accounts_sql(n)
    transactions_sql = build_transactions_sql(n, sizes)

    t0 = time.perf_counter()
    print("  writing accounts.parquet ...")
    _write_parquet(con, accounts_sql, out_dir / "accounts.parquet")
    t1 = time.perf_counter()
    print(f"    accounts.parquet: {(t1 - t0):.2f}s")

    print("  writing transactions.parquet ...")
    _write_parquet(con, transactions_sql, out_dir / "transactions.parquet")
    t2 = time.perf_counter()
    print(f"    transactions.parquet: {(t2 - t1):.2f}s")

    print("  writing accounts.csv (Neo4j-headered) ...")
    _write_neo4j_accounts_csv(con, accounts_sql, out_dir / "accounts.csv")
    t3 = time.perf_counter()
    print(f"    accounts.csv: {(t3 - t2):.2f}s")

    print("  writing transactions.csv (Neo4j-headered) ...")
    _write_neo4j_transactions_csv(con, transactions_sql,
                                  out_dir / "transactions.csv")
    t4 = time.perf_counter()
    print(f"    transactions.csv: {(t4 - t3):.2f}s")

    # Counts for the manifest (reads back the parquet so we record what
    # actually landed on disk, not the source SQL's claimed row count).
    nodes_n = con.execute(
        f"SELECT COUNT(*) FROM read_parquet('{(out_dir / 'accounts.parquet').as_posix()}')"
    ).fetchone()[0]
    edges_n = con.execute(
        f"SELECT COUNT(*) FROM read_parquet('{(out_dir / 'transactions.parquet').as_posix()}')"
    ).fetchone()[0]
    con.close()

    manifest = {
        "schema_version": 1,
        "scale": scale,
        "n_nodes": int(nodes_n),
        "n_edges": int(edges_n),
        "edge_batch_sizes": sizes,
        "files": {
            name: {
                "size_bytes": (out_dir / name).stat().st_size,
                "sha256":    _sha256_file(out_dir / name),
            }
            for name in (
                "accounts.parquet", "transactions.parquet",
                "accounts.csv",     "transactions.csv",
            )
        },
        "neo4j_relationship_type": NEO4J_REL_TYPE,
        "header": {
            "accounts.csv":     ACCOUNTS_CSV_HEADER,
            "transactions.csv": TRANSACTIONS_CSV_HEADER,
        },
        "elapsed_seconds": round(time.perf_counter() - t0, 3),
    }
    with (out_dir / "manifest.json").open("w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, sort_keys=True)
        f.write("\n")

    print(f"  done. nodes={nodes_n:,}  edges={edges_n:,}  "
          f"elapsed={manifest['elapsed_seconds']:.2f}s")
    return out_dir


def main() -> int:
    p = argparse.ArgumentParser(
        description="Generate the graph-finance benchmark dataset.",
    )
    p.add_argument(
        "--scale", type=int, default=int(os.environ.get("BENCH_SCALE_FACTOR", "1")),
        help="Scale factor: 1=smoke (100K nodes), 10=standard (1M), 100=demo (10M).",
    )
    p.add_argument(
        "--out", default=str(DEFAULT_OUT_PARENT),
        help="Parent directory for output (default: <repo>/data).",
    )
    p.add_argument(
        "--keep", action="store_true",
        help="Keep existing files at the destination if they exist.",
    )
    args = p.parse_args()
    generate(args.scale, Path(args.out), keep_existing=args.keep)
    return 0


if __name__ == "__main__":
    sys.exit(main())
