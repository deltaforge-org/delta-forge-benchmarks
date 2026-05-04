"""Graph-finance workload: head-to-head Cypher comparison.

Runs the bench-generated synthetic global-banking-network graph through
DeltaForge's native Cypher runtime and Neo4j Community + GDS, and reports
per-query wall-clock + engine-reported times plus deterministic result
hashes for cross-engine correctness.

Workload shape
--------------

setup_steps (per engine):
    df    -> workloads/graph/load_df.sql
    neo4j -> workloads/graph/load_neo4j.cypher

measured_steps:
    A portable subset of the original 10M-finance demo's queries, plus
    five GDS-equivalent algorithm queries. Each measured step carries
    a `per_engine_sql` dict so the same logical query becomes:

      DF:    USE bench.fin.fin_graph
             MATCH (n) RETURN count(n)

      Neo4j: MATCH (n:Account) RETURN count(n)

    Result hashes are compared cross-engine for the deterministic
    queries (counts, MATCH expansions, WCC component sizes, triangle
    count top-K). Non-deterministic algorithms (Louvain, sampled
    betweenness, raw PageRank scores at floating-point precision) are
    timed but not hashed; the report flags them as such so a reviewer
    sees the difference is intentional.

cleanup_steps (per engine):
    df    -> workloads/graph/cleanup_df.sql
    neo4j -> workloads/graph/cleanup_neo4j.cypher

Why this is a separate workload
-------------------------------
The TPC-H workloads run on every (SQL-capable) engine in the bench. The
graph_finance workload only makes sense on engines with a graph runtime,
so it sets `applicable_engines = ('df', 'neo4j')` and the runner skips
it on the Spark adapters.
"""
from __future__ import annotations

from pathlib import Path

from engines.base import (
    STEP_CYPHER_DML,
    STEP_CYPHER_QUERY,
    STEP_SQL_DDL,
    WorkloadStep,
)

from .spec import Workload

WORKLOAD_DIR = Path(__file__).parent / "graph"
LOAD_DF_SQL = (WORKLOAD_DIR / "load_df.sql").read_text(encoding="utf-8")
LOAD_NEO4J_CYPHER = (WORKLOAD_DIR / "load_neo4j.cypher").read_text(encoding="utf-8")
CLEANUP_DF_SQL = (WORKLOAD_DIR / "cleanup_df.sql").read_text(encoding="utf-8")
CLEANUP_NEO4J_CYPHER = (WORKLOAD_DIR / "cleanup_neo4j.cypher").read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# Setup / cleanup
# ---------------------------------------------------------------------------

def _setup_steps() -> list[WorkloadStep]:
    """One step that does double duty across `df` and `neo4j`. The runner
    swaps in the engine-specific `kind` (SQL DDL vs Cypher DML) and the
    engine-specific text body."""
    return [
        WorkloadStep(
            id="load",
            kind=STEP_SQL_DDL,  # default; overridden per engine below
            description="Load accounts + transactions, build CSR / GDS projection",
            measured=False,
            per_engine_sql={
                "df":    LOAD_DF_SQL,
                "neo4j": LOAD_NEO4J_CYPHER,
            },
            per_engine_kind={
                "df":    STEP_SQL_DDL,
                "neo4j": STEP_CYPHER_DML,
            },
        ),
    ]


def _cleanup_steps() -> list[WorkloadStep]:
    return [
        WorkloadStep(
            id="cleanup",
            kind=STEP_SQL_DDL,
            description="Drop graph, tables, schema, zone (or wipe Neo4j db)",
            measured=False,
            per_engine_sql={
                "df":    CLEANUP_DF_SQL,
                "neo4j": CLEANUP_NEO4J_CYPHER,
            },
            per_engine_kind={
                "df":    STEP_SQL_DDL,
                "neo4j": STEP_CYPHER_DML,
            },
        ),
    ]


# ---------------------------------------------------------------------------
# Measured queries
# ---------------------------------------------------------------------------
#
# DF Cypher uses `USE <graph>` to target the named graph; Neo4j addresses
# nodes by label and relationships by type, so the equivalent has no USE
# clause but uses :Account / :TRANSACTED labels. Both engines accept
# `count(n)`, `count(r)`, ORDER BY, LIMIT, and arithmetic comparisons.
#
# The DF variants intentionally do NOT use `ON GPU`. The fair head-to-head
# is CPU vs CPU because Neo4j has no GPU path; the GPU comparison is a
# separate (future) workload variant.

DF_USE = "USE bench.fin.fin_graph\n"


def _q_count_nodes() -> WorkloadStep:
    return WorkloadStep(
        id="q01_count_nodes",
        kind=STEP_CYPHER_QUERY,
        expects_rows=True,
        description="Total node (Account) count",
        per_engine_sql={
            "df": DF_USE + "MATCH (n) RETURN count(n) AS total_accounts",
            "neo4j": "MATCH (n:Account) RETURN count(n) AS total_accounts",
        },
    )


def _q_count_edges() -> WorkloadStep:
    return WorkloadStep(
        id="q02_count_edges",
        kind=STEP_CYPHER_QUERY,
        expects_rows=True,
        description="Total relationship (transaction) count",
        per_engine_sql={
            "df": DF_USE + "MATCH (a)-[r]->(b) RETURN count(r) AS total_transactions",
            "neo4j": "MATCH (:Account)-[r:TRANSACTED]->(:Account) "
                     "RETURN count(r) AS total_transactions",
        },
    )


def _q_bank_distribution() -> WorkloadStep:
    return WorkloadStep(
        id="q03_bank_distribution",
        kind=STEP_CYPHER_QUERY,
        expects_rows=True,
        description="Account count per bank (deterministic 30 rows)",
        per_engine_sql={
            "df": DF_USE
                  + "MATCH (n) RETURN n.bank AS bank, count(n) AS headcount "
                    "ORDER BY headcount DESC, bank ASC",
            "neo4j": "MATCH (n:Account) "
                     "RETURN n.bank AS bank, count(n) AS headcount "
                     "ORDER BY headcount DESC, bank ASC",
        },
    )


def _q_advisory_count() -> WorkloadStep:
    return WorkloadStep(
        id="q04_advisory_count",
        kind=STEP_CYPHER_QUERY,
        expects_rows=True,
        description="Edges where transaction_type = 'advisory'",
        per_engine_sql={
            "df": DF_USE + "MATCH (a)-[r]->(b) "
                          "WHERE r.transaction_type = 'advisory' "
                          "RETURN count(r) AS advisory_count",
            "neo4j": "MATCH ()-[r:TRANSACTED]->() "
                     "WHERE r.transaction_type = 'advisory' "
                     "RETURN count(r) AS advisory_count",
        },
    )


def _q_cross_bank_pairs() -> WorkloadStep:
    return WorkloadStep(
        id="q05_cross_bank_pairs",
        kind=STEP_CYPHER_QUERY,
        expects_rows=True,
        description="Top 30 cross-bank pair connection counts + avg weight",
        per_engine_sql={
            "df": DF_USE
                  + "MATCH (a)-[r]->(b) WHERE a.bank <> b.bank "
                    "RETURN a.bank AS from_bank, b.bank AS to_bank, "
                    "       count(r) AS connections, avg(r.weight) AS avg_strength "
                    "ORDER BY connections DESC, from_bank ASC, to_bank ASC LIMIT 30",
            "neo4j": "MATCH (a:Account)-[r:TRANSACTED]->(b:Account) "
                     "WHERE a.bank <> b.bank "
                     "RETURN a.bank AS from_bank, b.bank AS to_bank, "
                     "       count(r) AS connections, avg(r.weight) AS avg_strength "
                     "ORDER BY connections DESC, from_bank ASC, to_bank ASC LIMIT 30",
        },
    )


def _q_jpmorgan_intra_bank_top25() -> WorkloadStep:
    return WorkloadStep(
        id="q06_jpmorgan_intra_top25",
        kind=STEP_CYPHER_QUERY,
        expects_rows=True,
        description="Top 25 JPMorgan->JPMorgan transactions by weight",
        per_engine_sql={
            "df": DF_USE
                  + "MATCH (a)-[r]->(b) "
                    "WHERE a.bank = 'JPMorgan' AND b.bank = 'JPMorgan' "
                    "RETURN a.id AS from_id, b.id AS to_id, "
                    "       r.transaction_type AS type, r.weight AS weight "
                    "ORDER BY weight DESC, from_id ASC, to_id ASC LIMIT 25",
            "neo4j": "MATCH (a:Account)-[r:TRANSACTED]->(b:Account) "
                     "WHERE a.bank = 'JPMorgan' AND b.bank = 'JPMorgan' "
                     "RETURN a.id AS from_id, b.id AS to_id, "
                     "       r.transaction_type AS type, r.weight AS weight "
                     "ORDER BY weight DESC, from_id ASC, to_id ASC LIMIT 25",
        },
    )


def _q_transaction_type_distribution() -> WorkloadStep:
    return WorkloadStep(
        id="q07_transaction_type_dist",
        kind=STEP_CYPHER_QUERY,
        expects_rows=True,
        description="Distribution of edge transaction_type (deterministic, 18 rows)",
        per_engine_sql={
            "df": DF_USE
                  + "MATCH (a)-[r]->(b) "
                    "RETURN r.transaction_type AS type, count(r) AS cnt "
                    "ORDER BY cnt DESC, type ASC",
            "neo4j": "MATCH ()-[r:TRANSACTED]->() "
                     "RETURN r.transaction_type AS type, count(r) AS cnt "
                     "ORDER BY cnt DESC, type ASC",
        },
    )


def _q_subgraph_low_ids() -> WorkloadStep:
    return WorkloadStep(
        id="q08_subgraph_low_ids",
        kind=STEP_CYPHER_QUERY,
        expects_rows=True,
        description="Edges where both endpoints have id <= 100",
        per_engine_sql={
            "df": DF_USE
                  + "MATCH (a)-[r]->(b) WHERE a.id <= 100 AND b.id <= 100 "
                    "RETURN a.id AS from_id, b.id AS to_id, "
                    "       r.transaction_type AS type, r.weight AS weight "
                    "ORDER BY from_id ASC, to_id ASC, weight DESC",
            "neo4j": "MATCH (a:Account)-[r:TRANSACTED]->(b:Account) "
                     "WHERE a.id <= 100 AND b.id <= 100 "
                     "RETURN a.id AS from_id, b.id AS to_id, "
                     "       r.transaction_type AS type, r.weight AS weight "
                     "ORDER BY from_id ASC, to_id ASC, weight DESC",
        },
    )


def _q_high_risk_per_bank() -> WorkloadStep:
    return WorkloadStep(
        id="q09_high_risk_per_bank",
        kind=STEP_CYPHER_QUERY,
        expects_rows=True,
        description="Per-bank count of risk_tier = 'high' accounts",
        per_engine_sql={
            "df": DF_USE
                  + "MATCH (n) WHERE n.risk_tier = 'high' "
                    "RETURN n.bank AS bank, count(n) AS high_risk_cnt "
                    "ORDER BY high_risk_cnt DESC, bank ASC",
            "neo4j": "MATCH (n:Account) WHERE n.risk_tier = 'high' "
                     "RETURN n.bank AS bank, count(n) AS high_risk_cnt "
                     "ORDER BY high_risk_cnt DESC, bank ASC",
        },
    )


def _q_pagerank_top25() -> WorkloadStep:
    # PageRank scores are floating-point; engines with different summation
    # orders can disagree on score values past a few decimals, which can
    # rotate node ids across the top-25 boundary. Mark expects_rows=False
    # so we still measure latency but skip the cross-engine hash compare.
    return WorkloadStep(
        id="q10_pagerank_top25",
        kind=STEP_CYPHER_QUERY,
        expects_rows=False,
        description="GDS PageRank (20 iterations), top 25 by score [timing-only]",
        per_engine_sql={
            "df": DF_USE
                  + "CALL algo.pageRank({dampingFactor: 0.85, iterations: 20}) "
                    "YIELD node_id, score "
                    "RETURN node_id, score ORDER BY score DESC, node_id ASC LIMIT 25",
            "neo4j": "CALL gds.pageRank.stream('finance', "
                     "{dampingFactor: 0.85, maxIterations: 20}) "
                     "YIELD nodeId, score "
                     "RETURN nodeId AS node_id, score "
                     "ORDER BY score DESC, node_id ASC LIMIT 25",
        },
    )


def _q_connected_components() -> WorkloadStep:
    # WCC component sizes are deterministic (the multiset of component
    # sizes is invariant under representative choice). Hash works.
    return WorkloadStep(
        id="q11_connected_components",
        kind=STEP_CYPHER_QUERY,
        expects_rows=True,
        description="GDS Weakly Connected Components: distribution of sizes",
        per_engine_sql={
            "df": DF_USE
                  + "CALL algo.connectedComponents() YIELD node_id, component_id "
                    "WITH component_id, count(*) AS sz "
                    "RETURN sz, count(component_id) AS num_components "
                    "ORDER BY sz DESC LIMIT 25",
            "neo4j": "CALL gds.wcc.stream('finance') YIELD nodeId, componentId "
                     "WITH componentId, count(*) AS sz "
                     "RETURN sz, count(componentId) AS num_components "
                     "ORDER BY sz DESC LIMIT 25",
        },
    )


def _q_louvain() -> WorkloadStep:
    # Louvain is stochastic in node ordering; community labels are not
    # comparable across engines and even across runs of the same engine.
    return WorkloadStep(
        id="q12_louvain_top_communities",
        kind=STEP_CYPHER_QUERY,
        expects_rows=False,
        description="GDS Louvain community detection, top 25 sizes [timing-only]",
        per_engine_sql={
            "df": DF_USE
                  + "CALL algo.louvain({resolution: 1.0}) "
                    "YIELD node_id, community_id "
                    "WITH community_id, count(*) AS sz "
                    "RETURN sz ORDER BY sz DESC LIMIT 25",
            "neo4j": "CALL gds.louvain.stream('finance') "
                     "YIELD nodeId, communityId "
                     "WITH communityId, count(*) AS sz "
                     "RETURN sz ORDER BY sz DESC LIMIT 25",
        },
    )


def _q_triangle_count() -> WorkloadStep:
    return WorkloadStep(
        id="q13_triangle_count_top25",
        kind=STEP_CYPHER_QUERY,
        # Triangle count per node is deterministic; we hash on the
        # (node_id, triangle_count) pairs of the top 25.
        expects_rows=True,
        description="GDS Triangle Count, top 25 nodes",
        per_engine_sql={
            "df": DF_USE
                  + "CALL algo.triangleCount() YIELD node_id, triangle_count "
                    "RETURN node_id, triangle_count "
                    "ORDER BY triangle_count DESC, node_id ASC LIMIT 25",
            "neo4j": "CALL gds.triangleCount.stream('finance') "
                     "YIELD nodeId, triangleCount "
                     "RETURN nodeId AS node_id, triangleCount AS triangle_count "
                     "ORDER BY triangle_count DESC, node_id ASC LIMIT 25",
        },
    )


def _q_betweenness() -> WorkloadStep:
    # Sampled betweenness is stochastic; both DF (samplingSize: 1000) and
    # GDS (samplingSize) pick different traversal sources, so the score
    # multisets are not directly comparable.
    return WorkloadStep(
        id="q14_betweenness_top25",
        kind=STEP_CYPHER_QUERY,
        expects_rows=False,
        description="GDS Betweenness Centrality (sampled), top 25 [timing-only]",
        per_engine_sql={
            "df": DF_USE
                  + "CALL algo.betweenness({samplingSize: 1000}) "
                    "YIELD node_id, centrality "
                    "RETURN node_id, centrality "
                    "ORDER BY centrality DESC, node_id ASC LIMIT 25",
            "neo4j": "CALL gds.betweenness.stream('finance', {samplingSize: 1000}) "
                     "YIELD nodeId, score "
                     "RETURN nodeId AS node_id, score AS centrality "
                     "ORDER BY centrality DESC, node_id ASC LIMIT 25",
        },
    )


def _measured_steps() -> list[WorkloadStep]:
    return [
        _q_count_nodes(),
        _q_count_edges(),
        _q_bank_distribution(),
        _q_advisory_count(),
        _q_cross_bank_pairs(),
        _q_jpmorgan_intra_bank_top25(),
        _q_transaction_type_distribution(),
        _q_subgraph_low_ids(),
        _q_high_risk_per_bank(),
        _q_pagerank_top25(),
        _q_connected_components(),
        _q_louvain(),
        _q_triangle_count(),
        _q_betweenness(),
    ]


WORKLOAD = Workload(
    name="graph_finance",
    description=(
        "10M-account synthetic global-banking-network graph. "
        "Head-to-head Cypher comparison: DeltaForge vs Neo4j Community + GDS."
    ),
    setup_steps=_setup_steps(),
    measured_steps=_measured_steps(),
    cleanup_steps=_cleanup_steps(),
    cold_runs=1,
    warm_runs=4,  # graph queries are heavier; 1 cold + 4 warm is the
                  # cheaper cousin of the TPC-H 1+9 protocol that still
                  # produces a stable median + meaningful tail.
    requires_data_at_scale=None,
    applicable_engines=("df", "neo4j"),
    data_subdir="graph_finance_sf{scale}",
)
