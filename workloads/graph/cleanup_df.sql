-- Cleanup the DeltaForge side of the graph_finance workload.
-- Order matters: graph -> tables -> schema -> zone, because a non-empty
-- schema cannot be dropped and a non-empty zone cannot be dropped.

DROP GRAPH IF EXISTS bench.fin.fin_graph;
DROP TABLE IF EXISTS bench.fin.gfn_transactions WITH FILES;
DROP TABLE IF EXISTS bench.fin.gfn_accounts WITH FILES;
DROP SCHEMA IF EXISTS bench.fin;
DROP ZONE IF EXISTS bench;
