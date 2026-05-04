// Cleanup the Neo4j side of the graph_finance workload.
// Drops the GDS projection first (it holds a reference to the live
// graph) then wipes the database.

CALL gds.graph.drop('finance', false) YIELD graphName
RETURN count(graphName) AS dropped;

MATCH (n) DETACH DELETE n;
DROP CONSTRAINT account_id_unique IF EXISTS;
