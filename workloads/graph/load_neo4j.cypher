// Neo4j load script for the graph_finance workload.
// Loads identical bytes to the DeltaForge side: the same accounts.csv and
// transactions.csv that data_gen/generate_graph_finance.py emits.
//
// At scale=1 (smoke, 100K nodes / 480K edges) LOAD CSV finishes in seconds.
// At scale=10 (1M / 4.8M edges) it takes a few minutes.
// At scale=100 (10M / 48M edges) the right tool is `neo4j-admin database
// import full` -- LOAD CSV would still work but is the wrong shape for
// that scale. The bench documents both paths in the README; this file
// runs the LOAD CSV path so the harness is self-contained out of the box.
//
// File path: the neo4j compose service mounts the bench's data/ root at
// /var/lib/neo4j/import inside the container, so file:///<...>/<file>.csv
// resolves to data/<...>/<file>.csv on the host. The {scale} placeholder
// is substituted by the bench runner's resolve_step at execution time.

// Idempotency: drop everything first so a re-run of the workload starts
// clean. MATCH (n) DETACH DELETE is the canonical "wipe the database"
// idiom on Neo4j Community.
MATCH (n) DETACH DELETE n;

// Constraint doubles as a primary-key index: lookups by Account.id (the
// pattern every load row relies on) become O(log N) instead of O(N).
CREATE CONSTRAINT account_id_unique IF NOT EXISTS
    FOR (a:Account) REQUIRE a.id IS UNIQUE;

// Account loader. PERIODIC COMMIT is implicit on CALL { ... } IN
// TRANSACTIONS in Neo4j 5; explicit batch size keeps memory bounded.
CALL {
    LOAD CSV WITH HEADERS FROM 'file:///graph_finance_sf{scale}/accounts.csv' AS row
    CREATE (:Account {
        id:           toInteger(row.`id:ID(Account)`),
        name:         row.name,
        bank:         row.bank,
        city:         row.city,
        account_type: row.account_type,
        risk_tier:    row.risk_tier,
        balance_band: row.balance_band,
        kyc_level:    row.kyc_level,
        open_year:    toInteger(row.`open_year:int`),
        active:       toBoolean(row.`active:boolean`)
    })
} IN TRANSACTIONS OF 50000 ROWS;

// Edge loader. Same batch shape; weight is parsed as float, tx_year as
// int, transaction_type as plain string. The :TYPE column is dropped
// because every row carries the same constant 'TRANSACTED' and we model
// rel-type as a single label with the actual transaction_type as a
// property (parity with the DeltaForge side).
CALL {
    LOAD CSV WITH HEADERS FROM 'file:///graph_finance_sf{scale}/transactions.csv' AS row
    MATCH (a:Account {id: toInteger(row.`:START_ID(Account)`)})
    MATCH (b:Account {id: toInteger(row.`:END_ID(Account)`)})
    CREATE (a)-[:TRANSACTED {
        id:               toInteger(row.`id:long`),
        weight:           toFloat(row.`weight:double`),
        transaction_type: row.transaction_type,
        tx_year:          toInteger(row.`tx_year:int`)
    }]->(b)
} IN TRANSACTIONS OF 25000 ROWS;

// GDS projection. The five algorithm queries below all stream from this
// projection; building it as a setup step keeps the projection cost out
// of the per-query measurement, matching the methodological choice we
// made for `CREATE GRAPHCSR` on DeltaForge above.
CALL gds.graph.drop('finance', false) YIELD graphName
RETURN count(graphName) AS dropped;

CALL gds.graph.project(
    'finance',
    'Account',
    'TRANSACTED',
    {relationshipProperties: 'weight'}
) YIELD graphName, nodeCount, relationshipCount
RETURN graphName, nodeCount, relationshipCount;
