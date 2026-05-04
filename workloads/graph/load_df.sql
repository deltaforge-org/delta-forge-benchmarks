-- DeltaForge load script for the graph_finance workload.
-- Reads the bench-generated parquet at {data_dir} and produces:
--   bench.fin.gfn_accounts       (DELTA TABLE, 10 columns)
--   bench.fin.gfn_transactions   (DELTA TABLE, 6 columns)
--   bench.fin.fin_graph          (GRAPH over both tables)
--   .dcsr sidecar for the graph (CSR topology, hot for the first query)
--
-- The {data_dir} placeholder is substituted by the runner. POSIX-style
-- forward slashes are emitted on every platform; DF accepts the file://
-- prefix uniformly.

CREATE ZONE IF NOT EXISTS bench TYPE EXTERNAL
    COMMENT 'Bench-managed external zone for the graph_finance workload';

CREATE SCHEMA IF NOT EXISTS bench.fin
    COMMENT 'Graph-finance benchmark dataset; reproducible from bench data_gen';

-- Account dimension: 10 columns.
CREATE DELTA TABLE IF NOT EXISTS bench.fin.gfn_accounts (
    id              BIGINT,
    name            STRING,
    bank            STRING,
    city            STRING,
    account_type    STRING,
    risk_tier       STRING,
    balance_band    STRING,
    kyc_level       STRING,
    open_year       INT,
    active          BOOLEAN
) LOCATION 'graph_finance/gfn_accounts';

INSERT INTO bench.fin.gfn_accounts
SELECT id, name, bank, city, account_type, risk_tier, balance_band,
       kyc_level, open_year, active
FROM read_parquet('file://{data_dir}/accounts.parquet');

-- Transaction (edge) fact table: 6 columns + the foreign-key pair (src,dst)
-- that the graph definition uses.
CREATE DELTA TABLE IF NOT EXISTS bench.fin.gfn_transactions (
    id                  BIGINT,
    src                 BIGINT,
    dst                 BIGINT,
    weight              DOUBLE,
    transaction_type    STRING,
    tx_year             INT
) LOCATION 'graph_finance/gfn_transactions';

INSERT INTO bench.fin.gfn_transactions
SELECT id, src, dst, weight, transaction_type, tx_year
FROM read_parquet('file://{data_dir}/transactions.parquet');

-- Z-order on (id, account_type, risk_tier) for the accounts table and on
-- (src, dst) for transactions. Same ordering as the demo so CSR build
-- reads sequentially from Parquet at first-query time.
OPTIMIZE bench.fin.gfn_accounts ZORDER BY (id, account_type, risk_tier);
OPTIMIZE bench.fin.gfn_transactions ZORDER BY (src, dst);

-- Graph definition. Single edge type (TRANSACTED) is implicit; the actual
-- transaction_type lives as an edge property so per-type filters use
-- WHERE r.transaction_type = '...' on both engines.
CREATE GRAPH IF NOT EXISTS bench.fin.fin_graph
    VERTEX TABLE bench.fin.gfn_accounts
        ID COLUMN id NODE TYPE COLUMN bank NODE NAME COLUMN name
    EDGE TABLE bench.fin.gfn_transactions
        SOURCE COLUMN src TARGET COLUMN dst
        WEIGHT COLUMN weight EDGE TYPE COLUMN transaction_type
    DIRECTED;

-- Materialise the CSR topology so the first measured query starts warm.
-- Equivalent to Neo4j's `CALL gds.graph.project(...)` setup phase below.
CREATE GRAPHCSR bench.fin.fin_graph;
