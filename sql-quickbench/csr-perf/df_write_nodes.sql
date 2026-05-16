-- csr-perf: write 3M vertex nodes.
-- Schema mirrors the gpu-stress-test demo so the topology is realistic
-- (power-law titles, 20 departments, 15 cities).

CREATE SCHEMA IF NOT EXISTS pbi.csr_bench;

DROP DELTA TABLE IF EXISTS pbi.csr_bench.csr_bench_nodes WITH FILES;

CREATE DELTA TABLE pbi.csr_bench.csr_bench_nodes (
    id          BIGINT  NOT NULL,
    department  STRING  NOT NULL,
    city        STRING  NOT NULL,
    title       STRING  NOT NULL,
    level       STRING  NOT NULL,
    active      BOOLEAN NOT NULL
) LOCATION 'B:/odbc_df/df-demo/csr-bench/csr_bench_nodes';

INSERT INTO pbi.csr_bench.csr_bench_nodes
SELECT
    id::BIGINT,
    CASE (id % 20)
        WHEN 0  THEN 'Engineering'       WHEN 1  THEN 'Marketing'
        WHEN 2  THEN 'HR'                WHEN 3  THEN 'Finance'
        WHEN 4  THEN 'Sales'             WHEN 5  THEN 'Operations'
        WHEN 6  THEN 'Legal'             WHEN 7  THEN 'Product'
        WHEN 8  THEN 'Data Science'      WHEN 9  THEN 'DevOps'
        WHEN 10 THEN 'Security'          WHEN 11 THEN 'Customer Support'
        WHEN 12 THEN 'Research'          WHEN 13 THEN 'Design'
        WHEN 14 THEN 'QA'                WHEN 15 THEN 'Platform'
        WHEN 16 THEN 'Infrastructure'    WHEN 17 THEN 'Analytics'
        WHEN 18 THEN 'Mobile'            WHEN 19 THEN 'AI/ML'
    END AS department,
    CASE (id % 15)
        WHEN 0  THEN 'NYC'        WHEN 1  THEN 'SF'
        WHEN 2  THEN 'Chicago'    WHEN 3  THEN 'London'
        WHEN 4  THEN 'Berlin'     WHEN 5  THEN 'Tokyo'
        WHEN 6  THEN 'Sydney'     WHEN 7  THEN 'Toronto'
        WHEN 8  THEN 'Singapore'  WHEN 9  THEN 'Dublin'
        WHEN 10 THEN 'Seattle'    WHEN 11 THEN 'Austin'
        WHEN 12 THEN 'Amsterdam'  WHEN 13 THEN 'Mumbai'
        WHEN 14 THEN 'Paris'
    END AS city,
    CASE
        WHEN id % 1000 = 0 THEN 'VP'
        WHEN id % 500  = 0 THEN 'Director'
        WHEN id % 100  = 0 THEN 'Senior Manager'
        WHEN id % 50   = 0 THEN 'Manager'
        WHEN id % 20   = 0 THEN 'Senior Engineer'
        WHEN id % 5    = 0 THEN 'Engineer'
        ELSE                    'Associate'
    END AS title,
    CASE
        WHEN id % 1000 = 0 THEN 'L8'
        WHEN id % 500  = 0 THEN 'L7'
        WHEN id % 100  = 0 THEN 'L6'
        WHEN id % 50   = 0 THEN 'L5'
        WHEN id % 20   = 0 THEN 'L4'
        WHEN id % 5    = 0 THEN 'L3'
        WHEN id % 3    = 0 THEN 'L2'
        ELSE                    'L1'
    END AS level,
    (id % 21 != 0) AS active
FROM generate_series(1, 3000000) AS t(id);

OPTIMIZE pbi.csr_bench.csr_bench_nodes ZORDER BY (id, department, level);

SELECT COUNT(*) AS nodes_written FROM pbi.csr_bench.csr_bench_nodes;
