# Changelog

All notable changes to this benchmark suite are documented here.
Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Added (Delta-read drop-in workloads: TPC-DS, SSB, JOB)
- **`tpcds_read_delta` workload**: 99 canonical TPC-DS queries on the 24-table snowflake against plain Delta. Same shape as `tpch_read_delta` (per-engine attach paths, df OPEN preamble, `SHOW STATS ACTUAL` wrap). Queries are the official TPC-DS templates instantiated at seed=0, extracted from DuckDB's `tpcds` extension. Generator: `data_gen/generate_tpcds_delta.py`. Fixture: `data/tpcds_sf{N}_delta/`.
- **`ssb_read_delta` workload**: 13 Star Schema Benchmark queries (O'Neil et al., 2009) on a 5-table star. The fixture is derived in SQL from the existing TPC-H plain-Delta tables following the canonical TPC-H -> SSB mapping, so SSB at SF=N reuses the TPC-H SF=N fixture. Generator: `data_gen/generate_ssb_delta.py`. Fixture: `data/ssb_sf{N}_delta/`.
- **`job_read_delta` workload**: 113 Join Order Benchmark queries (Leis et al., VLDB 2015) on the 21-table IMDB snapshot. Fixed-size (no scale factor). Generator downloads `imdb.tgz` from the JOB authors' CWI mirror, applies the schema via DuckDB, exports to parquet, then Spark writes plain Delta. Queries committed under `workloads/job/queries/` from the upstream MIT-licensed `gregrahn/join-order-benchmark` repo. Generator: `data_gen/generate_job_delta.py`. Fixture: `data/job_delta/`.
- All three follow the same drop-in pattern as `tpch_read_delta` and need no bench_runner registry edit; they are discovered automatically.

### Removed
- **Neo4j / `graph_finance` chapter** dropped from the bench. The benchmark
  is scoped to engines reading and writing Delta tables; Neo4j cannot read
  Delta, so a head-to-head against it was comparing graph stores, not
  Delta engines, and belonged in a separate suite. Deleted files:
  `engines/neo4j_engine.py`, `workloads/graph_finance.py`,
  `data_gen/generate_graph_finance.py`, `workloads/graph/`. Stripped
  `neo4j` from `bench_runner.py`'s engine registry, the `_purge.py`
  cache-clear helper, `docker-compose.yml` + `docker-compose.override.yml`
  service definitions and env vars, and the `neo4j==5.26.0` line in
  `requirements.txt`. The README's "Graph chapter" section was removed
  and the future-chapters roadmap trimmed to Delta-only workloads.
- **Future-chapters list trimmed.** The roadmap previously listed v0.2
  through v0.9 chapters as if they were funded; replaced with a
  "Next / If asked" two-tier list and an explicit "considered and not
  included" note (ClickBench raw fixture, TPC-C, YCSB, graph benchmarks).

### Added (Graph chapter: DeltaForge vs Neo4j)
- **`graph_finance` workload** (`workloads/graph_finance.py`): 14 portable Cypher queries exercising MATCH expansion, WHERE filters, aggregations, and the five GDS algorithms (PageRank, WCC, Louvain, Triangle Count, Betweenness) against a 10M-account synthetic global-banking graph. Workload declares `applicable_engines = ('df', 'neo4j')`, so the runner skips it on Spark.
- **Neo4j engine adapter** (`engines/neo4j_engine.py`): Bolt client to the compose-managed neo4j service. Lifecycle probes `RETURN 1` until the server accepts connections; queries record wall-clock + `result_available_after + result_consumed_after` as the engine-reported time. Stochastic algorithms (Louvain, sampled betweenness, PageRank float-precision) are timed only; deterministic queries (counts, MATCH, WCC component-size distribution, triangle-count top-K) are hashed cross-engine for correctness validation. Memory metrics are not collected because the neo4j JVM lives in a different container.
- **DeltaForge engine adapter** (`engines/df_engine.py`): replaced the Phase-2 stub with a real implementation. Drives `delta-forge-cli --format json query` for both SQL and Cypher, parses the CLI's JSON output (`columns`, `rows`, `row_count`, `execution_time_ms`), splits multi-statement scripts, locates the worker PID for RSS/CPU sampling, and pulls `(<n>ms)` out of the DML success line so non-query steps still report engine time.
- **Graph data generator** (`data_gen/generate_graph_finance.py`): deterministic, parameterized by `--scale`. Reproduces the `graph-gpu-10m-finance` demo's exact 7-batch topology in DuckDB (10M nodes, 48,099,998 edges at scale=100), and emits parallel formats so DF and Neo4j load identical bytes: `accounts.parquet` / `transactions.parquet` for the Delta side and `accounts.csv` / `transactions.csv` with Neo4j-typed bulk-import headers (`:ID(Account)`, `:START_ID`, `:END_ID`, `:TYPE`) for `LOAD CSV` (or `neo4j-admin database import full` at the largest scales). Manifest records SHA-256 of every output.
- **Workload step contract extended**: new step kinds `STEP_CYPHER_QUERY` and `STEP_CYPHER_DML`; new step fields `per_engine_sql` (text override per engine name) and `per_engine_kind` (kind override per engine name). Lets a single logical step compile to DF SQL DDL on one engine and Cypher DML on another.
- **Workload routing knobs**: `Workload.applicable_engines` (skip a workload on engines that can't run it) and `Workload.data_subdir` (each workload picks its own staged-data path; defaults to `tpch_sf{scale}`, the graph workload uses `graph_finance_sf{scale}`).
- **Runner placeholder substitution** uses an explicit allowlist (`{data_dir}`, `{data_basename}`, `{scale}`) instead of `str.format()`, so Cypher map literals (`{key: value}`) do not collide with field syntax.
- **Compose stack**: `neo4j` service (Neo4j 5.26 Community + GDS Community plugin) added to `docker/docker-compose.yml`; bench container gets `NEO4J_BOLT_URL`, `NEO4J_USER`, `NEO4J_PASSWORD`, `NEO4J_DATABASE` env vars; the bench's `data/` is bind-mounted at `/var/lib/neo4j/import` so `LOAD CSV` reads the same generator output the DF side reads as parquet.
- **Cold-run path for Neo4j**: `engines/_purge.py::purge_neo4j_caches()` issues `CALL db.clearQueryCaches()` over Bolt for in-process cache invalidation. Documented limit: this does not touch the neo4j container's OS page cache; for a fully cold neo4j run, restart the neo4j compose service between iterations.
- **`neo4j==5.26.0`** added to `requirements.txt`.

### Added
- Repository skeleton (Phase 1).
- Single-image Docker layout: bench harness, DeltaForge engine binaries, PostgreSQL 15 (apt PGDG), and Spark 4.0 all in one container.
- Apt-installed PostgreSQL 15 inside the bench image, replacing the earlier embedded-pg plan. The container's entrypoint owns the Postgres lifecycle so first-run is deterministic.
- Bench entrypoint script (`docker/bench-entrypoint.sh`) implementing the headless bootstrap contract from `delta-forge-bootstrap/src/inputs.rs`: starts Postgres, creates role + DB, sets `DELTA_FORGE_DB_URL`/`ADMIN_PASSWORD`/`ENGINEER_PASSWORD`, starts `delta-forge-server`, waits for `:3000/health`, starts `delta-forge-worker`, then `exec`s the user CMD.
- Privileged `dropcaches` sidecar for OS page-cache flushing between cold runs.
- `bench_runner.py` skeleton with `--dry-run` mode that emits a planned-matrix manifest without launching engines.

### Changed
- Switched the public-distribution model: the image is intended for Docker Hub publication as `deltaforge/benchmarks:<tag>`. The README documents the pull-and-run flow as the primary path; building from this repo is the secondary path for unreleased engine commits.
- Adopted the SQLFlow publish pattern as the v0.1 path: local PowerShell (`docker-build.ps1`) using `docker buildx build --push` against the Docker Desktop credential cache, with `--attest type=provenance,mode=max --attest type=sbom`. No GitHub Actions secrets required for v0.1; the GHA workflow stays in the repo as the v0.2 path.
- Wired engine-binary acquisition to the deltaforge-org public release pipeline: `delta-forge-cli` and `delta-forge-worker` are downloaded from `github.com/deltaforge-org/delta-forge/releases/download/v${DF_VERSION}/` at image-build time, GPG-verified against the DeltaForge release key, and installed under `/usr/local/bin/`. The `delta-forge-server` (control plane) binary is source-built by the publish workflow from the same engine tag because it is not yet in the public release component set; that's a one-line engine change away from going pure-download.
- Dropped the dbgen-builder Docker stage. Data generation moves to DuckDB's built-in `tpch` extension (MIT-licensed, no registration), pending the data-gen rewrite.

### Added (Phase 3: hardware spec capture + scale-tier guidance)
- `engines/host_facts.py`: full hardware/OS snapshot embedded in every run's `manifest.json`. Captures CPU model + per-core scaling governor + ISA flags (AVX2/AVX-512/AES-NI/etc.), memory, filesystem + measured disk read/write throughput (256 MB cold-cache probe), virtualization (WSL2 / container / hypervisor), cgroup CPU + memory limits actually applied, OS / kernel / glibc, Python + Java versions, pinned package versions. Runnable standalone: `python -m engines.host_facts --short`.
- One-paragraph host summary printed at run start so reviewers see the host shape immediately.
- Scale-tier pre-flight checks in `bench_runner.py`: disk-free vs scale (errors at SF=100 if <400 GB free), RAM vs scale (warns at SF=10 if <16 GB available), spark-default OOM caveat at SF>=10, WSL2 disk-bottleneck note. `--force` overrides errors for "I really do want to push this host past its limits" runs.

### Changed (Phase 5: best-known Spark tunings)
- `engines/spark_tuned_engine.py` expanded from a 6-key tuning to a 22-key best-known-OSS profile, every key with a one-line rationale and a doc-link source. New additions:
  - **AQE skew handling** (`spark.sql.adaptive.skewJoin.*`): re-partitions skewed lineitem join keys mid-run.
  - **AQE local shuffle reader**: reduces stage count on broadcast joins.
  - **`spark.sql.autoBroadcastJoinThreshold=100MB`** (up from default 10MB): lets TPC-H dim tables broadcast naturally.
  - **Memory partitioning**: `memory.fraction=0.7`, `memory.storageFraction=0.3`, `memory.offHeap.enabled=true` with a 4 GB off-heap pool. Reduces GC pressure on shuffle-heavy plans.
  - **Parquet aggregate pushdown** (`spark.sql.parquet.aggregatePushdown=true`): pushes COUNT/MIN/MAX into the Parquet reader.
  - **Cost-based optimizer + join reorder + histograms** (`spark.sql.cbo.*`, `spark.sql.statistics.histogram.enabled`): reorders TPC-H multi-join queries (Q5, Q7, Q8, Q9) by computed selectivity.
  - **Kryo serializer**: Spark's official perf recommendation.
  - **Arrow PySpark path** (`spark.sql.execution.arrow.pyspark.enabled=true`): faster Python <-> JVM result transfer.
- Settings excluded on purpose: Databricks-runtime-only Delta keys (`spark.databricks.delta.optimizeWrite.enabled`, `spark.databricks.delta.autoCompact.enabled`) are silently ignored by OSS delta-spark and would mislead reviewers about what was actually tested.

### Added (Phase 4: Linux server runbook)
- `requirements.txt`: pinned Python deps mirroring the Dockerfile.
- `scripts/install.sh`: idempotent apt + venv + pinned-pip setup for a fresh Ubuntu/Debian server. Resolves and persists `JAVA_HOME`, prints host facts at the end.
- `scripts/run_smoke.sh`: 10-15 min sanity run at SF=1 with `spark-default`/`tpch_read`. Use immediately after install.sh.
- `scripts/run_bench.sh`: canonical SF=10 published-headline run. Generates data if absent, runs both Spark engines across all five workloads, tees output to a logfile, generates the report, tarballs the run dir for `scp` export. Args: `SCALE ENGINES WORKLOADS TAG` (all optional with sensible defaults).
- README "Running on a Linux server" section with the three-command flow + expected wall times + failure-mode taxonomy.

### Changed
- **SF=10 is the v0.1 published headline tier.** Earlier scale-tier framing positioned SF=100 as the headline; v0.1's reference host has 32 GB of RAM, so SF=100 (which needs ~96 GB) defers to v0.2 once a larger reference host is in place. SF=10 (60M-row lineitem, real shuffle pressure on 4 GB Spark driver) is plenty to answer the "do I need a Spark cluster?" question in v0.1.

### Added (Phase 2: actually runnable)
- Generalized engine ABC from `run_query` to `run_step` so write/CRUD workloads share the same protocol as reads. New types: `WorkloadStep` with kinds `SQL_QUERY/SQL_DML/SQL_DDL/MAINTENANCE/PYTHON`, `StepResult`, `ColdStartMetrics`.
- `engines/spark_default_engine.py` and `engines/spark_tuned_engine.py` are real implementations now: SparkSession lifecycle, wall-clock + Spark-reported plan time, psutil 100ms RSS/CPU sampling per step, canonical row-set hashing for cross-engine correctness validation.
- `engines/_metrics.py`: psutil-based sampler over the engine process tree (handles JVM children).
- `data_gen/generate_tpch.py` rewritten on DuckDB's `tpch` extension. SF=1 in 8.5s into 8 deterministic Parquet files with SHA-256 manifest. No registration with TPC, MIT-licensed.
- `workloads/tpch/queries/q01..q22.sql` extracted verbatim from DuckDB's `tpch_queries()`.
- `workloads/spec.py` framework: `Workload`, `WorkloadStep`, `StepRunRecord`, `WorkloadResult`, deterministic `hash_result_rows()` for cross-engine agreement, `discover()` for auto-loading workload modules.
- Five concrete workloads exercising read + write + CRUD + maintenance:
  - `tpch_read` (22 read queries)
  - `bulk_load` (8-table Parquet -> Delta load throughput)
  - `merge_cdc` (1% UPDATE + 1% DELETE via MERGE; CDC shape)
  - `update_delete` (UPDATE/DELETE at 1% / 10% / 50% selectivity)
  - `optimize` (file compaction after 8 small appends)
- `bench_runner.py` is a real runner now, not just `--dry-run`. Drives engines through cold-start + setup + measured (cold + warm) + cleanup, emits per-step JSONL records and a manifest.json with host/data/engine version facts.
- `reports/generate_report.py`: aggregates `raw/*.jsonl` into `summary.csv` (min/median/p95/mean/stddev per cell), writes `report.md` with run context + cold-start table + correctness disagreement section + per-workload result tables. Zero pandas/matplotlib dependency to keep the report auditable.

### Pending for v0.1.0
- (Engine repo) Add `server` to `DEFAULT_COMPONENTS` in `scripts/build-release.sh` so `delta-forge-server` ships in every release. Once landed, the bench publish workflow drops its source-build step.
- DeltaForge engine adapter (`engines/df_engine.py`): swap the stub for real subprocess driving against a running `delta-forge-server` + `delta-forge-worker` (started by `docker/bench-entrypoint.sh`).
- First end-to-end published run at SF=1 and SF=10 on the documented reference hardware.
- TPC-H data generator rewritten on top of DuckDB's `tpch` extension.
- Engine adapters: `df_engine.py`, `spark_default_engine.py`, `spark_tuned_engine.py`.
- All 22 TPC-H queries (`workloads/tpch/queries/q01.sql` through `q22.sql`), sourced from DuckDB's bundled `tpch_queries`.
- Cold/warm protocol, 100ms-interval system-metric sampler.
- Report generator (markdown, PNG, plotly HTML, honest-losses section).
- `.github/workflows/docker-publish.yml` (multi-platform build + push to Docker Hub on tag).
- First end-to-end run at scale factor 1 and scale factor 10 on the documented reference hardware.

## v0.1.0 (planned)

Initial public release.
- Workload: TPC-H scale factor 1 (1 GB) and scale factor 10 (10 GB), all 22 queries, 1 cold + 9 warm runs each.
- Engines compared: DeltaForge, Spark with stock-default config, Spark with tuned config.
- Reference hardware: documented in `README.md`.
- Honest-losses section: queries where DeltaForge ties or loses are listed in the executive summary, not buried.
