# Changelog

All notable changes to this benchmark suite are documented here.
Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

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
