# delta-forge-benchmarks

A reproducible, scripted, single-host benchmark suite comparing **DeltaForge**
against **Apache Spark** on the TPC-H workload. Designed to survive hostile
reading: every input is deterministic, every artifact is published, every
configuration is auditable.

> **Status:** v0.1 in progress. Phase 1 (skeleton + Docker stack) is done; data
> generation, engine adapters, and the first published run are still pending.
> See `CHANGELOG.md` for what is in this checkout today.

---

## What this is, and what it is not

**This is** an honest, reproducible head-to-head benchmark. The harness, the
data, the SQL, the engine versions, and the hardware are all pinned and
documented. You can clone this repo, run one command, and produce numbers on
your own machine that are directly comparable to the numbers we publish under
`results/`.

**This is not** a marketing benchmark. There is no cherry-picking. Queries
where DeltaForge ties or loses are reported in the executive summary, by
name, with the slowdown factor.

## Design invariants (non-negotiable)

These properties are baked into the harness. If we ever break one, that's a
release blocker.

1. **Scripted, deterministic data generation. No live streams.**
   All input data is produced by a deterministic script (`data_gen/generate_tpch.py`,
   which wraps the official TPC `dbgen`) into static Parquet files on disk
   before any engine starts. SHA-256 of every Parquet file is recorded in
   `manifest.json`. The benchmark never reads from a CDC feed, a Kafka topic,
   a network stream, or anything time-varying. Both engines read **identical
   bytes** from the same on-disk files.
2. **One engine runs at a time.** The harness never co-runs DeltaForge and
   Spark. Whichever engine is active gets the container's full CPU and memory
   budget.
3. **Identical sandbox.** Both engines run inside the same Docker image with
   the same `--cpus` and `--memory` limits. There is no Spark-specific
   privilege, mount, or network advantage that DeltaForge does not also have.
4. **Explicit state purge between engines.** Engine processes are killed,
   `/tmp` is cleared, and the host OS page cache is dropped (via the
   privileged `dropcaches` sidecar) before any cold run. Runs where the page
   cache could not be verified-cold are labeled `cold-os-cache=unverified`
   and excluded from the headline number.
5. **Two Spark baselines published.** "Stock OSS defaults" (the config a user
   gets from `pip install pyspark` with no extra tuning) and "Tuned" (AQE on,
   shuffle partitions sized to data, executor memory raised). Both are
   published side by side. The exact configuration of each is in
   `engines/spark_default_engine.py` and `engines/spark_tuned_engine.py`,
   reproduced verbatim in this README under "Spark configurations" once v0.1
   ships.
6. **Honest losses.** Every query result is reported. Queries where DeltaForge p95 > Spark p95 by more than 5% are listed in the executive summary,
   not buried in a table.
7. **Open license.** Apache 2.0. Anyone may run, modify, and republish
   results.

## Reproducing the benchmark

### Running on a Linux server (no Docker)

For a dedicated Linux server, the bench has three shell scripts that
together get you from "fresh Ubuntu/Debian install" to "tarballed results"
in a few commands:

```
git clone <bench-repo-url> delta-forge-benchmarks
cd delta-forge-benchmarks
./scripts/install.sh           # apt + venv + pinned pip; idempotent
./scripts/run_smoke.sh         # ~10-15 min; SF=1; sanity-checks the host
./scripts/run_bench.sh         # ~3-6 h; SF=10; the v0.1 published headline
```

Or one-shot it (skip the smoke if you trust the install):

```
./scripts/install.sh && ./scripts/run_bench.sh
```

| Script | Purpose | Wall time | What it produces |
|---|---|---|---|
| [install.sh](scripts/install.sh) | apt-installs JDK 17 + python venv tooling, pip-installs the pinned `requirements.txt`, writes `JAVA_HOME` to `.env`. Idempotent: a second run is a no-op. | ~2-5 min on a fresh server | `.venv/`, `.env` |
| [run_smoke.sh](scripts/run_smoke.sh) | Generates SF=1 TPC-H (if absent), runs `spark-default` on `tpch_read` only. Use this immediately after `install.sh` to catch host-config issues before burning hours. | ~10-15 min on a 16+ GB host | `results/<smoke-tag>/{manifest.json,summary.csv,report.md,raw/}` |
| [run_bench.sh](scripts/run_bench.sh) | Generates SF=10 TPC-H (if absent), runs both Spark engines across all five workloads (`tpch_read`, `bulk_load`, `merge_cdc`, `update_delete`, `optimize`). Tarballs the results dir for easy `scp` off-server. | ~3-6 h on a 16-32 GB host | `results/<bench-tag>/...` + `results/<bench-tag>.tar.gz` |

Args to `run_bench.sh` (all optional, defaults shown):
```
./scripts/run_bench.sh [SCALE] [ENGINES] [WORKLOADS] [TAG]
./scripts/run_bench.sh 10 spark-default,spark-tuned tpch_read,bulk_load,merge_cdc,update_delete,optimize bench-<utc>
```

Failures that are **expected and reported, not aborts**:
- `spark-default` OOMing on `lineitem` at SF=10 (4 GB driver heap is OOTB
  and genuinely too small for 60M-row joins). The harness records `exit_code != 0`
  for the affected steps and continues to the next workload.
- Any individual query failing on one engine. The report shows it as a
  failure and the cross-engine comparison degrades gracefully.

To export results from the server back to a workstation:
```
scp user@server:/path/to/delta-forge-benchmarks/results/<tag>.tar.gz .
tar -xzf <tag>.tar.gz
$EDITOR <tag>/report.md
```

### Hardware spec capture (automatic)

Every run records the host's hardware state into `manifest.json` automatically.
The harness prints a one-paragraph summary at run start so you can verify
the host shape immediately:

```
CPU:    Intel(R) Core(TM) i9-7980XE CPU @ 2.60GHz (18 physical / 36 threads,
        max 4400 MHz, governor=performance, ISA=aes,avx,avx2,avx512f,bmi2)
Memory: 31.2 GiB total
Cgroup: cpu=8 cores  memory=16384 MiB  (/sys/fs/cgroup/...)
Disk:   read 1810.28 MB/s  write 756.80 MB/s  (ext4 on /dev/nvme0n1p2)
Virt:   WSL2, VM (wsl)
```

Captured into `manifest.json` (schema documented in `engines/host_facts.py`):

- **CPU**: vendor, model, microcode, physical cores, logical threads,
  ISA flags (AVX2 / AVX-512 / AES-NI / SHA-NI / etc.), per-core scaling
  governor + driver, current/min/max frequency. The governor field alone
  determines whether a host benchmarks 2x slower than published numbers.
- **Memory**: `MemTotal`, `MemAvailable`, `Cached`, `HugePages_Total` from
  `/proc/meminfo`.
- **Disk**: filesystem of the bench data path, mount options, backing
  device (`lsblk` if available), and a *measured* sequential read +
  write throughput (256 MB cold-cache probe). Catches WSL2/9P slowdown
  that `/proc/cpuinfo` cannot see.
- **OS**: kernel + version + machine, `/etc/os-release` (distro + version),
  glibc version.
- **Virtualization**: WSL2, container (cgroup-based detection), hypervisor
  (CPU flag + `systemd-detect-virt`).
- **Cgroup limits**: actual `cpu.max` and `memory.max` applied to the bench
  process. Lets a reviewer confirm `docker --cpus 8 --memory 16g` was
  actually enforced.
- **Python + Java**: versions and resolved paths.
- **Pinned packages**: pyspark, delta-spark, duckdb, deltalake, psutil,
  pandas, matplotlib versions.

Sanity-check on a host without running the full bench:

```
python -m engines.host_facts --short                # one-paragraph
python -m engines.host_facts --data-path data/tpch_sf1   # full JSON
```

Reference hardware specs for any published `results/<tag>/` baseline are
in that run's `manifest.json` under `host`.

### Required tools

- Docker 24+ with Compose v2
- A Linux host kernel that allows `--privileged` containers for the
  `dropcaches` sidecar (otherwise the page-cache drop is skipped and runs
  are labeled `cold-os-cache=unverified`)
- Disk + RAM scaled to your chosen TPC-H scale factor (see "Scale tiers" below)

### Scale tiers

TPC-H scale factor is the headline knob.

| Tier | `--scale` | Parquet bytes | Lineitem rows | Disk free | RAM recommended | Wall time (3 engines x 22 q x 10 runs) | v0.1 status |
|---|--:|--:|--:|--:|--:|--:|---|
| smoke    |    1 |  ~1 GB  |   6.0M |   4 GB |   8 GB | ~30-45 min | runs |
| **standard** |   **10** | **~10 GB**  |  **60.0M** | **40 GB**  | **16 GB** | **~3-6 h** | **published headline** |
| at-scale |  100 | ~100 GB | 600.0M | 400 GB | 96 GB | ~16-30 h | v0.2 (needs reference host) |
| stress   | 1000 |  ~1 TB  |   6.0B |   4 TB | 512 GB | days       | future |

**`--scale 1` is for smoke testing.** It's the harness's "does the engine
run at all" gate. It is *not* a credible engine comparison; both engines
finish every query in seconds, so noise dominates the ratios.

**`--scale 10` is the v0.1 published headline.** At 10 GB, default-Spark's
4 GB driver heap is genuinely under pressure on lineitem joins; AQE tuning
matters; engine differences emerge. The 60M-row lineitem table is large
enough that joins shuffle real work, cold reads take real time, and the
"do I need a Spark cluster?" question becomes interesting in the right
direction. Reproducible on any 16+ GB workstation.

**`--scale 100` is the future at-scale tier**, deferred to v0.2. It needs
~96 GB RAM and ~400 GB disk; we publish it from a documented cloud or
dedicated reference host once one is in place. The interesting result
there is that stock-default Spark typically fails or spills heavily,
while tuned Spark and DF compete on realistic workloads. SF=10 is the
right v0.1 publish; SF=100 is the right v0.2 publish.

The harness pre-flight checks disk free and RAM at each tier and surfaces
warnings (or errors, with `--force` to override) before launching. Examples:

```
$ python bench_runner.py --scale 100 --engines spark-default,spark-tuned,df --dry-run
...
  warn: available memory 31.2 GB is below the recommended 96 GB for SF=100.
        spark-default (4 GB driver) is likely to OOM on lineitem.
        spark-tuned and df may still complete.
  note: spark-default at SF=100 uses the stock 4 GB driver heap. Expect
        warnings or OOM on lineitem. This is the documented baseline; compare
        against spark-tuned for the realistic Spark number.
```

**SF=10 and SF=100 are gated by disk-free and surface RAM warnings**, but
not by RAM hard-fail: an engine OOMing at a given scale is itself a data
point worth publishing. We label such runs `failed` in the report rather
than refusing to attempt them.

To generate data at any tier:
```
python data_gen/generate_tpch.py --scale 1     # ~9 sec
python data_gen/generate_tpch.py --scale 10    # ~90 sec
python data_gen/generate_tpch.py --scale 100   # ~15 min
```

### Public image: pull and run

The bench is published as a public Docker Hub image. Reviewers do not need
this repo's source to run it; the harness, the engines, and Postgres are all
inside the image. The repo is for reading methodology, auditing, and
contributing patches.

```bash
# 1. Pull a tagged release (or `latest`).
docker pull deltaforge/benchmarks:v0.1.0

# 2. Drop the .env file in your CWD and edit caps + secrets.
curl -fsSL https://raw.githubusercontent.com/deltaforge/delta-forge-benchmarks/v0.1.0/docker/.env.example \
    -o .env
$EDITOR .env

# 3. Run a smoke test (SF=1, query q01 only, 3 runs each engine).
docker run --rm -it \
    --name bench \
    --cpus="${BENCH_CPUS:-8}" --memory="${BENCH_MEMORY:-16g}" \
    --env-file .env \
    -v bench_data:/workspace/data \
    -v bench_pgdata:/var/lib/postgresql/data \
    -v $(pwd)/results:/workspace/results \
    deltaforge/benchmarks:v0.1.0 \
    python bench_runner.py --scale 1 --queries q01 --runs 3
```

For the full canonical run (3 engines × 22 queries × 10 runs at SF=1) plus
the cold-cache `dropcaches` sidecar, use Compose:

```bash
curl -fsSL https://raw.githubusercontent.com/deltaforge/delta-forge-benchmarks/v0.1.0/docker/docker-compose.yml \
    -o docker-compose.yml
docker compose --env-file .env up -d
docker compose exec bench python bench_runner.py --scale 1
docker compose exec bench python reports/generate_report.py --results-dir results/<timestamp>
```

Results land under `./results/<timestamp>-<host>/` on the host.

### Image tag policy

| Tag | Meaning |
|---|---|
| `vX.Y.Z` | Immutable release. Pin this for reproducible runs. The bench repo CHANGELOG lists what each tag changed. |
| `vX.Y` | Floating tag, points at the latest patch in that minor line. |
| `latest` | Floating tag, points at the most recent published release. Useful for "give me the newest", not for citations. |

Every tagged image is built deterministically by `.github/workflows/docker-publish.yml`
on a clean GitHub Actions runner. The manifest of the published image
includes:
- the bench repo commit SHA (`org.opencontainers.image.revision`)
- the DeltaForge engine commit SHA (`com.deltaforge.engine.revision`)
- the build date

You can confirm an image's provenance with `docker inspect deltaforge/benchmarks:vX.Y.Z`.

### How engine binaries reach the image

The Dockerfile uses two acquisition paths, both deterministic and tied to a
single `DF_VERSION` build-arg (a bare engine version like `0.5.2`):

1. **`delta-forge-cli` and `delta-forge-worker`** are downloaded from the
   public release at
   `https://github.com/deltaforge-org/delta-forge/releases/download/v${DF_VERSION}/`,
   specifically the `deltaforge-cli-${DF_VERSION}-linux-x64.tar.gz` and
   `deltaforge-compute-${DF_VERSION}-linux-x64.tar.gz` artifacts. Each
   tarball is GPG-verified at build time against the DeltaForge release
   public key bundled into the image.
2. **`delta-forge-server` (control plane)** is **not** in the public release
   set today (the `deltaforge-org/delta-forge` release components are
   `cli, mcp, compute`; there is no `server` component). The bench's publish
   workflow source-builds it from the engine repo at the same `v${DF_VERSION}`
   tag and stages it under `./build/df-bins/` for the Dockerfile to copy in.

When the engine adds `server` to its release components (a one-line change
to `delta-forge/scripts/build-release.sh:39`'s `DEFAULT_COMPONENTS`), the
bench's publish workflow drops the source-build step and downloads all three
binaries the same way. The image contract does not change.

### Building and publishing the image (PowerShell)

The supported publish path mirrors the SQLFlow build script: a local
PowerShell script that uses `docker buildx build --push` against a
Docker Hub login already cached by Docker Desktop. No CI secrets, no
out-of-band credentials.

**One-time setup**:

1. `docker login` (Docker Desktop handles credential storage).
2. `docker buildx create --name mybuilder --use` if `mybuilder` does not
   already exist on your machine.
3. Stage `delta-forge-server` under `.\build\df-bins\` (it is the one
   binary not yet in the deltaforge-org public release; see "How engine
   binaries reach the image" above):
   ```powershell
   cd <engine-repo>
   git checkout v0.5.2          # or whichever DF_VERSION you want
   cargo build --release -p delta-forge-control --bin delta-forge-server `
               --features "api,cloud-all"
   Copy-Item target\release\delta-forge-server <bench-repo>\build\df-bins\
   ```
4. Stage the DeltaForge release public key at
   `.\docker\deltaforge-release-key.asc`. Fetch it from
   `https://github.com/deltaforge-org/delta-forge/releases/download/v<DfVersion>/deltaforge-release-key.asc`.

**Publish**:

```powershell
.\docker-build.ps1 -DfVersion 0.5.2 -ImageTag v0.1.0 `
                   -DfGitSha (git -C <engine-repo> rev-parse v0.5.2) `
                   -DfGpgFingerprint <release-key-fingerprint> `
                   -Repo public
```

The script:
- Verifies the staged binary + GPG key are present and the `mybuilder`
  buildx builder exists.
- Builds with `--platform linux/amd64`, `--attest type=provenance,mode=max`,
  and `--attest type=sbom` (mirroring SQLFlow's flags).
- Pushes both `deltaforge/benchmarks:<ImageTag>` and `:latest`.
- Records engine repo, engine version, and engine commit as image labels
  so reviewers can verify which DeltaForge they are benchmarking.

For a build-only smoke test without pushing, add `-NoPush` (the script
substitutes `--load` so the resulting image lands in your local Docker
daemon).

### Building from this repo as a one-off (no publish)

If you only want to run the bench locally without publishing:

```powershell
.\docker-build.ps1 -DfVersion 0.5.2 -ImageTag local -NoPush
docker run --rm -it `
    -e DELTA_FORGE_ADMIN_PASSWORD=local `
    -e DELTA_FORGE_ENGINEER_PASSWORD=local `
    deltaforge/benchmarks:local `
    python bench_runner.py --scale 1 --queries q01 --runs 3
```

A locally-built image carries the same labels and behaves identically to
the published one, modulo the engine commit SHA the labels record.

### CI-driven publishing (deferred to v0.2)

`.github/workflows/docker-publish.yml` exists in the repo as a fully
wired alternative path: it source-builds `delta-forge-server` from the
engine repo at the matching tag, downloads the GPG release key, runs
`docker buildx build --push` against Docker Hub, and smoke-tests the
published image. It needs three secrets configured on this repo before
it will run: `DOCKERHUB_USERNAME`, `DOCKERHUB_TOKEN`, and
`DELTAFORGE_GPG_FINGERPRINT`. We treat it as v0.2 future work; the
local PowerShell flow above is the v0.1 path.

### What's inside the image

| Component | Version | License |
|---|---|---|
| Eclipse Temurin OpenJDK | 17 (LTS) | GPL-2.0 with Classpath Exception (redistributable) |
| Python | 3.11 (Ubuntu Jammy) | PSF |
| PostgreSQL | 15 (apt PGDG) | PostgreSQL License |
| PySpark | 4.0.0 | Apache 2.0 |
| delta-spark | 4.0.0 | Apache 2.0 |
| DuckDB (Python) | pinned in `Dockerfile` | MIT |
| deltalake (Python) | pinned in `Dockerfile` | Apache 2.0 |
| DeltaForge binaries | from build args; recorded as `DF_GIT_SHA` | DeltaForge Community License |

The image is **not** a runtime endorsed by the Apache Spark or Delta Lake
projects; it bundles their official PyPI distributions for benchmark
purposes and credits them under their original licenses. See `LICENSE` and
the `org.opencontainers.image.licenses` label on the published image.

### Trust posture (what the public image guarantees, and does not)

- **Guaranteed.** Bit-identical bytes for the bench harness, the Postgres
  install, the JDK, PySpark, delta-spark, DuckDB, and deltalake at the tag
  you pulled. SHA-256 of every published tag is in `CHANGELOG.md`.
- **Guaranteed.** Engine commit SHA recorded in image labels and in every
  run's `manifest.json`.
- **Not guaranteed.** Hardware. The bench numbers we publish under
  `results/v0.1.0-baseline/` were produced on the documented reference box;
  your numbers depend on your CPU, RAM, disk, kernel, and noisy neighbors.
- **Not guaranteed.** That the engine binaries inside this image are the
  most recent DeltaForge release. They are the engine commit listed in the
  tag's manifest. We publish a new bench image whenever the engine ships a
  release we want benchmarked.

## Methodology

### Workload: TPC-H

We use the canonical 22 TPC-H queries (`workloads/tpch/queries/q01.sql`
through `q22.sql`). Schema is the standard 8 tables (lineitem, orders,
customer, supplier, part, partsupp, nation, region). Both engines load
the same Parquet files into Delta-format tables. Engine-specific
optimizations (Z-order on DeltaForge, OPTIMIZE on Spark) are run if
the engine supports them, with the wall-clock time recorded in the
load-phase row of `manifest.json`.

### Run protocol

For each engine, for each scale factor, for each query:
- 1 cold run, after a full state purge (kill processes, drop OS page cache,
  restart engine, no warm-up).
- 9 consecutive warm runs in the same engine instance.
- The headline number is **median of warm**. p95 is reported alongside.
- Cold time is reported separately.

This is the same shape as the "1 cold + 9 warm" protocol used by most
public TPC-H reports.

### Statistical reporting

For each `(engine, query, scale)` triple, we publish:
- min, median, p95, mean, stddev (in milliseconds, wall clock)
- engine-reported time (when the engine returns one, e.g. `delta-forge-cli`'s
  `execution_time_ms`)
- peak resident memory (RSS) and average CPU% during the query, sampled
  at 100 ms intervals from `/proc/<pid>/status`

The headline cross-query number is the **geometric mean** of warm-median
ratios across all 22 queries, not the arithmetic mean (a single query that
runs in 50 ms must not dominate a query that runs in 50 s).

### Cold-cache definition

A "cold" run satisfies all of:
- the engine container's process tree was killed and restarted, so no JVM heap
  or in-process buffer pool survives;
- the host's `/proc/sys/vm/drop_caches` was written via the `dropcaches`
  sidecar, with stdout `DROPCACHES_OK` recorded in the run JSON;
- `/tmp` was cleared.

If any of these failed (e.g. the host disallows privileged containers and the
sidecar refused to start), the run JSON carries `purge_verified: false` and
`cold-os-cache: unverified`. Those records are still emitted but are excluded
from the cold-time aggregate.

## Repository layout

```
.
├── bench_runner.py              # main entry point
├── engines/
│   ├── df_engine.py             # DeltaForge: drives delta-forge-cli + control + worker
│   ├── spark_default_engine.py  # Spark with stock-OSS defaults
│   ├── spark_tuned_engine.py    # Spark with AQE + tuned shuffle partitions + executor memory
│   ├── _spark_session.py        # vendored from delta-forge engine repo, pinned at DF_GIT_SHA
│   └── _purge.py                # explicit between-engine state purge
├── workloads/tpch/
│   ├── schema.sql               # 8-table TPC-H DDL (Delta format)
│   ├── load.sql                 # COPY INTO from generated parquet
│   └── queries/q01.sql ... q22.sql
├── data_gen/
│   ├── generate_tpch.py         # dbgen wrapper, emits parquet via DuckDB
│   └── dbgen-src/               # vendored TPC-H dbgen (you populate this; see data_gen/README.md)
├── docker/
│   ├── Dockerfile               # single bundled image with both engines
│   ├── Dockerfile.dropcaches    # privileged sidecar (~10 lines, audit it yourself)
│   ├── dropcaches-handler.sh    # the only thing the sidecar runs
│   ├── docker-compose.yml
│   └── .env.example
├── reports/
│   ├── generate_report.py       # aggregate JSON, emit md + PNG + plotly HTML
│   └── template.md.j2
├── results/                     # per-run artifacts (gitignored except summary.csv)
├── README.md                    # this file
├── CHANGELOG.md
└── LICENSE                      # Apache 2.0
```

## Spark configurations

The exact `SparkSession.builder` call for each baseline is committed in
`engines/spark_default_engine.py` and `engines/spark_tuned_engine.py`. They
will be quoted verbatim here once v0.1 ships, so reviewers can audit without
opening source files. The "stock-defaults" config mirrors the default in
[delta-forge-demos/verify_lib/spark_session.py](https://github.com/) at the
pinned `DF_GIT_SHA`: `local[*]`, 4 GB driver, no AQE override.

## Graph chapter: DeltaForge vs Neo4j

The `graph_finance` workload is a head-to-head Cypher comparison against
**Neo4j Community 5.x + GDS Community**. It exists alongside the TPC-H
chapter; same harness, same statistical reporting, same correctness-hash
mechanism. It only runs on engines with a graph runtime (DeltaForge,
Neo4j); the runner skips it on the Spark adapters via the workload's
`applicable_engines` declaration.

### Dataset

A synthetic global-banking-network graph mirroring the
[`graph-gpu-10m-finance`](../delta-forge-demos/demos/graph/graph-gpu-10m-finance/)
demo, generated by `data_gen/generate_graph_finance.py`. The generator is
deterministic: re-running with the same `--scale` produces files with
identical SHA-256s, recorded in the per-run manifest.

| `--scale` | Nodes | Edges | Output size | Use |
|---:|---:|---:|---:|---|
| 1 | 100 000 | ~480 000 | ~70 MB | smoke / CI |
| 10 | 1 000 000 | ~4 800 000 | ~700 MB | laptop standard (v0.1 graph headline) |
| 100 | 10 000 000 | 48 099 998 | ~7 GB | demo-equivalent (matches the DF demo bit for bit) |

Each scale produces four files in `data/graph_finance_sf<scale>/`:
`accounts.parquet`, `transactions.parquet` (read by the DeltaForge side
through Delta tables) and `accounts.csv`, `transactions.csv` with Neo4j
bulk-import-style typed headers (`:ID(Account)`, `:START_ID`, `:END_ID`,
`:TYPE`). Both engines see the same row counts and the same per-row
values; the only column where the bench's data diverges from the demo's
is the `name` column, because the demo uses Rust's `fake-rs` corpus and
the bench uses a deterministic 100-name cyclic list (cross-language
reproducibility wins over name realism).

### Queries

Fourteen portable Cypher queries in `workloads/graph_finance.py`. Each
step carries a `per_engine_sql` map so the same logical query becomes:

| ID | Description | Determinism |
|---|---|---|
| q01 | Total node count | deterministic |
| q02 | Total edge count | deterministic |
| q03 | Account count per bank (30 rows) | deterministic |
| q04 | Edges where `transaction_type = 'advisory'` | deterministic |
| q05 | Top 30 cross-bank pair counts + avg weight | deterministic |
| q06 | Top 25 JPMorgan->JPMorgan edges by weight | deterministic |
| q07 | Edge `transaction_type` distribution (18 rows) | deterministic |
| q08 | Subgraph extraction: edges with both endpoint ids ≤ 100 | deterministic |
| q09 | Per-bank count of `risk_tier = 'high'` accounts | deterministic |
| q10 | GDS PageRank, top 25 by score (20 iterations) | timing-only (FP precision) |
| q11 | GDS WCC component-size distribution | deterministic (sizes invariant) |
| q12 | GDS Louvain community sizes | timing-only (stochastic) |
| q13 | GDS Triangle Count, top 25 nodes | deterministic |
| q14 | GDS Betweenness Centrality (sampled), top 25 | timing-only (stochastic) |

Deterministic queries are hashed cross-engine: a digest mismatch is a
release blocker, same as the TPC-H chapter. Timing-only queries run on
both engines and report wall-clock + engine-reported milliseconds, but
their result rows are not compared because the algorithm is intrinsically
non-comparable across engines (floating-point summation order, sample
selection, etc.).

The DeltaForge variants run **CPU-only** (no `ON GPU` hint); Neo4j has no
GPU path, so CPU-vs-CPU is the apples-to-apples comparison. A separate
GPU variant of the same workload is future work.

### Methodological choices

- **GDS projection / `CREATE GRAPHCSR` is a setup step, not a measured
  step.** Both engines pay this cost once before the timed queries
  begin. Measuring it inside the per-query latency would inflate the
  first-query number on both sides and obscure steady-state performance.
- **Cold-run cache invalidation on Neo4j is partial.** The bench issues
  `CALL db.clearQueryCaches()` over Bolt before each cold run, which
  flushes the plan and result caches but not the OS page cache for files
  on the neo4j container's volume. For a fully cold Neo4j run, run
  `docker compose restart neo4j` between iterations; otherwise the
  bench labels the runs `purge_verified=False` for that engine and the
  report excludes them from the cold-time aggregate.
- **Neo4j memory metrics are not collected.** The JVM runs in a
  separate compose container and is not visible to `psutil` from the
  bench container's view of `/proc`. The wall-clock and engine-reported
  millis are the comparable numbers; for an RSS reading on the Neo4j
  side use `docker stats bench-neo4j` during the run.
- **Single relationship type (`:TRANSACTED`).** The 18 distinct
  transaction types from the source graph live as a property
  (`r.transaction_type`) on both engines, exactly as the DF demo models
  them. This keeps the GDS projection a one-liner (`gds.graph.project(...)`
  with one rel type) rather than fanning out into 18 projections.

### Reproducing the graph chapter

Generate the data, then run the bench scoped to the graph workload and
the two engines that can run it:

```
python data_gen/generate_graph_finance.py --scale 10
python bench_runner.py --scale 10 --engines df,neo4j --workloads graph_finance
```

Or in the compose stack:

```
docker compose --env-file .env up -d
docker compose exec bench python data_gen/generate_graph_finance.py --scale 10
docker compose exec bench python bench_runner.py \
    --scale 10 --engines df,neo4j --workloads graph_finance
```

Recommended Neo4j memory at each scale tier (set in `.env` before
`docker compose up -d`):

| `--scale` | `NEO4J_HEAP` | `NEO4J_PAGECACHE` | Notes |
|---:|---:|---:|---|
| 1 | 1G | 1G | smoke; default values are oversized but harmless |
| 10 | 4G | 4G | matches the compose default |
| 100 | 8G | 16G | bump host RAM to 32 GB+; LOAD CSV at 48M edges takes ~1 hour |

For scale=100 (the demo-equivalent 10M/48M graph), prefer the offline
bulk loader (`neo4j-admin database import full --nodes=Account=accounts.csv
--relationships=transactions.csv`) over LOAD CSV; skip the bench's load
step by passing `--workloads graph_finance` after pre-loading and
projecting the graph yourself.

## What v0.1 does not cover

Each item below is a planned future release, not a permanent gap. Adding a
chapter is cheap; adding it badly is expensive, so we ship them one at a
time once the methodology of the previous chapter has survived independent
reproduction.

| Release | Workload chapter |
|---|---|
| v0.2 | TPC-DS (99 queries) at SF=1 and SF=10 |
| v0.3 | Real-world workloads: MERGE / SCD2, time-travel joins, OPTIMIZE / VACUUM, CDC ingestion |
| v0.4 | BI / ODBC workloads (Power BI and Tableau-shape queries, DirectQuery refresh times) |
| v0.5 | Concurrency: multiple simultaneous clients, tail-latency under load |
| v0.6 | Cloud-instance reference target (in addition to local) |

## Contributing

PRs that improve methodology, tighten the Spark "tuned" config, add new
queries to existing chapters, or reproduce the v0.1 numbers on different
hardware are welcome. The bar is methodology, not advocacy.

## License

Apache License 2.0. See `LICENSE`.
