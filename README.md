# delta-forge-benchmarks

A reproducible, scripted, single-host benchmark suite comparing **DeltaForge**
against **DuckDB** (with the read-only `delta` extension) and **Apache Spark**
(default + tuned) on the TPC-H workload, read against **plain Delta tables**
(no deletion vectors, no column mapping, no row tracking). Designed to
survive hostile reading: every input is deterministic, every artifact is
published, every configuration is auditable.

> **Status:** v0.1 in progress. The TPC-H Delta read chapter
> (`tpch_read_delta`) runs end-to-end across all three engines at SF=1 with
> zero failures; the SF=10 publish target is still pending a reference host.
> See `CHANGELOG.md` for what is in this checkout today, and the "Reproduced
> SF=1 numbers" section below for the current head-to-head.

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

## Reproduced SF=1 numbers (`tpch_read_delta`)

22 canonical TPC-H queries against plain Delta tables, 1 cold + 9 warm per
query, warm-median in milliseconds. Same on-disk Delta fixture for all three
engines. df numbers are server-reported `SHOW STATS ACTUAL.total_time_ms`
(plan + compile + execute + drain, table-attach excluded); DuckDB and Spark
numbers are wall-clock around the SELECT (their views are pre-registered in
untimed setup). Hardware: 8 cgroup CPUs / 16 GiB memory on a WSL2 host
backed by ext4 (read 1815 MB/s, write 740 MB/s).

| query  |       df  |  duckdb  | spark-default |
|--------|----------:|---------:|--------------:|
| q01    |    425.92 |   166.27 |       2874.65 |
| q02    |    127.37 |    88.36 |       1781.71 |
| q03    |    241.23 |   152.08 |       1487.62 |
| q04    |    172.71 |   129.73 |       1168.42 |
| q05    |    312.58 |   169.18 |       2180.74 |
| q06    |    107.70 |    90.25 |        332.50 |
| q07    |    255.28 |   189.86 |       1735.43 |
| q08    |    251.16 |   228.63 |       2019.68 |
| q09    |    368.09 |   294.45 |       2209.83 |
| q10    |    267.01 |   272.03 |       1991.91 |
| q11    |     80.39 |    86.51 |       1353.59 |
| q12    |    206.36 |   142.88 |        975.94 |
| q13    |    195.88 |   178.06 |       1159.62 |
| q14    |    108.01 |   124.14 |        606.29 |
| q15    |    139.08 |   115.55 |       1271.87 |
| q16    |    134.59 |   144.87 |       1234.12 |
| q17    |    230.06 |   174.15 |       1552.87 |
| q18    |    606.21 |   200.55 |       2573.95 |
| q19    |    172.15 |   186.22 |        621.91 |
| q20    |    158.91 |   179.26 |       1444.99 |
| q21    |    428.13 |   397.57 |       3194.64 |
| q22    |     94.54 |   131.29 |       1356.99 |
| median |    202.93 |   161.85 |       1486.19 |

**Headline.** On the warm-median across 22 queries, DeltaForge is **~1.25x
slower than DuckDB** (203 ms vs 162 ms) and **~7.3x faster than stock-OSS
Spark** (203 ms vs 1486 ms). df wins 7 of 22 against DuckDB (q10, q11, q14,
q16, q19, q20, q22, plus q11 effectively tied) and wins **22 of 22 against
Spark**. DuckDB's largest wins over df are on high-cardinality aggregations
(q18 3.02x, q01 2.56x) and complex multi-join plans (q05 1.85x, q03 1.59x);
df's largest wins over DuckDB are simple aggregations and small scans where
its planner overhead is the smaller fraction (q22 1.39x).

**The SF=1 caveat.** At SF=1, every engine finishes most queries in under
half a second; noise dominates ratios near 1.0x and engine architecture
differences only show up at the extremes. The SF=10 publish (under prep) is
where engine differences emerge cleanly; SF=1 is the smoke test that proves
the harness, the fixture, and the three engine paths are honest.

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

### Workload: TPC-H, plain Delta read (`tpch_read_delta`)

We use the canonical 22 TPC-H queries (`workloads/tpch/queries/q01.sql`
through `q22.sql`). Schema is the standard 8 tables (lineitem, orders,
customer, supplier, part, partsupp, nation, region).

The fixture is **plain Delta** (`data_gen/generate_tpch_delta.py`): each
table is written once into `/workspace/data/tpch_sf<N>_delta/<table>/`
with `delta.enableDeletionVectors = false`, `delta.columnMapping.mode =
'none'`, and `delta.enableRowTracking = false`. All three engines read the
same on-disk Delta files. The fixture is plain rather than the modern DV +
column-mapping default because DuckDB's read-only `delta` extension only
supports the plain protocol; the alternative was to drop DuckDB from the
comparison, which would have removed the most useful single-node baseline
for DeltaForge.

**Table attach is not part of the measured time.** Each engine attaches its
view of the Delta directory once, before any timed query, and pays no
attach cost during the measurement:

- **DuckDB**: `INSTALL delta; LOAD delta;` plus one
  `CREATE OR REPLACE VIEW <t> AS SELECT * FROM delta_scan('<path>')` per
  table, in the workload's untimed `setup_steps`.
- **Spark**: one
  `CREATE OR REPLACE TEMPORARY VIEW <t> USING delta OPTIONS (path '<path>')`
  per table, in `setup_steps`.
- **DeltaForge**: a per-query
  `OPEN DELTA TABLE '<path>' AS <t>` preamble is prepended to each
  measured SELECT (DeltaForge sessions are short-lived and `OPEN` is
  session-scoped, so the preamble runs once per CLI invocation alongside
  the query). The DF engine adapter splits the multi-statement script,
  runs the OPENs as untimed preamble, and wraps **only the final SELECT**
  with `SHOW STATS ACTUAL`. The `total_time_ms` value `SHOW STATS ACTUAL`
  returns is what the headline table reports for DF: it covers plan +
  compile + execute + drain for the SELECT, but excludes the OPENs.

**Why `SHOW STATS ACTUAL`, not bare `execution_time_ms`.** The CLI's plain
`execution_time_ms` field is measured at the handler return point, which
is before the partition-ticket drain finishes on the streaming path. On
SF=1 it under-reports the real query time by 3-5x. `SHOW STATS ACTUAL`
runs the same query under an instrumented executor that does not return
until the last batch is drained, so its `total_time_ms` is the honest
end-to-end execution time. Using bare `execution_time_ms` would have made
DeltaForge look 3-5x faster than it actually is, so we don't.

**SQL gap (documented).** DeltaForge today has no SQL command that
persistently registers an existing Delta directory in the catalog;
`REGISTER TABLE`, `OPEN DELTA TABLE`, and `CREATE DELTA TABLE IF NOT
EXISTS` all collapse to a session-scoped attach. The OPEN preamble pattern
is the bench's workaround. See [docs/bug/sql-gap-persistent-delta-attach.md](../docs/bug/sql-gap-persistent-delta-attach.md)
in the engine repo for the three proposed fix shapes; once one lands, the
preamble goes away and df setup becomes a single one-time DDL like the
other engines.

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
│   ├── duckdb_engine.py         # DuckDB with the read-only delta extension
│   ├── spark_default_engine.py  # Spark with stock-OSS defaults
│   ├── spark_tuned_engine.py    # Spark with AQE + tuned shuffle partitions + executor memory
│   ├── _spark_session.py        # vendored from delta-forge engine repo, pinned at DF_GIT_SHA
│   └── _purge.py                # explicit between-engine state purge
├── workloads/
│   ├── tpch_read_delta.py       # 22 TPC-H queries against plain Delta tables
│   └── tpch/queries/q01.sql ... q22.sql
├── data_gen/
│   └── generate_tpch_delta.py   # writes plain Delta (DV/CM/RT off) via Spark, one-time fixture
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

## Additional Delta-read drop-in workloads (`tpcds_read_delta`, `ssb_read_delta`, `job_read_delta`)

Three more standardized read workloads ship in the same shape as
`tpch_read_delta`: same plain-Delta protocol, same per-engine setup
(DuckDB views in setup, Spark views in setup, df OPEN preamble per
query), same `SHOW STATS ACTUAL` measurement contract. Each one has a
one-shot data-gen script under `data_gen/` and is published as a
canonical Delta fixture under `/workspace/data/`. Once the fixture is
generated, the workload runs the same way as TPC-H:

```bash
docker compose exec bench python bench_runner.py --scale 1 \
    --engines df,duckdb,spark-default --workloads tpcds_read_delta
```

| Workload | Standard | Queries | Tables | Fixture path | Generator |
| --- | --- | --- | --: | --- | --- |
| `tpcds_read_delta` | TPC-DS (TPC body) | 99 | 24 | `data/tpcds_sf{N}_delta/` | `data_gen/generate_tpcds_delta.py --scale N` |
| `ssb_read_delta` | SSB (O'Neil et al., 2009) | 13 | 5 | `data/ssb_sf{N}_delta/` | `data_gen/generate_ssb_delta.py --scale N` |
| `job_read_delta` | JOB (Leis et al., VLDB 2015) | 113 | 21 | `data/job_delta/` | `data_gen/generate_job_delta.py` |

### TPC-DS (`tpcds_read_delta`)

99 canonical TPC-DS queries on the 24-table snowflake. Data generated
by DuckDB's `tpcds` extension (in-process `dsdgen` call), exported to
parquet, then rewritten as plain Delta via Spark. The 99 queries are
the official TPC-DS templates instantiated at seed=0 (the upstream
DuckDB-bundled set), extracted to `workloads/tpcds/queries/q01.sql`
through `q99.sql` at setup time.

What TPC-DS adds over TPC-H: a much wider schema (24 tables vs 8),
window functions, ROLLUP / GROUPING SETS, semi-joins, and a far harder
join-order search space. The two together are the de-facto OLAP cover
set.

### SSB (`ssb_read_delta`)

13 canonical Star Schema Benchmark queries (O'Neil et al., 2009) on
the 5-table star: lineorder fact + date, part, supplier, customer
dimensions. The fixture is derived in SQL from the existing TPC-H
plain-Delta tables (no separate generator), following the canonical
TPC-H -> SSB mapping cited in the SSB paper, so SSB at SF=N reuses the
TPC-H SF=N fixture.

What SSB adds over TPC-H: the explicit denormalized star, which is
the BI-shape most warehouses serve, plus an aggregation-heavy query
mix (4 query flights, all sum / group-by patterns). Smallest of the
three drop-ins.

### JOB (`job_read_delta`)

113 real queries over a 21-table IMDB snapshot (Leis et al.,
"How Good Are Query Optimizers, Really?", VLDB 2015). Fixed-size
fixture (~3.6 GB unpacked, ~1 GB as plain Delta); no scale factor.
Data and queries both come from the JOB authors' canonical CWI
snapshot and the upstream MIT-licensed query set; the data-gen script
downloads `imdb.tgz` on first run, applies the schema via DuckDB,
exports to parquet, then Spark writes plain Delta.

What JOB adds over TPC-H and TPC-DS: it is **purpose-built to stress
query-optimizer cardinality estimation**. The 113 queries were chosen
specifically because they expose the gap between the cardinalities a
planner estimates and the cardinalities it actually encounters. This
is where df's planner most directly competes with DuckDB's and Spark's.

## Future chapters

**Scope filter.** Every workload in this benchmark reads from or writes
to **Delta tables**. Benchmarks that fundamentally cannot run on the
Delta format (graph databases against a native graph store, KV
workloads, OLTP against row stores) are out of scope for this suite. The
point is to measure DeltaForge against other engines reading and writing
the same Delta files, not against engines doing something different.

The current TPC-H Delta read chapter is the published v0.1 headline.
The TPC-DS / SSB / JOB drop-ins above are wired up but unpublished
(any user can generate and run them). The write-half (synthetic CTAS /
INSERT into Delta, df vs Spark) is the only near-term addition under
active work.

| When | Workload chapter | Notes |
| --- | --- | --- |
| Next | `tpch_write_delta_synthetic`: CTAS from `generate_series` / `range` into plain Delta, then INSERT-from-SELECT. df + Spark; DuckDB's delta extension is read-only and sits this out. | The other half of "Delta engine". Synthetic source so no input-format decoder lands in the headline. |
| If asked | **Delta-protocol-specific writes**: MERGE / SCD2, time-travel joins (`AS OF VERSION`), OPTIMIZE / Z-ORDER, VACUUM, CDC ingestion (`table_changes`). df vs Spark + Delta Lake. | No analogue on bare Parquet engines; this is where the DeltaForge writer differentiates from Spark + Delta Lake. |
| If asked | **BI / ODBC over Delta**: Power BI / Tableau / Excel query shapes. df ODBC driver vs Spark Thrift / Databricks SQL endpoint, same plain-Delta TPC-H fixture. | The ODBC driver is a first-class shipping artifact; this is where it gets measured against the same workloads BI tools actually issue. |

**Standards we considered and explicitly do not include:**

- **ClickBench raw fixture**: a single 14 GB compressed Parquet file is
  not a real-world layout. Its query shapes are interesting, but adopting
  them would require partitioning the data into a real Delta layout
  first, at which point we are running a different benchmark.
- **TPC-C, YCSB**: OLTP / KV workloads, not Delta access patterns.
  Running them would produce numbers that mislead more than they inform.
- **Graph benchmarks (LDBC SNB and similar)**: the comparator engines
  (Neo4j, Memgraph, TigerGraph) don't read Delta. Running them is
  comparing graph stores, not Delta engines, and belongs in a separate
  suite if at all.

## Contributing

PRs that improve methodology, tighten the Spark "tuned" config, add new
queries to existing chapters, or reproduce the v0.1 numbers on different
hardware are welcome. The bar is methodology, not advocacy.

## License

Apache License 2.0. See `LICENSE`.
