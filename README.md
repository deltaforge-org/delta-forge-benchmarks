# delta-forge-benchmarks

Reproducible, scripted, single-host benchmark suite. Five workloads,
four engines, the **same plain Delta tables** on the **same hardware**:

- **TPC-H** — 22 queries, 8 tables
- **TPC-DS** — 99 queries, 24 tables
- **SSB** — 13 queries, 5-table star
- **JOB** — 113 queries, 21-table IMDB snapshot
- **Synthetic writes** — 10M-row CTAS from deterministic in-memory generators

Engines: **DeltaForge** vs **DuckDB** (read-only `delta` extension) vs
**Spark default** vs **Spark tuned**.

## Results at a glance (SF=1)

Warm-median across the queries in each benchmark, in milliseconds.
Smaller is faster. df numbers are server-reported
`SHOW STATS ACTUAL.total_time_ms`; DuckDB and Spark numbers are wall
around the SELECT (views pre-registered in untimed setup). Full per-query
tables on each per-benchmark page linked below.

| Benchmark | df (ms) | DuckDB (ms) | Spark default (ms) | Spark tuned (ms) | Detail |
| --- | ---: | ---: | ---: | ---: | --- |
| [TPC-H](published/tpch.md) | **255** | **173** | 1,478 | 1,528 | 22 queries |
| [TPC-DS](published/tpcds.md) | **271** | **171** | 1,568 (8 fail) | 1,464 | 99 queries |
| [SSB](published/ssb.md) | **191** | **75** | 685 | 628 | 13 queries |
| [JOB](published/job.md) | **976** | **632** | crashed | crashed | 113 queries |

DuckDB wins every read at SF=1 (1.5x-2.5x faster than df). df beats both
Spark profiles by 5x-8x on every read. **JOB exposed a Spark stability
limit**: Spark default's JVM crashed after q06d (21 of 113 queries
completed) and Spark tuned failed to start on the JOB engine. We do
not publish a median for the partial Spark runs because the 21
successful queries are an unrepresentative early subset, not a
random sample. df and DuckDB completed all 113.

### Write throughput (10M rows, plain Delta CTAS, synthetic source)

| Engine | warm-median (ms) | rows/sec | vs df |
| --- | ---: | ---: | ---: |
| **[DeltaForge](published/writes.md)** | **1,542** | **6.48 M** | 1.00x |
| Spark default | 6,640 | 1.51 M | 0.23x |
| Spark tuned | 6,228 | 1.61 M | 0.25x |

**df writes Delta tables ~4x faster than Spark on single-node.** DuckDB
sits this out (its `delta` extension is read-only).

[All results pages →](published/index.md) ·
[Methodology →](published/methodology.md)

## Run it

One line sets it up on any supported OS; one line runs it. Everything comes from
official **signed releases**: nothing is built from source, there is no first-run
wizard, and Docker is not required. The platform boots fully **headless and
unattended** (it bootstraps an embedded database and an embedded compute node and
activates your license on first start), so the exact same commands work on a
laptop, a CI runner, or a headless server.

### 1. Set up (one line)

Downloads the signed DeltaForge platform + CLI and sets up the comparison engines.

**macOS / Linux**

```bash
curl -fsSL https://deltaforge.org/bench/install.sh | bash
```

**Windows (PowerShell)**

```powershell
irm https://deltaforge.org/bench/install.ps1 | iex
```

The installer checks your machine first (OS, CPU, Python, disk, free port) and,
if anything is missing, tells you exactly what and how to fix it. DeltaForge
needs a license key to run the engine, so the installer asks for one (free at
[console.deltaforge.org](https://console.deltaforge.org); takes a minute).

The platform it installs is the official signed `deltaforge` release, the same
binary the desktop app ships. The benchmark boots it with no browser wizard and
no manual activation: an embedded PostgreSQL and an embedded compute node come up
in-process and the device activates online on first start. Nothing is compiled,
on any OS.

### 2. Run

```bash
cd delta-forge-benchmarks
./bench                 # quick SF=1 pass across all four engines
./bench --scale 10      # the standard headline tier
```

On Windows use `.\bench.ps1` (e.g. `.\bench.ps1 -Scale 10`); it boots the
platform headless exactly like `./bench` does on macOS / Linux, with no window
and no prompts.

`./bench` starts DeltaForge headless, waits until it is ready, runs the queries
on every engine, and shuts the platform (and its embedded database) down when
finished. Once a license key is in `.env`, no step asks you anything.

### 3. Read the results

Each run is saved under `results/<timestamp>-<host>-<tag>/` with per-query
timings, host facts, and the exact engine versions.

---

**License.** DeltaForge needs a license key to run the engine, and the benchmark
does not bundle one: you bring your own. It is free and takes a minute, no credit
card, at [console.deltaforge.org](https://console.deltaforge.org). The installer
prompts for it interactively; if you would rather not be prompted (or are piping
the installer in), set it up front:

```bash
DELTA_FORGE_LICENSE_KEY=<your-key> curl -fsSL https://deltaforge.org/bench/install.sh | bash
```

The key is written into `.env`; `./bench` refuses to start until one is present,
and a key that the engine rejects (expired, wrong, or out of daily compute) stops
the run early with a one-line message rather than failing query by query.

**You need** a 64-bit Linux (x64), macOS (Apple Silicon or Intel), or Windows
(x64) PC, Python 3.9+, and an internet connection for the one-time download.
Bigger scales need more disk and RAM (SF=1 ≈ 1 GB). Full details:
[docs/setup.md](docs/setup.md).

## Hardware (these numbers)

```text
Workstation:       Intel Core i9-7980XE @ 2.60 GHz (18 physical / 36 threads, 32 GiB RAM)
Host OS:           Microsoft Windows 11 Pro, build 26100
Virtualization:    Hyper-V -> WSL2 (Ubuntu) -> Docker Desktop (containerd)
Container OS:      Ubuntu 22.04.5 LTS (kernel 6.6.87.2-microsoft-standard-WSL2)
Container cgroup:  cpu=8 cores, memory=16384 MiB
Bench data path:   ext4 on /dev/sde (Docker Desktop virtual disk inside the WSL2 VM)
```

A native Linux host with a passthrough NVMe drive would produce
1.5-2x higher absolute throughput; **engine-to-engine ratios travel
across hardware reasonably well, absolute milliseconds do not.**

## Repository layout

```text
.
├── install.sh / install.ps1        # one-command setup (macOS+Linux / Windows)
├── bench / bench.ps1               # launcher: boot platform → run → tear down
├── README.md                       # this file (at-a-glance results)
├── docs/
│   ├── setup.md                    # install, run, scale tiers, hardware capture
│   └── design.md                   # design invariants, scope filter, future chapters
├── published/                      # marketing-linkable, per-bench markdown
│   ├── index.md                    # TOC
│   ├── methodology.md              # measurement contract
│   ├── tpch.md, tpcds.md, ssb.md, job.md, writes.md
├── bench_runner.py                 # the harness (invoked by ./bench)
├── engines/
│   ├── df_engine.py                # DeltaForge: drives deltaforge-cli against the platform
│   ├── duckdb_engine.py            # DuckDB with the read-only delta extension
│   ├── spark_default_engine.py     # Spark stock-defaults baseline
│   ├── spark_tuned_engine.py       # Spark tuned (~40 keys, every key rationalized)
│   └── _spark_session.py, _purge.py, host_facts.py
├── workloads/
│   ├── tpch_read_delta.py          # 22 TPC-H queries on plain Delta
│   ├── tpcds_read_delta.py         # 99 TPC-DS queries
│   ├── ssb_read_delta.py           # 13 SSB queries
│   ├── job_read_delta.py           # 113 JOB queries
│   ├── synthetic_write_delta.py    # 10M-row CTAS from synthetic source
│   └── tpch/, tpcds/, ssb/, job/ queries/*.sql
├── data_gen/                       # per-benchmark fixture generators
└── reports/                        # JSONL -> summary + publish markdown
```

The only DeltaForge artifacts the benchmark needs are the **platform** and the
**CLI**, both pulled from the official signed release. The platform embeds the
control plane and the compute node in a single process, so there is no separate
worker, no Postgres, and no Docker to manage.

## About DeltaForge

This repository benchmarks the DeltaForge engine. Background on the
engine, table-format support, BI drivers, and how single-node vs.
distributed execution decisions get made lives on deltaforge.org:

| Topic                            | Where                                                                                                |
| -------------------------------- | ---------------------------------------------------------------------------------------------------- |
| Engine overview                  | [deltaforge.org](https://deltaforge.org)                                                             |
| Why a native engine              | [pages/why.html](https://deltaforge.org/pages/why.html)                                              |
| Architecture                     | [pages/architecture.html](https://deltaforge.org/pages/architecture.html)                            |
| SQL engine                       | [pages/sql-engine.html](https://deltaforge.org/pages/sql-engine.html)                                |
| Vectorized compute               | [pages/compute-engine.html](https://deltaforge.org/pages/compute-engine.html)                        |
| Delta Lake table format          | [pages/table-format.html](https://deltaforge.org/pages/table-format.html)                            |
| Apache Iceberg + UniForm         | [pages/iceberg.html](https://deltaforge.org/pages/iceberg.html)                                      |
| ODBC driver                      | [pages/odbc.html](https://deltaforge.org/pages/odbc.html)                                            |
| ADBC driver + Power BI           | [pages/adbc.html](https://deltaforge.org/pages/adbc.html)                                            |
| BI driver micro-benchmark        | [pages/benchmarks-bi-drivers.html](https://deltaforge.org/pages/benchmarks-bi-drivers.html)          |
| Conformance against Apache Spark | [pages/conformance.html](https://deltaforge.org/pages/conformance.html)                              |
| Install                          | [pages/install.html](https://deltaforge.org/pages/install.html)                                      |
| Pricing                          | [pages/pricing.html](https://deltaforge.org/pages/pricing.html)                                      |
| Contact                          | [pages/contact.html](https://deltaforge.org/pages/contact.html)                                      |

If you reproduce the numbers in this repo, cite the deltaforge.org
page that matches the workload you measured plus the Git SHA recorded
in your run manifest.

## License

Apache License 2.0. See [`LICENSE`](LICENSE).

This repo bundles official PyPI distributions of Apache Spark, Delta
Lake, and DuckDB for benchmark purposes; each is credited under its
own license. The image is **not** a runtime endorsed by those
projects.
