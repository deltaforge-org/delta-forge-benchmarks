# Methodology — DeltaForge benchmark suite

The published numbers in this directory are produced by
[`delta-forge-benchmarks/`](../). This page documents what each
number means and what it does not.

Related pages: [Index](index.md) — [TPC-H](tpch.md) — [TPC-DS](tpcds.md) — [SSB](ssb.md) — [JOB](job.md) — [Writes](writes.md)

## What the suite measures

Four standardized read benchmarks against the same on-disk **plain
Delta** tables (no deletion vectors, no column mapping, no row tracking)
on the same hardware:

| Benchmark | Standard | Queries | Tables |
| --- | --- | --: | --: |
| [TPC-H](tpch.md) | TPC body | 22 | 8 |
| [TPC-DS](tpcds.md) | TPC body | 99 | 24 |
| [SSB](ssb.md) | O'Neil et al. 2009 | 13 | 5 |
| [JOB](job.md) | Leis et al. VLDB 2015 | 113 | 21 |

## Engines under test

| Engine | Version | Mode | Reading Delta via |
| --- | --- | --- | --- |
| **DeltaForge** | DeltaForge release in image manifest | server + worker (single host) | native `OPEN DELTA TABLE` |
| **DuckDB** | as pinned in `requirements.txt` | in-process Python | `delta` extension (`delta_scan(...)`) |
| **Spark (default)** | PySpark 4.0.0 + delta-spark 4.0.0 | `local[*]`, 4 GB driver, no AQE override | `USING delta OPTIONS (path '...')` |
| **Spark (tuned)** | same | `local[*]`, 8 GB driver + 4 GB off-heap, AQE / DPP / runtime bloom filter / ANSI / CBO on | `USING delta OPTIONS (path '...')` |

The full spark-tuned config is committed verbatim in [`engines/spark_tuned_engine.py`](../engines/spark_tuned_engine.py)
with a one-line rationale per setting.

## Fixture protocol — "plain Delta"

Every benchmark reads from Delta tables written with all advanced
features explicitly **disabled**:

- `delta.enableDeletionVectors = false`
- `delta.columnMapping.mode = none`
- `delta.enableRowTracking = false`

This is the same baseline Delta protocol DuckDB's read-only delta
extension supports. Without this constraint, DuckDB would drop out and
the comparison would collapse to df vs Spark only.

## Run protocol — "1 cold + 9 warm"

For every (engine, query) pair the suite runs **1 cold + 9 warm**
executions. Warm runs are inside the same engine instance, so the
engine sees the second-and-later run against a warm metadata cache and
warm JIT.

The published headline is the **warm-median** of the 9 warm runs. p95
and cold time are recorded in the per-run JSONL but not the headline.

This matches the standard reporting shape used by most public TPC-H
and TPC-DS publications.

## Measurement contract (per engine)

This is where most "Spark is X times slower" benchmarks lose
credibility. The contract:

- **DeltaForge:** the per-query SQL prepends an `OPEN DELTA TABLE
  '<path>' AS <name>` statement for every table the query needs, then
  the SELECT. The df engine adapter wraps **only the final SELECT** in
  a `SHOW STATS ACTUAL` envelope and reports its server-side
  `total_time_ms` (plan + compile + execute + drain) as df's number.
  The OPEN preamble runs in the same CLI invocation but is
  **excluded** from the published time.

- **DuckDB:** views are registered once in untimed setup
  (`INSTALL delta; LOAD delta; CREATE OR REPLACE VIEW <t> AS SELECT *
  FROM delta_scan('<path>')`). The measured timer is wall-clock around
  `con.execute(...).fetchall()`. DuckDB runs in-process so wall equals
  server time; nothing to exclude.

- **Spark (default and tuned):** temp views are registered once in
  untimed setup (`CREATE OR REPLACE TEMPORARY VIEW <t> USING delta
  OPTIONS (path '<path>')`). The measured timer is wall-clock around
  `spark.sql(...).collect()`. PySpark talks to the JVM via py4j
  in-process so wall equals server time.

In all four cases the measured value answers the same question:
**"how long does the engine spend executing this SELECT?"** The
table-attach cost is paid by every engine in untimed setup, so it does
not appear in any headline.

### Why not bare `execution_time_ms` for df?

DeltaForge's CLI returns an `execution_time_ms` field on its query
response. That value is captured at the handler-return point, which is
before the partition-ticket drain finishes on the streaming path. At
SF=1 it under-reports real execution time by 3-5x. Using it would have
made df look ~3-5x faster than it actually is. The bench explicitly
discards it and uses the instrumented `SHOW STATS ACTUAL.total_time_ms`
instead.

## What is **not** captured

- **CLI-to-server HTTP round-trip cost for df.** The published df
  number is server-side. A user calling df from a remote CLI would
  also pay ~80-90 ms HTTP round-trip on top. The in-process engines
  (DuckDB, Spark) have no remote round-trip, so excluding df's makes
  the comparison apples-to-apples.

- **JVM cold-start.** Spark's first invocation per benchmark pays a
  ~7-10 second JVM + delta-spark Ivy resolution cost. That cost is
  reported separately as `cold_start.ready_ms` and `first_query_ms` and
  is not folded into the per-query warm-median.

- **OS page cache state.** The bench currently runs with `--no-purge`
  on this WSL2 host because Docker Desktop on Windows cannot back the
  privileged page-cache-drop sidecar; runs are flagged
  `cold-os-cache=unverified`. Cold timings on this host should be read
  as warmer-than-truly-cold; warm timings are unaffected.

- **Anything off-host.** All four benches run on a single Linux
  container with 8 cgroup CPUs and 16 GiB memory. No cluster, no
  network shuffles.

## Hardware (this publish)

Captured by the harness into each run's `manifest.json`. The published
table sits on:

```text
Workstation:       Intel Core i9-7980XE @ 2.60 GHz (18 physical / 36 threads, 32 GiB RAM)
Host OS:           Microsoft Windows 11 Pro, build 26100
Virtualization:    Hyper-V → WSL2 (Ubuntu) → Docker Desktop (containerd)
Container OS:      Ubuntu 22.04.5 LTS (kernel 6.6.87.2-microsoft-standard-WSL2)
Container cgroup:  cpu=8 cores, memory=16384 MiB
Bench data path:   ext4 on /dev/sde (Docker Desktop virtual disk inside the WSL2 VM)
Disk throughput:   read ~1.3-2.1 GB/s, write ~470-750 MB/s (measured per-run)
```

The bench data lives on the WSL2-internal ext4 disk, not on the 9P-mounted
Windows path. The 9P bridge is what backs `/workspace` (the source-tree
mount); query latency is unaffected because the read benchmarks open
Delta files from the ext4 path.

**Why this matters for honest numbers:**

- This is a developer workstation under a Hyper-V + WSL2 + containerd
  stack, not a tuned bench server. Absolute numbers reflect that.
- A native Linux host (no Hyper-V, no WSL2) with a passthrough NVMe
  drive would likely produce 1.5-2x higher absolute throughput on the
  disk-bound queries.
- Engine-to-engine **ratios** travel across hardware reasonably well;
  the absolute milliseconds do not.
- The bench captures everything (CPU model, microcode, governor,
  cgroup limits, disk probe results, virt detection) into each run's
  `manifest.json` so reviewers can spot any apples-to-oranges drift.

A native Linux host or a Linux VM with passthrough disk would produce
higher absolute throughput; the engine-to-engine ratios are what
travels.

## Reproducibility

Every published number can be reproduced from the repo:

```bash
# 1. Build / pull the bench Docker image (see ../README.md "Public image").
# 2. Generate the fixtures (one per scale; takes single-digit minutes each):
docker compose exec bench python data_gen/generate_tpch_delta.py --scale 1
docker compose exec bench python data_gen/generate_tpcds_delta.py --scale 1
docker compose exec bench python data_gen/generate_ssb_delta.py    --scale 1
docker compose exec bench python data_gen/generate_job_delta.py
# 3. Run the benches:
docker compose exec bench python bench_runner.py \
    --scale 1 --engines df,duckdb,spark-default,spark-tuned \
    --workloads tpch_read_delta,tpcds_read_delta,ssb_read_delta,job_read_delta
```

Output lands in `results/<timestamp>-<host>/`. Each run's `manifest.json`
records engine versions, host facts, hardware spec, and the data SHA-256
of every Delta table read.

## Honest losses, in writing

DeltaForge is **not faster than DuckDB on most queries.** DuckDB is the
state of the art for vectorized single-node OLAP, and at SF=1 (where
fixture sizes are in the hundreds of MB) the contest is a fair fight
between two read-optimized engines. DeltaForge typically lands within
1-2x of DuckDB on warm-median and beats Spark by a wide margin across
all workloads.

The point of publishing this suite is **not** to claim DeltaForge wins
every query. It is to publish honest numbers from a reproducible
harness so the conversation about engine fit can be based on data
instead of marketing claims. Queries where DeltaForge loses are in
every published table, by name, with the slowdown factor.

## Source layout

| Path | What it is |
| --- | --- |
| [`workloads/tpch_read_delta.py`](../workloads/tpch_read_delta.py) | TPC-H workload definition |
| [`workloads/tpcds_read_delta.py`](../workloads/tpcds_read_delta.py) | TPC-DS workload definition |
| [`workloads/ssb_read_delta.py`](../workloads/ssb_read_delta.py) | SSB workload definition |
| [`workloads/job_read_delta.py`](../workloads/job_read_delta.py) | JOB workload definition |
| [`engines/df_engine.py`](../engines/df_engine.py) | DeltaForge adapter (`SHOW STATS ACTUAL` wrap, OPEN preamble) |
| [`engines/duckdb_engine.py`](../engines/duckdb_engine.py) | DuckDB adapter |
| [`engines/spark_default_engine.py`](../engines/spark_default_engine.py) | Spark stock-defaults |
| [`engines/spark_tuned_engine.py`](../engines/spark_tuned_engine.py) | Spark tuned profile (~40 keys, every key rationalized) |
| [`data_gen/`](../data_gen/) | Per-benchmark fixture generators (plain-Delta protocol) |
| [`bench_runner.py`](../bench_runner.py) | Harness entry point |

If you spot a methodological issue, the channel is
[GitHub issues](https://github.com/) on this repo. PRs that improve the
methodology are welcomed.
