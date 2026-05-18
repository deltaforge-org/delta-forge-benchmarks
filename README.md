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

## Quickstart

**1.** Get a **free** DeltaForge license key (no credit card required) at
[console.deltaforge.org](https://console.deltaforge.org) and put it in
`docker/.env`:

```bash
cp docker/.env.example docker/.env
$EDITOR docker/.env       # set DELTA_FORGE_LICENSE_KEY=DF1.<your-key>
```

**The license key is required.** DeltaForge cannot bootstrap without
one — the headless bootstrap fails and the bench container exits.
Sign-up at the console is free, no credit card, takes under a minute,
and the key activates online against the console on first container
start.

**2.** Bring up the stack and generate the fixtures (one-time per scale).
The compose file lives under `docker/`, so run compose commands from
there (the auto-loaded `docker-compose.override.yml` for Windows-host
quirks is also in that directory):

```bash
cd docker
docker compose up -d
docker compose exec bench python data_gen/generate_tpch_delta.py    --scale 1
docker compose exec bench python data_gen/generate_tpcds_delta.py   --scale 1
docker compose exec bench python data_gen/generate_ssb_delta.py     --scale 1
docker compose exec bench python data_gen/generate_job_delta.py
```

**3.** Run the bench:

```bash
docker compose exec bench python bench_runner.py \
    --scale 1 --engines df,duckdb,spark-default,spark-tuned \
    --workloads tpch_read_delta,tpcds_read_delta,ssb_read_delta,job_read_delta,synthetic_write_delta
```

Each workload produces a `results/<timestamp>-<host>-<tag>/` directory.
Generate a publish markdown for each with:

```bash
docker compose exec bench python reports/build_published.py \
    --results-dir results/<timestamp>-<tag> --bench tpch_read_delta \
    --out published/tpch.md
```

Full setup and reproducing instructions: [docs/setup.md](docs/setup.md).

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
├── README.md                       # this file (at-a-glance results)
├── docs/
│   ├── setup.md                    # install, run, scale tiers, hardware capture
│   ├── image.md                    # docker image pull/build/publish
│   └── design.md                   # design invariants, scope filter, future chapters
├── published/                      # marketing-linkable, per-bench markdown
│   ├── index.md                    # TOC
│   ├── methodology.md              # measurement contract
│   ├── tpch.md, tpcds.md, ssb.md, job.md, writes.md
├── bench_runner.py                 # main entry point
├── engines/
│   ├── df_engine.py                # DeltaForge: drives delta-forge-cli + server + worker
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
├── reports/
│   ├── build_published.py          # JSONL -> publish markdown
│   ├── summarize_run.py            # console summary
│   └── _headline.py                # quick cross-bench headline
├── docker/                         # Dockerfile, compose stack, dropcaches sidecar
└── scripts/                        # install.sh, run_smoke.sh, run_bench.sh
```

## License

Apache License 2.0. See [`LICENSE`](LICENSE).

This repo bundles official PyPI distributions of Apache Spark, Delta
Lake, and DuckDB for benchmark purposes; each is credited under its
own license. The image is **not** a runtime endorsed by those
projects.
