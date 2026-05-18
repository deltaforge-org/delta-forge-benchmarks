# Setup and reproducing

How to install, generate the fixtures, and run the bench end-to-end.
This is the long-form companion to the headlines on the
[main README](../README.md) and the per-benchmark pages in
[`published/`](../published/index.md).

## Before you start: get a license key

DeltaForge needs a license key to run at full benchmark capacity.
The key is **free, no credit card** — sign up at
[console.deltaforge.org](https://console.deltaforge.org) and copy the
key string into `docker/.env`:

```bash
cp docker/.env.example docker/.env
$EDITOR docker/.env
# set DELTA_FORGE_LICENSE_KEY=DF1.<your-key-from-console>
```

Without a license the server activates in evaluation mode at first
boot. That mode is enough to smoke-test the harness at SF=1 but caps
nodes / cores / DFCU usage and will throttle larger scales. The
bench-entrypoint script reads `DELTA_FORGE_LICENSE_KEY` from the
container environment, passes it to the headless bootstrap, and the
server activates online against the console at first start.

## Quickstart (Docker)

```bash
# 1. License key in docker/.env (see above)
# 2. Stack up (compose file + .env + override file all live under docker/)
cd docker
docker compose up -d

# 3. Fixtures (one-time per scale)
docker compose exec bench python data_gen/generate_tpch_delta.py    --scale 1
docker compose exec bench python data_gen/generate_tpcds_delta.py   --scale 1
docker compose exec bench python data_gen/generate_ssb_delta.py     --scale 1
docker compose exec bench python data_gen/generate_job_delta.py

# 4. Run
docker compose exec bench python bench_runner.py \
    --scale 1 --engines df,duckdb,spark-default,spark-tuned \
    --workloads tpch_read_delta,tpcds_read_delta,ssb_read_delta,job_read_delta,synthetic_write_delta
```

Each run lands in `results/<timestamp>-<host>-<tag>/`. Generate a publish
markdown for each with `reports/build_published.py --results-dir <dir>
--bench <workload-name> --out published/<short>.md`.

## Running on a Linux server (no Docker)

For a dedicated Linux server, the bench has shell scripts that go from
"fresh Ubuntu/Debian install" to "tarballed results" in a few commands:

```bash
git clone <bench-repo-url> delta-forge-benchmarks
cd delta-forge-benchmarks
./scripts/install.sh           # apt + venv + pinned pip; idempotent
./scripts/run_smoke.sh         # SF=1; sanity-checks the host
./scripts/run_bench.sh         # SF=10; the standard headline tier
```

| Script | Purpose | What it produces |
| --- | --- | --- |
| [`install.sh`](../scripts/install.sh) | apt-installs JDK 17 + python venv tooling, pip-installs the pinned `requirements.txt`, writes `JAVA_HOME` to `.env`. Idempotent. | `.venv/`, `.env` |
| [`run_smoke.sh`](../scripts/run_smoke.sh) | Generates SF=1 TPC-H (if absent), runs `spark-default` on `tpch_read_delta` only. | `results/<smoke-tag>/` |
| [`run_bench.sh`](../scripts/run_bench.sh) | Generates SF=10 TPC-H (if absent), runs all engines across all workloads. Tarballs the results dir. | `results/<bench-tag>/` + `.tar.gz` |

Args to `run_bench.sh` (all optional, defaults shown):

```bash
./scripts/run_bench.sh [SCALE] [ENGINES] [WORKLOADS] [TAG]
./scripts/run_bench.sh 10 df,duckdb,spark-default,spark-tuned \
    tpch_read_delta,tpcds_read_delta,ssb_read_delta,job_read_delta,synthetic_write_delta \
    bench-<utc>
```

Failures that are **expected and reported, not aborts**:

- `spark-default` OOMing on `lineitem` at SF=10 (4 GB driver heap is OOTB
  too small for 60M-row joins). The harness records `exit_code != 0` for
  the affected steps and continues.
- Any individual query failing on one engine. The report shows it as a
  failure; the cross-engine comparison degrades gracefully.

To export results from the server back to a workstation:

```bash
scp user@server:/path/to/delta-forge-benchmarks/results/<tag>.tar.gz .
tar -xzf <tag>.tar.gz
$EDITOR <tag>/report.md
```

## Required tools

- Docker 24+ with Compose v2
- A Linux host kernel that allows `--privileged` containers for the
  `dropcaches` sidecar (otherwise the page-cache drop is skipped and runs
  are labeled `cold-os-cache=unverified`)
- Disk + RAM scaled to your chosen TPC-H scale factor (see [Scale tiers](#scale-tiers) below)

## Hardware spec capture (automatic)

Every run records the host's hardware state into `manifest.json` automatically.
The harness prints a one-paragraph summary at run start so you can verify
the host shape immediately:

```text
CPU:    Intel(R) Core(TM) i9-7980XE CPU @ 2.60GHz (18 physical / 36 threads,
        max 4400 MHz, governor=performance, ISA=aes,avx,avx2,avx512f,bmi2)
Memory: 31.2 GiB total
Cgroup: cpu=8 cores  memory=16384 MiB  (/sys/fs/cgroup/...)
Disk:   read 1810.28 MB/s  write 756.80 MB/s  (ext4 on /dev/nvme0n1p2)
Virt:   WSL2, VM (wsl)
```

Captured into `manifest.json` (schema in [`engines/host_facts.py`](../engines/host_facts.py)):

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
  process.
- **Python + Java**: versions and resolved paths.
- **Pinned packages**: pyspark, delta-spark, duckdb, deltalake, psutil,
  pandas, matplotlib versions.

Sanity-check on a host without running the full bench:

```bash
python -m engines.host_facts --short                # one-paragraph
python -m engines.host_facts --data-path data/tpch_sf1   # full JSON
```

## Scale tiers

TPC-H scale factor is the headline knob.

| Tier | `--scale` | Parquet bytes | Lineitem rows | Disk free | RAM | Notes |
| --- | --: | --: | --: | --: | --: | --- |
| smoke | 1 | ~1 GB | 6.0M | 4 GB | 8 GB | sanity-check only; sub-second queries, noise dominates |
| standard | 10 | ~10 GB | 60.0M | 40 GB | 16 GB | the meaningful read tier |
| at-scale | 100 | ~100 GB | 600.0M | 400 GB | 96 GB | reference-host territory |
| stress | 1000 | ~1 TB | 6.0B | 4 TB | 512 GB | future |

**`--scale 1`** is the harness's "does the engine run at all" gate. At
this scale both engines finish every query in sub-seconds; the ratios
visible here are **engine architectural differences amplified by noise**,
not a workload that exercises shuffle or skew.

**`--scale 10`** is where Spark's default 4 GB driver heap is genuinely
under pressure on lineitem joins, AQE tuning matters, and engine
differences emerge. The 60M-row lineitem table is large enough that
joins shuffle real work and the "do I need a Spark cluster?" question
becomes interesting in the right direction.

**`--scale 100`** is the future at-scale tier. It needs ~96 GB RAM and
~400 GB disk. The interesting result there is that stock-default Spark
typically fails or spills heavily, while tuned Spark and df compete on
realistic workloads.

The harness pre-flight checks disk free and RAM at each tier and surfaces
warnings (or errors, with `--force` to override) before launching.

To generate data at any tier:

```bash
python data_gen/generate_tpch_delta.py    --scale 1     # SF=1
python data_gen/generate_tpcds_delta.py   --scale 1
python data_gen/generate_ssb_delta.py     --scale 1
python data_gen/generate_job_delta.py                   # fixed size
```

## Where the outputs go

| Path | What it is |
| --- | --- |
| `data/tpch_sf{N}_delta/`, `data/tpcds_sf{N}_delta/`, etc. | Plain-Delta fixtures (one-time per scale) |
| `data/job_delta/`, `data/imdb.tgz` | JOB fixed-size fixture |
| `results/<timestamp>-<host>-<tag>/raw/<engine>.jsonl` | Per-step records (one row per cold/warm run, per query, per engine) |
| `results/<timestamp>-<host>-<tag>/manifest.json` | Host facts + engine versions + data SHA-256 + preflight notes |
| `published/<bench>.md` | Aggregator output, marketing-link-able |

Everything under `data/` and `results/` is gitignored by default
(see [`.gitignore`](../.gitignore)).
