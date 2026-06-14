# Setup and reproducing

How to install, generate the fixtures, and run the bench end to end. This is the
long-form companion to the headlines on the [main README](../README.md) and the
per-benchmark pages in [`published/`](../published/index.md).

## One-command install

The installer downloads only official, signed DeltaForge releases (the platform
plus the CLI), sets up the comparison engines, and is ready to run. Nothing is
built from source; Docker is not required.

**macOS / Linux:**

```bash
curl -fsSL https://deltaforge.org/bench/install.sh | bash
```

**Windows (PowerShell):**

```powershell
irm https://deltaforge.org/bench/install.ps1 | iex
```

It performs a full preflight first and stops with a clear, actionable message if
anything is missing: unsupported OS/arch, missing Python (3.9+ required), no
network to github.com, too little disk or RAM, or a control-plane port already in
use. Then it:

1. resolves the latest release (override with `DF_VERSION=1.0.5`),
2. downloads the platform + CLI for your OS/arch and verifies both against
   `SHA256SUMS`,
3. installs them under `.engine/` (AppImage on Linux, `.app` from the `.dmg` on
   macOS, a portable `msiexec /a` extract on Windows),
4. creates a Python venv and installs the pinned harness dependencies,
5. provisions a pinned Temurin 17 JRE if you do not already have Java (Spark
   needs a JVM; df and DuckDB do not),
6. asks which license to use, and
7. writes `.env` describing everything it installed.

## Run

```bash
cd delta-forge-benchmarks
./bench                 # SF=1 smoke across all four engines
./bench --scale 10      # the standard headline tier
```

Windows: use `.\bench.ps1` (e.g. `.\bench.ps1 -Scale 10`).

`./bench` boots the DeltaForge platform (its embedded control plane + compute
node), waits until `deltaforge-cli health` passes, runs the harness, and shuts
the platform down on exit. Any flags after `./bench` pass straight through to
`bench_runner.py`, for example:

```bash
./bench --scale 10 --engines df,duckdb \
        --workloads tpch_read_delta,tpcds_read_delta,ssb_read_delta,job_read_delta,synthetic_write_delta
```

If the platform fails to start, `./bench` prints the tail of `logs/platform.log`
and diagnoses the common causes (license exhaustion, a busy port, a missing
display on a headless Linux host).

## License

DeltaForge requires a license to run. The installer ships with a built-in, free
**community** license and uses it automatically, so a first run needs no signup
and no prompt. That key is **shared and compute-capped** and can be exhausted by
other users. For an uncapped, private run, get your own free key at
[console.deltaforge.org](https://console.deltaforge.org) and set it ahead of time
(it is used as-is):

```bash
DELTA_FORGE_LICENSE_KEY=DF1.<your-key> curl -fsSL https://deltaforge.org/bench/install.sh | bash
```

You can change the license any time by editing `DELTA_FORGE_LICENSE_KEY` in
`.env`.

## Per-OS notes

- **macOS (Apple Silicon or Intel):** the `.dmg` is copied into `.engine/` and the
  quarantine flag is cleared so the first launch is not blocked by Gatekeeper.
- **Windows x64:** the platform `.msi` is extracted portably (no elevation, no
  system install, no registry writes). Run the installer in PowerShell; if script
  execution is blocked, start it with
  `powershell -ExecutionPolicy Bypass -File install.ps1`.
- **Linux x64:** the AppImage runs directly when FUSE is available and is
  auto-extracted to run without FUSE otherwise (common on servers/CI). On a
  headless host the platform needs a virtual display: install `xvfb` and `./bench`
  will wrap the platform in `xvfb-run` automatically.
- **Linux arm64 / Windows arm64:** no published platform build yet. Use an x64
  host, or macOS arm64 (which is published).

## Environment overrides

| Variable | Effect |
| --- | --- |
| `DF_VERSION` | Install a specific engine release instead of the latest. |
| `DELTA_FORGE_LICENSE_KEY` | Your DeltaForge license key (required to run the engine; set it to skip the interactive prompt). |
| `BENCH_HOME` | Where the harness lives (default: `./delta-forge-benchmarks`). |
| `DF_PREFIX` | Where engine binaries land (default: `$BENCH_HOME/.engine`). |
| `SKIP_SPARK=1` | Do not provision Java / Spark (df + DuckDB only). |
| `ASSUME_YES=1` | Accept defaults non-interactively (requires `DELTA_FORGE_LICENSE_KEY` to be set, since there is no prompt). |

## Required tools

The installer needs only these on the host; it checks for each and tells you how
to install anything missing:

- `python3` (3.9 or newer) and `pip`/`venv`
- `curl` and `tar`
- `git` (only when you pipe the script in and it has to clone the harness)
- On a **headless Linux** host: `xvfb` (so the desktop platform can run without a
  monitor)

## Hardware spec capture (automatic)

Every run records the host's hardware state into `manifest.json` automatically.
The harness prints a one-paragraph summary at run start so you can verify
the host shape immediately:

```text
CPU:    Intel(R) Core(TM) i9-7980XE CPU @ 2.60GHz (18 physical / 36 threads,
        max 4400 MHz, governor=performance, ISA=aes,avx,avx2,avx512f,bmi2)
Memory: 31.2 GiB total
Disk:   read 1810.28 MB/s  write 756.80 MB/s  (ext4 on /dev/nvme0n1p2)
Virt:   bare-metal
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
  write throughput (256 MB cold-cache probe).
- **OS**: kernel + version + machine, `/etc/os-release` (distro + version),
  glibc version.
- **Virtualization**: container, hypervisor (CPU flag + `systemd-detect-virt`).
- **Python + Java**: versions and resolved paths.
- **Pinned packages**: pyspark, delta-spark, duckdb, deltalake, psutil,
  pandas, matplotlib versions.

Sanity-check on a host without running the full bench:

```bash
.venv/bin/python -m engines.host_facts --short              # one-paragraph
.venv/bin/python -m engines.host_facts --data-path data/tpch_sf1   # full JSON
```

## Scale tiers

TPC-H scale factor is the headline knob.

| Tier | `--scale` | Parquet bytes | Lineitem rows | Disk free | RAM | Notes |
| --- | --: | --: | --: | --: | --: | --- |
| smoke | 1 | ~1 GB | 6.0M | 4 GB | 8 GB | sanity-check only; sub-second queries, noise dominates |
| standard | 10 | ~10 GB | 60.0M | 40 GB | 16 GB | the meaningful read tier |
| at-scale | 100 | ~100 GB | 600.0M | 400 GB | 96 GB | reference-host territory |
| stress | 1000 | ~1 TB | 6.0B | 4 TB | 512 GB | future |

**`--scale 1`** is the harness's "does the engine run at all" gate. At this scale
both engines finish every query in sub-seconds; the ratios visible here are
**engine architectural differences amplified by noise**, not a workload that
exercises shuffle or skew.

**`--scale 10`** is where Spark's default 4 GB driver heap is genuinely under
pressure on lineitem joins, AQE tuning matters, and engine differences emerge.

**`--scale 100`** is the future at-scale tier. It needs ~96 GB RAM and ~400 GB
disk. The interesting result there is that stock-default Spark typically fails or
spills heavily, while tuned Spark and df compete on realistic workloads.

The harness pre-flight checks disk free and RAM at each tier and surfaces
warnings (or errors, with `--force` to override) before launching.

`./bench` auto-generates the fixtures for any scheduled workload if they are
missing; to pre-generate a tier explicitly:

```bash
.venv/bin/python data_gen/generate_tpch_delta.py    --scale 1
.venv/bin/python data_gen/generate_tpcds_delta.py   --scale 1
.venv/bin/python data_gen/generate_ssb_delta.py     --scale 1
.venv/bin/python data_gen/generate_job_delta.py
```

## Where the outputs go

| Path | What it is |
| --- | --- |
| `data/tpch_sf{N}_delta/`, `data/tpcds_sf{N}_delta/`, etc. | Plain-Delta fixtures (one-time per scale) |
| `data/job_delta/`, `data/imdb.tgz` | JOB fixed-size fixture |
| `results/<timestamp>-<host>-<tag>/raw/<engine>.jsonl` | Per-step records (one row per cold/warm run, per query, per engine) |
| `results/<timestamp>-<host>-<tag>/manifest.json` | Host facts + engine versions + data SHA-256 + preflight notes |
| `logs/platform.log` | The DeltaForge platform's stdout/stderr for the run |
| `published/<bench>.md` | Aggregator output, marketing-link-able |

Everything under `data/` and `results/` is gitignored by default
(see [`.gitignore`](../.gitignore)).
