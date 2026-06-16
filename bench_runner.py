#!/usr/bin/env python3
"""delta-forge-benchmarks: main runner.

Drives one or more engines through one or more workloads and emits a per-run
JSON record per measured step + a manifest.json with host/version facts.

Run shape:
    for engine in engines:
        cold_start = engine.start()
        for workload in workloads:
            for step in workload.setup_steps:
                engine.run_step(step)
            for step in workload.measured_steps:
                for run_idx in range(workload.cold_runs + workload.warm_runs):
                    cold = run_idx < workload.cold_runs
                    if cold:
                        purge_for_cold(engine.process_patterns)
                    record = engine.run_step(step)
                    write_record(...)
            for step in workload.cleanup_steps:
                engine.run_step(step)
        engine.stop()

Cold-start metrics are recorded once per engine, reported separately from
step times. The dropcaches sidecar is optional: when it's not reachable,
runs are labeled `purge_verified=False` and the report excludes them from
cold-time aggregates.
"""
from __future__ import annotations

import argparse
import dataclasses
import datetime as dt
import hashlib
import importlib
import json
import os
import platform
import re
import socket
import subprocess
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(REPO_ROOT))  # so `import engines.*` and `import workloads.*` work

DEFAULT_RESULTS_DIR = REPO_ROOT / "results"
WORKLOADS_DIR = REPO_ROOT / "workloads"

ENGINE_REGISTRY = {
    "spark-default": ("engines.spark_default_engine", "SparkDefaultEngine"),
    "spark-tuned":   ("engines.spark_tuned_engine",   "SparkTunedEngine"),
    "df":            ("engines.df_engine",            "DeltaForgeEngine"),
    "duckdb":        ("engines.duckdb_engine",        "DuckDBEngine"),
}


# ---------------------------------------------------------------------------
# Host facts (delegated to engines.host_facts so the schema is reusable)
# ---------------------------------------------------------------------------

def collect_host_facts(data_dir: Path | None) -> dict:
    """Full host snapshot: CPU model + frequency governor + ISA flags,
    memory, disk (filesystem + measured read/write throughput),
    virtualization detection, cgroup limits, package versions.

    The dict shape is documented under `engines.host_facts.HOST_FACTS_SCHEMA_VERSION`.
    """
    from engines.host_facts import collect
    return collect(data_dir)


# ---------------------------------------------------------------------------
# Pre-flight checks: don't start a multi-hour run if the host can't carry it.
# ---------------------------------------------------------------------------

# Approximate disk footprint at each TPC-H scale factor (Parquet bytes plus
# room for Delta-format copies + checkpoints + small spill). These are
# conservative ceilings, sufficient for a SF run on either engine.
SCALE_DISK_GB_BUDGET = {1: 4, 10: 40, 30: 120, 100: 400, 300: 1200, 1000: 4000}

# Minimum RAM (host or cgroup) we recommend for a scale factor. SF=10 with
# `spark-default` (4 GB driver) will OOM on lineitem; we surface that as a
# warning, not an error, because DeltaForge handles it fine and "Spark OOMs
# at this scale with default config" is itself a finding worth publishing.
SCALE_RAM_GB_RECOMMENDED = {1: 8, 10: 16, 30: 32, 100: 96, 300: 256, 1000: 512}


def disk_free_gb(path: Path) -> float:
    """Free bytes at `path`'s filesystem, in GB."""
    try:
        if hasattr(os, "statvfs"):
            st = os.statvfs(path)
            return (st.f_bavail * st.f_frsize) / (1024 ** 3)
        import shutil as _shutil
        usage = _shutil.disk_usage(path)
        return usage.free / (1024 ** 3)
    except OSError:
        return float("nan")


def preflight(scale: int, data_dir: Path, engines: list[str], host_facts: dict,
              force: bool = False) -> list[str]:
    """Return a list of warning/error strings. Errors prefix with 'ERROR:';
    warnings prefix with 'warn:'. Caller decides whether to abort."""
    issues: list[str] = []

    # Disk: make sure the bench root has enough free space for the requested
    # scale plus headroom for Delta tables.
    needed_gb = SCALE_DISK_GB_BUDGET.get(scale, max(2 * scale, 4))
    free_gb = disk_free_gb(data_dir.parent if data_dir.exists() else REPO_ROOT)
    if free_gb < needed_gb and not force:
        issues.append(
            f"ERROR: only {free_gb:.1f} GB free on {data_dir.parent}; "
            f"SF={scale} needs ~{needed_gb} GB. Free space or pass --force to proceed."
        )
    elif free_gb < needed_gb * 1.5:
        issues.append(
            f"warn: free disk {free_gb:.1f} GB is tight for SF={scale} "
            f"(recommended ~{needed_gb * 1.5:.0f} GB)."
        )

    # Memory: warn if RAM (host or cgroup) is below the recommended threshold
    # for this scale. Stock-default Spark in particular OOMs hard at SF>=10
    # with 4 GB driver memory.
    rec_gb = SCALE_RAM_GB_RECOMMENDED.get(scale)
    cg = host_facts.get("cgroup", {}) or {}
    cgroup_mem_mb = cg.get("memory_max_mb")
    host_mem_kb = (host_facts.get("memory") or {}).get("MemTotal")
    if cgroup_mem_mb and isinstance(cgroup_mem_mb, (int, float)):
        avail_gb = cgroup_mem_mb / 1024.0
    elif host_mem_kb:
        avail_gb = host_mem_kb / (1024.0 * 1024.0)
    else:
        avail_gb = None

    if rec_gb and avail_gb and avail_gb < rec_gb:
        issues.append(
            f"warn: available memory {avail_gb:.1f} GB is below the recommended "
            f"{rec_gb} GB for SF={scale}. spark-default (4 GB driver) is likely "
            f"to OOM on lineitem. spark-tuned and df may still complete."
        )

    # If running stock-default Spark at SF>=10 specifically, surface this as
    # a deliberate caveat regardless of measured RAM.
    if "spark-default" in engines and scale >= 10:
        issues.append(
            f"note: spark-default at SF={scale} uses the stock 4 GB driver heap. "
            f"Expect warnings or OOM on lineitem. This is the documented baseline; "
            f"compare against spark-tuned for the realistic Spark number."
        )

    # WSL2 caveat: disk speed under 9P is much lower than bare-metal NVMe.
    if (host_facts.get("virtualization") or {}).get("wsl2"):
        issues.append(
            f"note: running under WSL2. Disk throughput is bottlenecked by "
            f"the 9P host filesystem; published numbers should be from a "
            f"native Linux host or a Linux VM with a passthrough disk."
        )

    return issues


def hash_data_dir(data_dir: Path) -> dict:
    hashes = {}
    if not data_dir.exists():
        return hashes
    for path in sorted(data_dir.rglob("*")):
        if path.is_file() and path.suffix == ".parquet":
            h = hashlib.sha256()
            with path.open("rb") as f:
                for chunk in iter(lambda: f.read(1 << 20), b""):
                    h.update(chunk)
            hashes[str(path.relative_to(data_dir))] = h.hexdigest()
    return hashes


# ---------------------------------------------------------------------------
# Output directory
# ---------------------------------------------------------------------------

def build_results_dir(parent: Path, tag: str | None) -> Path:
    stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    host = socket.gethostname()
    name = f"{stamp}-{host}" + (f"-{tag}" if tag else "")
    out = parent / name
    (out / "raw").mkdir(parents=True, exist_ok=True)
    (out / "logs").mkdir(parents=True, exist_ok=True)
    return out


# ---------------------------------------------------------------------------
# Step execution: substitute placeholders, run, record
# ---------------------------------------------------------------------------

# Limited placeholder set: ONLY these three names get substituted. We do
# not use str.format() because Cypher map literals (`{key: value}`) and
# str.format()'s field syntax collide. A regex over a known allowlist
# keeps the substitution lossless against arbitrary Cypher / SQL.
_PLACEHOLDER_RE = re.compile(r"\{(data_dir|data_basename|scale)\}")


def resolve_step(step, data_dir: Path, engine_name: str, scale: int):
    """Resolve a step for a specific engine: pick the per-engine kind/sql
    variants if the workload provided them, then substitute the three
    runner-managed placeholders (``{data_dir}``, ``{data_basename}``,
    ``{scale}``).
    """
    sql = step.sql
    kind = step.kind
    if step.per_engine_sql and engine_name in step.per_engine_sql:
        sql = step.per_engine_sql[engine_name]
    if step.per_engine_kind and engine_name in step.per_engine_kind:
        kind = step.per_engine_kind[engine_name]

    if sql is None:
        return dataclasses.replace(step, kind=kind)

    values = {
        "data_dir": str(data_dir).replace("\\", "/"),
        "data_basename": Path(data_dir).name,
        "scale": str(scale),
    }
    new_sql = _PLACEHOLDER_RE.sub(lambda m: values[m.group(1)], sql)
    return dataclasses.replace(step, kind=kind, sql=new_sql)


def run_workload(engine, workload, data_dir: Path, scale: int, raw_dir: Path,
                 cold_purge_fn=None) -> list[dict]:
    """Run one workload on one engine. Returns list of per-step JSON records
    AND appends each record to raw_dir/<engine>.jsonl as it completes.

    The per-step append is durability: if the worker process is OOM-killed
    or otherwise crashes mid-workload, every record for queries that
    already finished is on disk. The end-of-engine bulk write that this
    pattern replaces would have lost those records to the kernel. The
    scale-out concurrency bench depends on this: streams hitting the
    cgroup MemoryMax produce partial jsonls, not empty ones, so the
    aggregator can show "q01-q47 completed, q48 OOM" instead of just
    "stream missing.\""""
    records: list[dict] = []
    jsonl_path = raw_dir / f"{engine.name}.jsonl"
    raw_dir.mkdir(parents=True, exist_ok=True)

    def _append_record(rec: dict) -> None:
        records.append(rec)
        with jsonl_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(rec, sort_keys=True) + "\n")
            f.flush()
            try:
                os.fsync(f.fileno())
            except OSError:
                # fsync on tmpfs / overlayfs sometimes errors; the
                # buffered+flushed write is still durable enough for the
                # OOM-kill scenario we care about.
                pass

    print(f"  [setup] {len(workload.setup_steps)} steps")
    setup_ms_total = 0.0
    for step in workload.setup_steps:
        resolved = resolve_step(step, data_dir, engine.name, scale)
        # A step with no SQL and no Python fn for THIS engine is not applicable
        # to it (e.g. df's per-workload Delta-mount step is empty because df
        # registers in the catalog phase instead). Skip it rather than handing
        # the adapter an empty SQL_DDL, which would error and abort the workload.
        if resolved.sql is None and resolved.fn is None:
            continue
        result = engine.run_step(resolved)
        setup_ms_total += result.wall_ms
        if result.exit_code != 0:
            print(f"    [setup FAIL] {step.id}: {result.error}", file=sys.stderr)
            return records  # abort; subsequent steps would fail anyway
    print(f"  [setup] done in {setup_ms_total/1000.0:.2f}s")

    print(f"  [measured] {len(workload.measured_steps)} steps "
          f"x ({workload.cold_runs} cold + {workload.warm_runs} warm)")
    for step in workload.measured_steps:
        for run_idx in range(workload.cold_runs + workload.warm_runs):
            cold = run_idx < workload.cold_runs
            purge_verified = False
            if cold and cold_purge_fn is not None:
                try:
                    purge_result = cold_purge_fn()
                    purge_verified = bool(purge_result and purge_result.verified)
                except Exception as e:
                    print(f"    [warn] purge failed: {e}", file=sys.stderr)

            resolved = resolve_step(step, data_dir, engine.name, scale)
            result = engine.run_step(resolved)
            rec = {
                "workload": workload.name,
                "engine": engine.name,
                "step_id": step.id,
                "step_kind": step.kind,
                "step_description": step.description,
                "run_idx": run_idx,
                "cold": cold,
                "purge_verified": purge_verified,
                "wall_ms": result.wall_ms,
                "engine_reported_ms": result.engine_reported_ms,
                "rss_peak_mb": result.rss_peak_mb,
                "cpu_pct_avg": result.cpu_pct_avg,
                "rows_returned": result.rows_returned,
                "result_sha256": result.result_sha256,
                "exit_code": result.exit_code,
                "error": result.error,
                "extra": result.extra,
                "timestamp_utc": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
            }
            _append_record(rec)
            tag = "cold" if cold else "warm"
            ok = "ok" if result.exit_code == 0 else f"FAIL: {result.error}"
            print(f"    {step.id:>20s} run={run_idx} {tag:4s}  {result.wall_ms:>10.2f} ms  {ok}")

    print(f"  [cleanup] {len(workload.cleanup_steps)} steps")
    cleanup_ms_total = 0.0
    for step in workload.cleanup_steps:
        resolved = resolve_step(step, data_dir, engine.name, scale)
        if resolved.sql is None and resolved.fn is None:
            continue  # not applicable to this engine (see setup loop)
        result = engine.run_step(resolved)
        cleanup_ms_total += result.wall_ms
    print(f"  [cleanup] done in {cleanup_ms_total/1000.0:.2f}s")

    return records


def workload_data_dir(workload, scale: int) -> Path:
    """Resolve the staged-data directory for a workload at a given scale."""
    sub = workload.data_subdir.format(scale=scale)
    return REPO_ROOT / "data" / sub


# ---------------------------------------------------------------------------
# Delta fixture generation (auto-built on first run of a *_read_delta workload)
# ---------------------------------------------------------------------------

def _ensure_tpch_delta(scale: int, data_root: Path) -> None:
    """Ensure the TPC-H parquet AND tpch_sf{scale}_delta tables exist. The Spark
    delta generator reads the parquet; SSB in turn reads the TPC-H Delta tables."""
    pq = data_root / f"tpch_sf{scale}"
    if not (pq.exists() and any(pq.rglob("*.parquet"))):
        print(f"[data] TPC-H SF={scale} parquet not found — generating now...")
        from data_gen.generate_tpch import generate as _gen_tpch
        _gen_tpch(scale, pq)
    delta = data_root / f"tpch_sf{scale}_delta"
    if not (delta.exists() and any(delta.glob("*"))):
        print(f"[data] TPC-H SF={scale} Delta tables not found — building from parquet (Spark)...")
        subprocess.run(
            [sys.executable, str(REPO_ROOT / "data_gen" / "generate_tpch_delta.py"),
             "--scale", str(scale), "--data-dir", str(data_root)],
            check=True,
        )


def _ensure_delta_fixtures(name: str, scale: int, data_root: Path, delta_dir: Path) -> None:
    """Build the Delta fixtures a ``*_read_delta`` workload reads. Each benchmark
    has its own generator and prerequisites (see data_gen/)."""
    gendir = REPO_ROOT / "data_gen"
    if name == "tpch_read_delta":
        _ensure_tpch_delta(scale, data_root)
    elif name == "tpcds_read_delta":
        print(f"[data] TPC-DS SF={scale} Delta tables not found — generating (DuckDB dsdgen + Spark)...")
        subprocess.run(
            [sys.executable, str(gendir / "generate_tpcds_delta.py"),
             "--scale", str(scale), "--data-dir", str(data_root)], check=True)
    elif name == "ssb_read_delta":
        _ensure_tpch_delta(scale, data_root)  # SSB is derived from the TPC-H Delta tables
        print(f"[data] SSB SF={scale} Delta tables not found — generating (Spark)...")
        subprocess.run(
            [sys.executable, str(gendir / "generate_ssb_delta.py"),
             "--scale", str(scale), "--data-dir", str(data_root)], check=True)
    elif name == "job_read_delta":
        print(f"[data] JOB Delta tables not found — downloading IMDB + generating (Spark)...")
        subprocess.run(
            [sys.executable, str(gendir / "generate_job_delta.py"),
             "--data-dir", str(data_root)], check=True)
    else:
        print(f"[data] no Delta-fixture generator mapped for {name}; "
              f"expected pre-staged data at {delta_dir}", file=sys.stderr)
        return
    print(f"[data] {name}: Delta fixtures ready.")


# ---------------------------------------------------------------------------
# Interactive front-end + auto-report
# ---------------------------------------------------------------------------

# Engine presets offered in the menu. df is always included: it is the engine
# under test. The others are the comparison baselines.
_ENGINE_PRESETS = [
    ("df",                                      "DeltaForge only (fastest setup, no Spark JVM)"),
    ("df,duckdb",                               "DeltaForge + DuckDB"),
    ("df,duckdb,spark-default,spark-tuned",     "DeltaForge + DuckDB + Spark (full comparison)"),
]

# Non-interactive fallbacks when a flag is omitted and there is no terminal to
# ask. The full suite mirrors the `bench` launcher's historical default.
_DEFAULT_ENGINES = "df,duckdb,spark-default,spark-tuned"
_DEFAULT_WORKLOADS = ("tpch_read_delta,tpcds_read_delta,ssb_read_delta,"
                      "job_read_delta,synthetic_write_delta")


def _ask(prompt: str, default: str) -> str:
    """Prompt on stderr (so it never pollutes captured stdout / a results pipe)
    and read one line from stdin. Empty input or EOF yields the default."""
    sys.stderr.write(prompt)
    sys.stderr.flush()
    try:
        line = sys.stdin.readline()
    except (EOFError, KeyboardInterrupt):
        sys.stderr.write("\n")
        return default
    if line == "":            # EOF
        sys.stderr.write("\n")
        return default
    line = line.strip()
    return line if line else default


def run_menu(available: dict) -> dict:
    """Ask the operator which benchmark(s), which engines, scale, and run depth,
    and return the resolved overrides. Every benchmark is gated: a single pick
    runs only that one. Robust against bad input (re-prompts, falls back to a
    sensible default) because the setup flow IS the product."""
    names = list(available.keys())

    sys.stderr.write("\n" + "=" * 60 + "\n")
    sys.stderr.write(" DeltaForge benchmark - choose what to run\n")
    sys.stderr.write("=" * 60 + "\n")
    sys.stderr.write("\n Benchmarks (pick one to gate the run, or 'a' for all):\n")
    for i, n in enumerate(names, 1):
        desc = getattr(available[n], "description", "")
        sys.stderr.write(f"   {i}) {n:24} {desc}\n")
    sys.stderr.write(f"   a) all ({len(names)})\n")

    # Benchmark selection: a number, a comma-list of numbers, or 'a'.
    workloads = names[0]
    for _ in range(3):
        raw = _ask(f"\n Select benchmark [1-{len(names)}, comma-list, or 'a'; default 1]: ", "1")
        if raw.lower() in ("a", "all"):
            workloads = ",".join(names); break
        try:
            picks = [int(x) for x in raw.replace(" ", "").split(",") if x]
            if picks and all(1 <= p <= len(names) for p in picks):
                workloads = ",".join(names[p - 1] for p in picks); break
        except ValueError:
            pass
        sys.stderr.write(f"   ! not a valid choice: {raw!r}\n")

    # Engine preset.
    sys.stderr.write("\n Engines:\n")
    for i, (combo, desc) in enumerate(_ENGINE_PRESETS, 1):
        sys.stderr.write(f"   {i}) {desc}\n")
    engines = _ENGINE_PRESETS[-1][0]
    for _ in range(3):
        raw = _ask(f" Select engines [1-{len(_ENGINE_PRESETS)}; default {len(_ENGINE_PRESETS)}]: ",
                   str(len(_ENGINE_PRESETS)))
        try:
            idx = int(raw)
            if 1 <= idx <= len(_ENGINE_PRESETS):
                engines = _ENGINE_PRESETS[idx - 1][0]; break
        except ValueError:
            pass
        sys.stderr.write(f"   ! not a valid choice: {raw!r}\n")

    # Scale factor.
    scale = 1
    for _ in range(3):
        raw = _ask(" Scale factor (1 = ~1 GB; bigger needs much more disk) [default 1]: ", "1")
        try:
            v = int(raw)
            if v >= 1:
                scale = v; break
        except ValueError:
            pass
        sys.stderr.write(f"   ! must be a positive integer: {raw!r}\n")

    # Run depth.
    sys.stderr.write("\n Run depth:\n")
    sys.stderr.write("   1) quick  (1 cold + 1 warm per query)\n")
    sys.stderr.write("   2) full   (each workload's declared default, typically 1 cold + 9 warm)\n")
    cold_runs, warm_runs = 1, 1
    raw = _ask(" Select depth [1-2; default 1]: ", "1")
    if raw.strip() == "2":
        cold_runs, warm_runs = None, None  # None = keep each workload's declared default

    sys.stderr.write("\n" + "=" * 60 + "\n")
    sys.stderr.write(f" Running: --workloads {workloads} --engines {engines} --scale {scale}"
                     + ("  (quick)" if warm_runs == 1 else "  (full)") + "\n")
    sys.stderr.write("=" * 60 + "\n\n")
    sys.stderr.flush()
    return {"workloads": workloads, "engines": engines, "scale": scale,
            "cold_runs": cold_runs, "warm_runs": warm_runs}


def _emit_report(out_dir: Path) -> None:
    """Render the human-readable report (timings + the cross-engine correctness
    verdict) from the raw records, so every run ends with a studyable proof, not
    just jsonl. Best-effort: a report failure never fails the benchmark, but the
    correctness verdict is surfaced prominently when it succeeds."""
    gen = REPO_ROOT / "reports" / "generate_report.py"
    if not gen.exists():
        return
    print("\n" + "=" * 60)
    print(" Generating run report + cross-engine correctness verdict")
    print("=" * 60)
    try:
        rc = subprocess.run([sys.executable, str(gen), "--results-dir", str(out_dir)],
                            check=False).returncode
    except Exception as e:  # noqa: BLE001 - report is auxiliary; never abort the run
        print(f"  (report generation skipped: {e})", file=sys.stderr)
        return
    report_md = out_dir / "report.md"
    if report_md.exists():
        print(f"\n Full report:  {report_md}")
        print(f" Summary CSV:  {out_dir / 'summary.csv'}")
    if rc != 0:
        print("  (report generator returned non-zero; see output above)", file=sys.stderr)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def cmd_run(args: argparse.Namespace) -> int:
    from workloads.spec import discover

    # Resolve workloads.
    available_workloads = discover(WORKLOADS_DIR)

    # Interactive front-end: when run on a terminal with neither --workloads nor
    # --engines given, ask. Otherwise (flags given, --non-interactive, or no TTY
    # such as CI / plain `docker run`) fill any omitted selection from defaults.
    interactive = (sys.stdin.isatty() and not args.non_interactive
                   and args.workloads is None and args.engines is None
                   and not args.dry_run)
    if interactive:
        sel = run_menu(available_workloads)
        args.workloads = sel["workloads"]
        args.engines = sel["engines"]
        args.scale = sel["scale"]
        args.cold_runs = sel["cold_runs"]
        args.warm_runs = sel["warm_runs"]
    else:
        if args.workloads is None:
            args.workloads = _DEFAULT_WORKLOADS
        if args.engines is None:
            args.engines = _DEFAULT_ENGINES
    if args.workloads:
        wanted = [w.strip() for w in args.workloads.split(",") if w.strip()]
        missing = [w for w in wanted if w not in available_workloads]
        if missing:
            sys.exit(f"error: unknown workload(s): {missing}. "
                     f"available: {sorted(available_workloads)}")
        workloads_to_run = [available_workloads[w] for w in wanted]
    else:
        workloads_to_run = list(available_workloads.values())

    # Apply per-run cold/warm overrides before any workload is scheduled. The
    # Workload dataclass is mutable so this is a direct field write; both
    # the scheduler at run_workload() and the [measured] log line above read
    # the live field, so the override takes effect transparently.
    if args.cold_runs is not None:
        if args.cold_runs < 0:
            sys.exit(f"error: --cold-runs must be >= 0 (got {args.cold_runs})")
        for wl in workloads_to_run:
            wl.cold_runs = args.cold_runs
    if args.warm_runs is not None:
        if args.warm_runs < 0:
            sys.exit(f"error: --warm-runs must be >= 0 (got {args.warm_runs})")
        for wl in workloads_to_run:
            wl.warm_runs = args.warm_runs
    if args.cold_runs is not None or args.warm_runs is not None:
        zero_runs = [w.name for w in workloads_to_run
                     if w.cold_runs + w.warm_runs == 0]
        if zero_runs:
            sys.exit(f"error: --cold-runs + --warm-runs == 0 would skip every "
                     f"measured step on {zero_runs}; at least one must be > 0")
        print(f"workloads: {[w.name for w in workloads_to_run]} "
              f"(override: cold_runs={args.cold_runs}, warm_runs={args.warm_runs})")
    else:
        print(f"workloads: {[w.name for w in workloads_to_run]}")

    # Resolve engines.
    engines_wanted = [e.strip() for e in args.engines.split(",") if e.strip()]
    unknown = [e for e in engines_wanted if e not in ENGINE_REGISTRY]
    if unknown:
        sys.exit(f"error: unknown engine(s): {unknown}. "
                 f"known: {sorted(ENGINE_REGISTRY)}")
    print(f"engines:   {engines_wanted}")
    print(f"scale:     {args.scale}")

    # Verify staged data exists for every workload that will run. Each
    # workload declares its own `data_subdir` (TPC-H workloads default to
    # `tpch_sf{scale}`). Dry-run skips the check so a user can preview
    # manifest.json without staging data first.
    workload_data: dict[str, Path] = {
        wl.name: workload_data_dir(wl, args.scale) for wl in workloads_to_run
    }
    if not args.dry_run:
        # Auto-build the Delta fixtures every *_read_delta workload reads from its
        # sibling {data_dir}_delta directory. Each benchmark has its own generator
        # (and its own prerequisites); without this the registered tables are
        # empty/absent and every query fails. Graph data is generated separately.
        for wl in workloads_to_run:
            if not wl.requires_input_data:
                continue
            d = workload_data[wl.name]
            data_root = d.parent
            delta_dir = Path(str(d) + "_delta")
            if delta_dir.exists() and any(delta_dir.glob("*")):
                continue  # fixtures already built
            _ensure_delta_fixtures(wl.name, args.scale, data_root, delta_dir)

        # Final check: error on any read workload that still has no data.
        # Write workloads (requires_input_data=False) are exempt.
        missing: list[str] = []
        for wl in workloads_to_run:
            if not wl.requires_input_data:
                continue
            d = workload_data[wl.name]
            if not (d.exists() and any(d.rglob("*.parquet"))):
                missing.append(f"{wl.name} -> {d}")
        if missing:
            print("error: missing staged data for:", file=sys.stderr)
            for line in missing:
                print(f"  {line}", file=sys.stderr)
            sys.exit(2)
    # First TPC-H workload's data dir (or the first workload's, if no TPC-H)
    # is used for the host-facts disk-throughput probe. Pick a stable one.
    primary_data_dir = next(
        (workload_data[wl.name] for wl in workloads_to_run
         if wl.data_subdir.startswith("tpch_sf")),
        workload_data[workloads_to_run[0].name] if workloads_to_run else REPO_ROOT,
    )
    data_dir = primary_data_dir

    # Build the run directory. An isolated single-engine child reuses the exact
    # directory the parent already created (passed via --out-dir) so every
    # engine's results land together; only the parent mints a fresh timestamped
    # dir.
    if args.out_dir:
        out_dir = Path(args.out_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
    else:
        out_dir = build_results_dir(Path(args.results_dir), args.tag)
    print(f"results:   {out_dir}")

    # Capture host facts up front so even an aborted pre-flight has the
    # spec recorded. The disk-throughput probe takes ~1-2s on most hosts.
    print("collecting host facts (CPU/memory/disk probe)...")
    host_facts = collect_host_facts(data_dir)
    from engines.host_facts import render_short
    print(render_short(host_facts))

    # Pre-flight: disk free, memory, scale-specific caveats.
    issues = preflight(args.scale, data_dir, engines_wanted, host_facts,
                       force=args.force)
    if issues:
        print()
        for line in issues:
            print(f"  {line}")
        if any(line.startswith("ERROR:") for line in issues) and not args.force:
            print("\nAborting. Pass --force to override.", file=sys.stderr)
            return 2

    # Manifest with host + data facts. Engine version_info is appended as
    # each engine starts.
    manifest = {
        "schema_version": 2,
        "scale": args.scale,
        "engines": engines_wanted,
        "workloads": [w.name for w in workloads_to_run],
        "host": host_facts,
        "preflight_issues": issues,
        "data_hashes_by_workload": {
            wl.name: hash_data_dir(workload_data[wl.name])
            for wl in workloads_to_run
        },
        "engine_versions": {},
        "cold_starts": {},
    }

    # Optional cold-purge hook.
    cold_purge_fn = None
    if not args.no_purge:
        try:
            from engines._purge import purge_for_cold_run
            engine_patterns = {
                "spark-default": ["pyspark", "java.*spark"],
                "spark-tuned":   ["pyspark", "java.*spark"],
                "df":            ["delta-forge-cli", "delta-forge-server",
                                  "delta-forge-compute"],
            }
            def cold_purge_fn():
                from engines._purge import purge_for_cold_run
                return purge_for_cold_run(
                    engine_patterns.get(current_engine_name, [])
                )
        except ImportError:
            pass

    if args.dry_run:
        with (out_dir / "manifest.json").open("w") as f:
            json.dump(manifest, f, indent=2, sort_keys=True)
            f.write("\n")
        print("\nDRY RUN: re-run without --dry-run to execute.")
        return 0

    # The real run loop. Each engine runs in its OWN OS process so a hard crash
    # in one engine -- notably a Spark JVM that gets kill -9'd by its own
    # OnOutOfMemoryError handler on a heavy join -- cannot take down the engines
    # that have not run yet. PySpark is one-JVM-per-process, so two Spark
    # sessions in a single process cannot both survive the first one crashing;
    # process isolation is the only correct fix. The parent spawns one child
    # per engine (re-invoking this script with --out-dir) and merges each
    # child's durable raw/<engine>.jsonl plus its raw/<engine>.manifest.json
    # fragment.
    raw_dir = out_dir / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)
    overall_start = time.perf_counter()

    if args.out_dir is None:
        # PARENT: spawn one isolated child process per engine, sequentially.
        for engine_name in engines_wanted:
            print(f"\n=== engine: {engine_name} (isolated subprocess) ===")
            child_cmd = [
                sys.executable, os.path.abspath(__file__),
                "--scale", str(args.scale),
                "--engines", engine_name,
                "--workloads", ",".join(w.name for w in workloads_to_run),
                "--results-dir", str(args.results_dir),
                "--out-dir", str(out_dir),
                "--non-interactive",
            ]
            if args.no_purge:
                child_cmd.append("--no-purge")
            if args.force:
                child_cmd.append("--force")
            if args.cold_runs is not None:
                child_cmd += ["--cold-runs", str(args.cold_runs)]
            if args.warm_runs is not None:
                child_cmd += ["--warm-runs", str(args.warm_runs)]
            try:
                rc = subprocess.run(child_cmd).returncode
            except Exception as e:  # noqa: BLE001
                print(f"  engine {engine_name}: subprocess launch failed: {e}",
                      file=sys.stderr)
                rc = -1
            if rc != 0:
                # The child writes its per-step jsonl as it runs, so whatever
                # completed before a crash is still scored. Record the crash and
                # keep going to the next engine.
                print(f"  engine {engine_name}: isolated subprocess exited "
                      f"rc={rc}; crash contained, continuing with the remaining "
                      f"engines.", file=sys.stderr)

        # Merge each child's manifest fragment into the parent manifest.
        for engine_name in engines_wanted:
            frag_path = raw_dir / f"{engine_name}.manifest.json"
            if not frag_path.exists():
                continue
            try:
                frag = json.loads(frag_path.read_text(encoding="utf-8"))
            except Exception:  # noqa: BLE001
                continue
            manifest["engine_versions"].update(
                {k: v for k, v in frag.get("engine_versions", {}).items()
                 if v is not None})
            manifest["cold_starts"].update(
                {k: v for k, v in frag.get("cold_starts", {}).items()
                 if v is not None})

        manifest["elapsed_seconds"] = round(time.perf_counter() - overall_start, 3)
        with (out_dir / "manifest.json").open("w") as f:
            json.dump(manifest, f, indent=2, sort_keys=True)
            f.write("\n")
        print(f"\nDone. Results: {out_dir}")

        # Render the studyable proof: per-query timings plus the cross-engine
        # correctness verdict. Skipped for a dry run (handled earlier).
        if not args.dry_run:
            _emit_report(out_dir)
        return 0

    # CHILD (--out-dir set): run the requested engine(s) in THIS process and
    # write a per-engine manifest fragment for the parent to merge. No
    # top-level manifest.json and no report here -- the parent owns both.
    for engine_name in engines_wanted:
        current_engine_name = engine_name  # noqa: F841 (used by cold_purge_fn closure)
        mod_path, cls_name = ENGINE_REGISTRY[engine_name]
        print(f"\n=== engine: {engine_name} ===")
        try:
            mod = importlib.import_module(mod_path)
            engine_cls = getattr(mod, cls_name)
            engine = engine_cls()
        except Exception as e:
            print(f"failed to import {engine_name}: {e}", file=sys.stderr)
            continue

        # Cold start.
        try:
            cold_start = engine.start()
            print(f"  cold_start: ready_ms={cold_start.import_to_session_ready_ms:.1f}, "
                  f"first_query_ms={cold_start.session_to_first_query_ms:.1f}")
            manifest["cold_starts"][engine_name] = {
                "import_to_session_ready_ms": cold_start.import_to_session_ready_ms,
                "session_to_first_query_ms": cold_start.session_to_first_query_ms,
            }
        except Exception as e:
            print(f"engine {engine_name} failed to start: {e}", file=sys.stderr)
            continue

        manifest["engine_versions"][engine_name] = engine.version_info()

        # Truncate the engine's jsonl once at session start so a re-run
        # under the same results dir does not accumulate stale records.
        # run_workload appends per-step from this point forward (durable
        # against an OOM-kill mid-workload).
        out_path = raw_dir / f"{engine_name}.jsonl"
        raw_dir.mkdir(parents=True, exist_ok=True)
        out_path.open("w", encoding="utf-8").close()

        # One-time catalog registration. Registering a workload's Delta tables
        # in the engine catalog is dataset setup tied to table creation, NOT
        # per-query work, so it runs exactly ONCE per engine session here,
        # before any measured step, and is never unregistered (the next
        # clean-slate boot starts from an empty catalog). REGISTER DELTA TABLE
        # is idempotent, so a same-session re-run is a no-op. Path-reading
        # engines (DuckDB/Spark) carry no per_engine_sql on these steps, so
        # resolve_step yields sql=None and they skip without touching a catalog.
        for workload in workloads_to_run:
            if (workload.applicable_engines is not None
                    and engine_name not in workload.applicable_engines):
                continue
            for step in workload.catalog_setup_steps:
                resolved = resolve_step(step, workload_data[workload.name],
                                        engine_name, scale=args.scale)
                if resolved.sql is None:
                    continue
                result = engine.run_step(resolved)
                if result.error:
                    print(f"  [catalog FAIL] {workload.name}/{step.id}: "
                          f"{result.error}", file=sys.stderr)
                else:
                    print(f"  [catalog] {workload.name}: registered "
                          f"{workload.name.split('_')[0]} tables in the catalog")

        records: list[dict] = []
        for workload in workloads_to_run:
            if (workload.applicable_engines is not None
                    and engine_name not in workload.applicable_engines):
                print(f"\n--- workload: {workload.name} (skipped on {engine_name}; "
                      f"applicable_engines={list(workload.applicable_engines)}) ---")
                continue
            print(f"\n--- workload: {workload.name} ---")
            try:
                records.extend(
                    run_workload(
                        engine, workload, workload_data[workload.name],
                        args.scale, raw_dir, cold_purge_fn,
                    )
                )
            except Exception as e:
                print(f"workload {workload.name} on {engine_name} failed: {e}",
                      file=sys.stderr)

        print(f"  wrote {len(records)} records to {out_path}")

        # Engine teardown.
        try:
            engine.stop()
        except Exception as e:
            print(f"  warn: engine.stop() failed: {e}", file=sys.stderr)

        # Write this engine's manifest fragment for the parent to merge. The
        # per-step jsonl was already written incrementally during the run, so it
        # survives even if this child crashes right after the last query.
        frag = {
            "engine_versions": {
                engine_name: manifest["engine_versions"].get(engine_name)},
            "cold_starts": {
                engine_name: manifest["cold_starts"].get(engine_name)},
        }
        (raw_dir / f"{engine_name}.manifest.json").write_text(
            json.dumps(frag) + "\n", encoding="utf-8")

    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="bench_runner",
        description="DeltaForge vs Spark benchmark harness.",
    )
    p.add_argument("--scale", type=int,
                   default=int(os.environ.get("BENCH_SCALE_FACTOR", "1")),
                   help="TPC-H scale factor (1 = ~1 GB, 10 = ~10 GB).")
    p.add_argument("--engines", default=None,
                   help="Comma-separated engine names. Omit on a terminal to be "
                        "asked interactively; omit non-interactively for the "
                        "full set (df,duckdb,spark-default,spark-tuned).")
    p.add_argument("--workloads", default=None,
                   help="Comma-separated workload names (the gate: pass one to run "
                        "just that benchmark). Omit on a terminal to pick from a "
                        "menu; omit non-interactively to run the full suite.")
    p.add_argument("--non-interactive", action="store_true",
                   help="Never prompt; use flags (or defaults) as given. Implied "
                        "automatically when stdin is not a terminal (CI, piped, "
                        "plain `docker run`).")
    # Inside the container the results volume is mounted at /results.
    # On the host, fall back to the repo's results/ directory.
    _default_results = "/results" if Path("/results").exists() else str(DEFAULT_RESULTS_DIR)
    p.add_argument("--results-dir", default=_default_results,
                   help="Where per-run artifacts go. Defaults to /results inside container.")
    p.add_argument("--tag", default=None,
                   help="Optional suffix appended to the results directory name.")
    p.add_argument("--out-dir", default=None,
                   help="INTERNAL: run as an isolated single-engine child into this "
                        "exact, already-created results dir. Set automatically by the "
                        "parent process when it spawns one subprocess per engine so a "
                        "Spark JVM crash in one engine cannot take down the others. "
                        "Not for direct use.")
    p.add_argument("--no-purge", action="store_true",
                   help="Skip the cold-run state purge (useful when the dropcaches sidecar is unavailable).")
    p.add_argument("--dry-run", action="store_true",
                   help="Plan and emit manifest.json, but do not launch any engine.")
    p.add_argument("--force", action="store_true",
                   help="Override pre-flight ERRORs (e.g. low disk free). Warnings always proceed.")
    p.add_argument("--cold-runs", type=int, default=None,
                   help="Override every scheduled workload's cold_runs. Unset = "
                        "use each workload's declared default (typically 1).")
    p.add_argument("--warm-runs", type=int, default=None,
                   help="Override every scheduled workload's warm_runs. Unset = "
                        "use each workload's declared default (typically 9). "
                        "Set to 0 for single-shot Power-stream runs; the "
                        "scale-out orchestrator (scale_out/orchestrate.sh) "
                        "uses --cold-runs 0 --warm-runs 1 per stream.")
    return p


def main() -> int:
    args = build_parser().parse_args()
    return cmd_run(args)


if __name__ == "__main__":
    raise SystemExit(main())
