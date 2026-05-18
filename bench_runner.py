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
    """Run one workload on one engine. Returns list of per-step JSON records."""
    records: list[dict] = []

    print(f"  [setup] {len(workload.setup_steps)} steps")
    setup_ms_total = 0.0
    for step in workload.setup_steps:
        resolved = resolve_step(step, data_dir, engine.name, scale)
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
                "timestamp_utc": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
            }
            records.append(rec)
            tag = "cold" if cold else "warm"
            ok = "ok" if result.exit_code == 0 else f"FAIL: {result.error}"
            print(f"    {step.id:>20s} run={run_idx} {tag:4s}  {result.wall_ms:>10.2f} ms  {ok}")

    print(f"  [cleanup] {len(workload.cleanup_steps)} steps")
    cleanup_ms_total = 0.0
    for step in workload.cleanup_steps:
        resolved = resolve_step(step, data_dir, engine.name, scale)
        result = engine.run_step(resolved)
        cleanup_ms_total += result.wall_ms
    print(f"  [cleanup] done in {cleanup_ms_total/1000.0:.2f}s")

    return records


def workload_data_dir(workload, scale: int) -> Path:
    """Resolve the staged-data directory for a workload at a given scale."""
    sub = workload.data_subdir.format(scale=scale)
    return REPO_ROOT / "data" / sub


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def cmd_run(args: argparse.Namespace) -> int:
    from workloads.spec import discover

    # Resolve workloads.
    available_workloads = discover(WORKLOADS_DIR)
    if args.workloads:
        wanted = [w.strip() for w in args.workloads.split(",") if w.strip()]
        missing = [w for w in wanted if w not in available_workloads]
        if missing:
            sys.exit(f"error: unknown workload(s): {missing}. "
                     f"available: {sorted(available_workloads)}")
        workloads_to_run = [available_workloads[w] for w in wanted]
    else:
        workloads_to_run = list(available_workloads.values())
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
        # Auto-generate TPC-H data if any TPC-H workload is scheduled and its
        # parquet files are missing. Graph workloads require separate generation
        # (the data is too large to bundle and needs the bench's data_gen script).
        for wl in workloads_to_run:
            if not wl.requires_input_data:
                continue
            d = workload_data[wl.name]
            if wl.data_subdir.startswith("tpch_sf") and not (
                d.exists() and any(d.rglob("*.parquet"))
            ):
                print(f"[data] TPC-H SF={args.scale} not found at {d} — generating now...")
                from data_gen.generate_tpch import generate as _gen_tpch
                _gen_tpch(args.scale, d)
                print(f"[data] TPC-H generation complete.")

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

    # Build the run directory.
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
                                  "delta-forge-worker"],
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

    # The real run loop.
    raw_dir = out_dir / "raw"
    overall_start = time.perf_counter()

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

        # Persist this engine's records.
        out_path = raw_dir / f"{engine_name}.jsonl"
        with out_path.open("w", encoding="utf-8") as f:
            for r in records:
                f.write(json.dumps(r, sort_keys=True) + "\n")
        print(f"  wrote {len(records)} records to {out_path}")

        # Engine teardown.
        try:
            engine.stop()
        except Exception as e:
            print(f"  warn: engine.stop() failed: {e}", file=sys.stderr)

    # Finalize manifest.
    manifest["elapsed_seconds"] = round(time.perf_counter() - overall_start, 3)
    with (out_dir / "manifest.json").open("w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, sort_keys=True)
        f.write("\n")
    print(f"\nDone. Results: {out_dir}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="bench_runner",
        description="DeltaForge vs Spark benchmark harness.",
    )
    p.add_argument("--scale", type=int,
                   default=int(os.environ.get("BENCH_SCALE_FACTOR", "1")),
                   help="TPC-H scale factor (1 = ~1 GB, 10 = ~10 GB).")
    p.add_argument("--engines", default="df,spark-default",
                   help="Comma-separated engine names. Default: df,spark-default.")
    p.add_argument("--workloads", default="tpch_read,bulk_load,crud",
                   help="Comma-separated workload names. Default: tpch_read,bulk_load,crud.")
    # Inside the container the results volume is mounted at /results.
    # On the host, fall back to the repo's results/ directory.
    _default_results = "/results" if Path("/results").exists() else str(DEFAULT_RESULTS_DIR)
    p.add_argument("--results-dir", default=_default_results,
                   help="Where per-run artifacts go. Defaults to /results inside container.")
    p.add_argument("--tag", default=None,
                   help="Optional suffix appended to the results directory name.")
    p.add_argument("--no-purge", action="store_true",
                   help="Skip the cold-run state purge (useful when the dropcaches sidecar is unavailable).")
    p.add_argument("--dry-run", action="store_true",
                   help="Plan and emit manifest.json, but do not launch any engine.")
    p.add_argument("--force", action="store_true",
                   help="Override pre-flight ERRORs (e.g. low disk free). Warnings always proceed.")
    return p


def main() -> int:
    args = build_parser().parse_args()
    return cmd_run(args)


if __name__ == "__main__":
    raise SystemExit(main())
