"""Host hardware + OS fact collector.

Comparing benchmark numbers across machines is meaningless without knowing
what the machines were. This module captures everything a skeptical
reviewer needs to decide whether your numbers and theirs were measured on
comparable hardware.

What we capture (all best-effort; missing fields land as `None` rather
than blowing up the run):

  - CPU: vendor, model, microarchitecture hint (from /proc/cpuinfo flags),
         physical core count, logical thread count, base + max frequency,
         cache sizes (L1/L2/L3), flags (AVX2, AVX-512, etc.).
  - Memory: total, available; from /proc/meminfo. (No dmidecode -- root only.)
  - Disk: filesystem of the bench data path, mount options, device backing
          (lsblk if available), and a *measured* sequential read throughput
          via a 256 MB cold-cache read. Catches WSL2/9P slowdown that
          /proc/cpuinfo can't see.
  - OS: distro + version (/etc/os-release), kernel, libc, glibc version.
  - Power/perf: CPU governor + scaling driver per-core (huge for fairness:
          `performance` vs `powersave` can be 2x).
  - Virtualization: WSL2 marker, container (cgroup detection), VM hints
          from /proc/cpuinfo flags + /sys/class/dmi/id.
  - Cgroup limits: actual cpu.max + memory.max applied to the bench
          process (vs what was requested via docker --cpus / --memory).
  - Python + Java: versions and resolved paths.
  - Pinned package versions: pyspark, delta-spark, duckdb, deltalake,
          psutil if importable.

The output dict has a stable schema; bumping it requires bumping
`HOST_FACTS_SCHEMA_VERSION` so older summary.csv files can be re-aligned.
"""
from __future__ import annotations

import datetime as dt
import os
import platform
import re
import socket
import subprocess
import sys
import time
from pathlib import Path

HOST_FACTS_SCHEMA_VERSION = 1


def _safe_read(path: str) -> str | None:
    try:
        return Path(path).read_text(encoding="utf-8", errors="replace")
    except OSError:
        return None


def _safe_run(cmd: list[str], timeout: float = 5.0) -> str | None:
    """Run a command, capture stdout, return None on any failure. Used for
    optional richer data (lscpu, lsblk) without making them required."""
    try:
        r = subprocess.run(
            cmd, capture_output=True, text=True, timeout=timeout, check=False
        )
        if r.returncode == 0:
            return r.stdout
    except (OSError, subprocess.TimeoutExpired):
        pass
    return None


# ---------------------------------------------------------------------------
# CPU
# ---------------------------------------------------------------------------

def _parse_cpuinfo() -> dict:
    """Parse /proc/cpuinfo into a flat summary."""
    text = _safe_read("/proc/cpuinfo")
    if not text:
        return {}

    fields_per_processor: list[dict] = []
    current: dict = {}
    for line in text.splitlines():
        if not line.strip():
            if current:
                fields_per_processor.append(current)
                current = {}
            continue
        if ":" in line:
            k, _, v = line.partition(":")
            current[k.strip().lower()] = v.strip()
    if current:
        fields_per_processor.append(current)

    if not fields_per_processor:
        return {}

    first = fields_per_processor[0]
    out: dict = {
        "logical_threads": len(fields_per_processor),
        "vendor": first.get("vendor_id"),
        "model_name": first.get("model name"),
        "family": first.get("cpu family"),
        "model": first.get("model"),
        "stepping": first.get("stepping"),
        "microcode": first.get("microcode"),
        "mhz_first_processor": _maybe_float(first.get("cpu mhz")),
        "cache_size": first.get("cache size"),
        "flags": first.get("flags", "").split() if first.get("flags") else [],
    }

    # Physical core count: distinct (physical id, core id) pairs.
    physical_pairs = {
        (p.get("physical id"), p.get("core id"))
        for p in fields_per_processor
        if p.get("core id") is not None
    }
    if physical_pairs:
        out["physical_cores"] = len(physical_pairs)
    else:
        # Fallback: cpu cores field of any processor.
        cc = first.get("cpu cores")
        if cc and cc.isdigit():
            out["physical_cores"] = int(cc)

    # Microarchitecture hint from flags. Not authoritative, just a useful
    # marker when comparing two hosts that report similar model strings.
    flag_set = set(out["flags"])
    out["isa"] = sorted(
        f for f in (
            "avx", "avx2", "avx512f", "avx512vnni", "avx_vnni",
            "sse4_2", "aes", "sha_ni", "bmi2", "fma",
            "amx_tile",
        )
        if f in flag_set
    )

    return out


def _maybe_float(v) -> float | None:
    try:
        return float(v) if v is not None else None
    except (TypeError, ValueError):
        return None


def _cpufreq_facts() -> dict:
    """Per-core scaling governor + min/max frequency. The same machine
    benchmarks 2x slower under `powersave` than under `performance`; this
    field is the second-most-important one in the manifest after the
    model name."""
    base = Path("/sys/devices/system/cpu")
    if not base.exists():
        return {}
    governors: list[str] = []
    drivers: list[str] = []
    cur_khz: list[int] = []
    max_khz: list[int] = []
    min_khz: list[int] = []
    for cpu_dir in sorted(base.glob("cpu[0-9]*")):
        gov = _safe_read(str(cpu_dir / "cpufreq" / "scaling_governor"))
        drv = _safe_read(str(cpu_dir / "cpufreq" / "scaling_driver"))
        cur = _safe_read(str(cpu_dir / "cpufreq" / "scaling_cur_freq"))
        mx = _safe_read(str(cpu_dir / "cpufreq" / "scaling_max_freq"))
        mn = _safe_read(str(cpu_dir / "cpufreq" / "scaling_min_freq"))
        if gov: governors.append(gov.strip())
        if drv: drivers.append(drv.strip())
        if cur and cur.strip().isdigit(): cur_khz.append(int(cur.strip()))
        if mx and mx.strip().isdigit():   max_khz.append(int(mx.strip()))
        if mn and mn.strip().isdigit():   min_khz.append(int(mn.strip()))

    out: dict = {}
    if governors:
        # Most hosts have one governor across all cores; collapse if uniform.
        unique = sorted(set(governors))
        out["governor"] = unique[0] if len(unique) == 1 else unique
    if drivers:
        unique = sorted(set(drivers))
        out["driver"] = unique[0] if len(unique) == 1 else unique
    if max_khz:
        out["max_mhz"] = max(max_khz) // 1000
    if min_khz:
        out["min_mhz"] = min(min_khz) // 1000
    if cur_khz:
        out["current_mhz_min"] = min(cur_khz) // 1000
        out["current_mhz_max"] = max(cur_khz) // 1000
    return out


def _lscpu_richer() -> dict:
    """lscpu -J adds NUMA + L1d/L1i/L2/L3 cache breakdowns. Optional."""
    out_text = _safe_run(["lscpu", "-J"])
    if not out_text:
        return {}
    try:
        import json as _json
        parsed = _json.loads(out_text)
    except Exception:
        return {}
    keep: dict = {}
    for entry in parsed.get("lscpu", []):
        f = entry.get("field", "").rstrip(":")
        v = entry.get("data")
        if f in {
            "Architecture", "Byte Order", "CPU op-mode(s)",
            "Vendor ID", "Model name", "CPU family", "Model", "Stepping",
            "BIOS Model name", "BIOS CPU family",
            "Thread(s) per core", "Core(s) per socket", "Socket(s)",
            "CPU max MHz", "CPU min MHz", "BogoMIPS",
            "L1d cache", "L1i cache", "L2 cache", "L3 cache",
            "NUMA node(s)", "NUMA node0 CPU(s)",
            "Virtualization", "Hypervisor vendor", "Virtualization type",
        }:
            keep[f] = v
    return keep


def cpu_facts() -> dict:
    base = _parse_cpuinfo()
    base["cpufreq"] = _cpufreq_facts()
    lscpu = _lscpu_richer()
    if lscpu:
        base["lscpu"] = lscpu
    return base


# ---------------------------------------------------------------------------
# Memory
# ---------------------------------------------------------------------------

def memory_facts() -> dict:
    text = _safe_read("/proc/meminfo")
    if not text:
        return {}
    out: dict = {}
    for line in text.splitlines():
        k, _, v = line.partition(":")
        v = v.strip()
        if not v:
            continue
        # MemTotal:        16384020 kB
        m = re.match(r"^(\d+)\s*([kMG]?B)?$", v)
        if not m:
            continue
        n, unit = int(m.group(1)), (m.group(2) or "kB")
        kb = n if unit == "kB" else (n * (1024 if unit == "MB" else 1024 * 1024))
        if k in {"MemTotal", "MemFree", "MemAvailable", "Buffers", "Cached",
                 "SwapTotal", "SwapFree", "Dirty", "HugePages_Total"}:
            out[k] = kb
    return out


# ---------------------------------------------------------------------------
# Disk: filesystem, mount, and a measured read throughput.
# ---------------------------------------------------------------------------

def _mount_for_path(path: Path) -> dict:
    """Find the mount entry covering `path`. Returns device, fstype,
    mount opts, and (if available) lsblk info for the device."""
    target = path.resolve()
    mounts_text = _safe_read("/proc/mounts")
    if not mounts_text:
        return {}
    best_match = None
    best_len = -1
    for line in mounts_text.splitlines():
        parts = line.split()
        if len(parts) < 4:
            continue
        device, mountpoint, fstype, opts = parts[0], parts[1], parts[2], parts[3]
        try:
            mp = Path(mountpoint).resolve()
        except OSError:
            continue
        # The longest prefix-match wins.
        try:
            target.relative_to(mp)
        except ValueError:
            continue
        if len(str(mp)) > best_len:
            best_len = len(str(mp))
            best_match = (device, mountpoint, fstype, opts)
    if not best_match:
        return {}
    device, mountpoint, fstype, opts = best_match
    out = {
        "path": str(target),
        "device": device,
        "mountpoint": mountpoint,
        "fstype": fstype,
        "options": opts.split(","),
    }
    # lsblk for the underlying device.
    lsblk = _safe_run(["lsblk", "-no", "NAME,ROTA,TRAN,MODEL,SIZE", device])
    if lsblk:
        out["lsblk"] = lsblk.strip().splitlines()
    return out


def measure_disk_read_mb_s(scratch_dir: Path,
                            payload_bytes: int = 256 * 1024 * 1024) -> dict:
    """Write a payload then read it back with O_DIRECT (or O_SYNC fallback)
    to approximate cold-cache sequential read throughput. Returns
    `{write_mb_s, read_mb_s, bytes}`. Best-effort: returns empty dict on
    permission or filesystem errors so it never blocks the run."""
    scratch_dir.mkdir(parents=True, exist_ok=True)
    path = scratch_dir / ".bench_disk_probe.bin"
    block = b"\x00" * (8 * 1024 * 1024)  # 8 MB blocks
    n_blocks = payload_bytes // len(block)

    # ----- write -----
    try:
        with path.open("wb", buffering=0) as f:
            t0 = time.perf_counter()
            for _ in range(n_blocks):
                f.write(block)
            os.fsync(f.fileno())
            write_dt = time.perf_counter() - t0
    except OSError:
        path.unlink(missing_ok=True)
        return {}

    # ----- drop the file's page cache so the read is honest -----
    try:
        # POSIX_FADV_DONTNEED: tell the kernel we don't need the cached pages.
        # On Linux this drops them. Fallback to opening O_DIRECT below if
        # this fails (e.g. on tmpfs / WSL 9P which don't honour fadvise).
        fd = os.open(str(path), os.O_RDONLY)
        try:
            try:
                os.posix_fadvise(fd, 0, payload_bytes, os.POSIX_FADV_DONTNEED)
            except (AttributeError, OSError):
                pass
        finally:
            os.close(fd)
    except OSError:
        pass

    # ----- read -----
    try:
        # Try O_DIRECT for a more honest cold-cache read. Aligned 8 MB
        # buffer, 4 KB-aligned offset (start of file is fine).
        try:
            fd = os.open(str(path), os.O_RDONLY | getattr(os, "O_DIRECT", 0))
        except OSError:
            fd = os.open(str(path), os.O_RDONLY)
        try:
            t0 = time.perf_counter()
            total = 0
            while True:
                chunk = os.read(fd, len(block))
                if not chunk:
                    break
                total += len(chunk)
            read_dt = time.perf_counter() - t0
        finally:
            os.close(fd)
    except OSError as e:
        path.unlink(missing_ok=True)
        return {"error": str(e)}

    path.unlink(missing_ok=True)

    return {
        "bytes": payload_bytes,
        "write_mb_s": round((payload_bytes / (1024 * 1024)) / max(write_dt, 1e-9), 2),
        "read_mb_s":  round((payload_bytes / (1024 * 1024)) / max(read_dt,  1e-9), 2),
    }


def disk_facts(data_path: Path | None = None) -> dict:
    out: dict = {}
    if data_path is not None:
        out["mount"] = _mount_for_path(data_path)
        out["throughput_probe"] = measure_disk_read_mb_s(data_path.parent)
    return out


# ---------------------------------------------------------------------------
# OS + libc + Java
# ---------------------------------------------------------------------------

def os_facts() -> dict:
    out: dict = {
        "uname": platform.uname()._asdict(),
        "platform": platform.platform(),
        "kernel_release": platform.release(),
        "kernel_version": platform.version(),
        "machine": platform.machine(),
    }
    osr = _safe_read("/etc/os-release")
    if osr:
        kv: dict = {}
        for line in osr.splitlines():
            if "=" in line:
                k, _, v = line.partition("=")
                kv[k.strip()] = v.strip().strip('"')
        out["os_release"] = kv
    # glibc version (best-effort).
    try:
        import ctypes
        libc = ctypes.CDLL("libc.so.6")
        libc.gnu_get_libc_version.restype = ctypes.c_char_p
        out["glibc"] = libc.gnu_get_libc_version().decode("ascii")
    except Exception:
        pass
    return out


def java_facts() -> dict:
    out: dict = {}
    java = os.environ.get("JAVA_HOME")
    if java:
        out["java_home"] = java
    ver = _safe_run(["java", "-version"])
    if ver is None:
        # `java -version` writes to stderr; capture both via shell.
        try:
            r = subprocess.run(
                ["java", "-version"], capture_output=True, text=True, timeout=5
            )
            ver = (r.stderr or r.stdout)
        except (OSError, subprocess.TimeoutExpired):
            ver = None
    if ver:
        out["version_string"] = ver.strip().splitlines()[0:3]
    return out


# ---------------------------------------------------------------------------
# Virtualization: WSL2, container, hypervisor
# ---------------------------------------------------------------------------

def virtualization_facts() -> dict:
    out: dict = {}
    # WSL2: kernel string contains "microsoft" or "WSL".
    rel = platform.release().lower()
    ver = platform.version().lower()
    out["wsl2"] = "microsoft" in rel or "wsl" in rel or "wsl" in ver
    # Container: cgroup file mentions docker/lxc/kubepods.
    cg = _safe_read("/proc/1/cgroup")
    if cg:
        out["container_hint"] = bool(re.search(r"docker|lxc|kubepods|containerd", cg))
    # Hypervisor flag in /proc/cpuinfo flags. Set on every VM.
    cpuinfo = _safe_read("/proc/cpuinfo") or ""
    out["hypervisor_flag"] = " hypervisor " in cpuinfo or cpuinfo.endswith(" hypervisor")
    # systemd-detect-virt (best-effort, not always present).
    detect = _safe_run(["systemd-detect-virt"])
    if detect is not None:
        out["systemd_detect_virt"] = detect.strip()
    # /sys/class/dmi/id/sys_vendor (root-readable on most distros).
    vendor = _safe_read("/sys/class/dmi/id/sys_vendor")
    if vendor:
        out["dmi_sys_vendor"] = vendor.strip()
    return out


# ---------------------------------------------------------------------------
# Cgroup limits the bench process is actually subject to
# ---------------------------------------------------------------------------

def cgroup_limits() -> dict:
    """What CPU/memory the cgroup actually grants this process. Useful
    when running in `docker --cpus 8 --memory 16g`: docker writes those
    into cpu.max + memory.max, and we read them back to confirm the
    container is enforcing what was asked. cgroup v2 path."""
    out: dict = {}
    try:
        my_cg = _safe_read("/proc/self/cgroup") or ""
        # cgroup v2 has a single line: "0::<path>"
        m = re.search(r"^0::(.+)$", my_cg, flags=re.MULTILINE)
        if not m:
            return out
        path = m.group(1).strip()
        base = Path("/sys/fs/cgroup") / path.lstrip("/")
        out["path"] = str(base)
        cpu_max = _safe_read(str(base / "cpu.max"))
        if cpu_max:
            parts = cpu_max.strip().split()
            # quota period_us; quota="max" means unlimited
            if len(parts) == 2:
                out["cpu_quota"] = parts[0]
                out["cpu_period_us"] = int(parts[1]) if parts[1].isdigit() else parts[1]
                if parts[0].isdigit():
                    out["cpu_quota_cores"] = round(int(parts[0]) / int(parts[1]), 2)
        mem_max = _safe_read(str(base / "memory.max"))
        if mem_max:
            mem_max = mem_max.strip()
            out["memory_max"] = mem_max
            if mem_max.isdigit():
                out["memory_max_mb"] = round(int(mem_max) / (1024 * 1024), 1)
    except Exception:
        pass
    return out


# ---------------------------------------------------------------------------
# Pinned package versions (Python side)
# ---------------------------------------------------------------------------

def python_pkg_facts() -> dict:
    out: dict = {
        "python_version": platform.python_version(),
        "python_executable": sys.executable,
    }
    for pkg in ("pyspark", "delta", "duckdb", "deltalake", "psutil", "pandas",
                "matplotlib", "plotly"):
        try:
            mod = __import__(pkg)
            out[pkg] = getattr(mod, "__version__", "unknown")
        except ImportError:
            pass
    return out


# ---------------------------------------------------------------------------
# Top-level entry: assemble the full snapshot.
# ---------------------------------------------------------------------------

def collect(data_path: Path | None = None) -> dict:
    """Return the full host_facts dict. `data_path` is the directory the
    bench will read/write its TPC-H Parquet from; the disk probe runs
    against its parent so we measure the right device."""
    return {
        "schema_version": HOST_FACTS_SCHEMA_VERSION,
        "captured_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
        "hostname": socket.gethostname(),
        "fqdn": socket.getfqdn(),
        "cpu": cpu_facts(),
        "memory": memory_facts(),
        "disk": disk_facts(data_path),
        "os": os_facts(),
        "java": java_facts(),
        "virtualization": virtualization_facts(),
        "cgroup": cgroup_limits(),
        "python": python_pkg_facts(),
        # Echo of bench-relevant env vars; the Docker entrypoint sets these.
        "env_pins": {
            k: os.environ.get(k, "unset")
            for k in (
                "BENCH_CPUS", "BENCH_MEMORY", "BENCH_SCALE_FACTOR",
                "DF_VERSION", "DF_GIT_SHA",
                "PYSPARK_VERSION", "DELTA_SPARK_VERSION", "JDK_VERSION",
            )
        },
    }


def render_short(facts: dict) -> str:
    """One-paragraph human summary printed at run start so a casual reader
    sees the host shape immediately."""
    cpu = facts.get("cpu", {})
    mem = facts.get("memory", {})
    cg = facts.get("cgroup", {})
    disk = facts.get("disk", {}).get("throughput_probe", {})
    virt = facts.get("virtualization", {})

    lines = []
    model = cpu.get("model_name") or cpu.get("vendor") or "unknown CPU"
    cores = cpu.get("physical_cores") or "?"
    threads = cpu.get("logical_threads") or "?"
    gov = cpu.get("cpufreq", {}).get("governor")
    max_mhz = cpu.get("cpufreq", {}).get("max_mhz")
    isa = ",".join(cpu.get("isa", [])[:5])

    lines.append(
        f"CPU:    {model} ({cores} physical / {threads} threads, "
        f"max {max_mhz} MHz, governor={gov}, ISA={isa})"
    )
    mem_total = mem.get("MemTotal")
    if mem_total:
        lines.append(f"Memory: {mem_total / 1024 / 1024:.1f} GiB total")
    if cg.get("cpu_quota_cores") or cg.get("memory_max_mb"):
        lines.append(
            f"Cgroup: cpu={cg.get('cpu_quota_cores', '?')} cores  "
            f"memory={cg.get('memory_max_mb', '?')} MiB  ({cg.get('path', '')})"
        )
    if disk.get("read_mb_s"):
        lines.append(
            f"Disk:   read {disk['read_mb_s']} MB/s  write {disk['write_mb_s']} MB/s  "
            f"({facts['disk'].get('mount', {}).get('fstype', '?')} on "
            f"{facts['disk'].get('mount', {}).get('device', '?')})"
        )
    extras = []
    if virt.get("wsl2"): extras.append("WSL2")
    if virt.get("container_hint"): extras.append("container")
    if virt.get("hypervisor_flag"): extras.append(f"VM ({virt.get('systemd_detect_virt') or 'hypervisor'})")
    if extras:
        lines.append(f"Virt:   {', '.join(extras)}")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# CLI: print the facts so `python -m engines.host_facts` works for sanity.
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse
    import json
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--data-path", default=None,
                    help="Path the bench reads from; the disk probe runs against its parent.")
    ap.add_argument("--short", action="store_true",
                    help="Print one-paragraph summary instead of full JSON.")
    args = ap.parse_args()

    facts = collect(Path(args.data_path) if args.data_path else None)
    if args.short:
        print(render_short(facts))
    else:
        print(json.dumps(facts, indent=2, sort_keys=True, default=str))
