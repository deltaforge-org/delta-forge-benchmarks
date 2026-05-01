"""psutil-based system metric sampler for engine process trees.

Used by engine adapters to capture peak RSS and average CPU% during a step
without depending on engine-specific telemetry.

Usage:
    sampler = MetricSampler(pid=spark_jvm_pid)
    with sampler:
        # ... run step ...
    print(sampler.peak_rss_mb, sampler.avg_cpu_pct)

The sampler runs in a daemon thread polling every 100 ms. Calling stop()
joins the thread and returns the accumulated stats. If the engine spawns
children, all descendants of the supplied PID are included.
"""
from __future__ import annotations

import threading
import time

import psutil


class MetricSampler:
    """Background sampler. Spawn one per measured step."""

    def __init__(self, pid: int, interval_ms: float = 100.0) -> None:
        self.root_pid = pid
        self.interval_s = interval_ms / 1000.0
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self.peak_rss_mb: float | None = None
        self.avg_cpu_pct: float | None = None
        self._cpu_samples: list[float] = []

    def __enter__(self) -> "MetricSampler":
        self.start()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.stop()

    def start(self) -> None:
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=2.0)
        if self._cpu_samples:
            self.avg_cpu_pct = sum(self._cpu_samples) / len(self._cpu_samples)

    def _run(self) -> None:
        try:
            root = psutil.Process(self.root_pid)
        except psutil.NoSuchProcess:
            return

        # Prime cpu_percent so subsequent calls return a meaningful delta.
        try:
            root.cpu_percent(interval=None)
        except psutil.NoSuchProcess:
            return

        peak_rss = 0
        while not self._stop.is_set():
            try:
                procs = [root] + root.children(recursive=True)
            except psutil.NoSuchProcess:
                break

            rss_total = 0
            cpu_total = 0.0
            for p in procs:
                try:
                    info = p.as_dict(attrs=["memory_info", "cpu_percent"])
                    if info["memory_info"]:
                        rss_total += info["memory_info"].rss
                    if info["cpu_percent"] is not None:
                        cpu_total += info["cpu_percent"]
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    continue

            peak_rss = max(peak_rss, rss_total)
            self._cpu_samples.append(cpu_total)
            time.sleep(self.interval_s)

        self.peak_rss_mb = peak_rss / (1024 * 1024)
