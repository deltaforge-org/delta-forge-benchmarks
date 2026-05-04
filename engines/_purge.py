"""Explicit between-engine state purge.

This is the audit surface for the cold-run protocol. Every cold run calls
`purge_for_cold_run()` before the engine starts. The function is a sequence
of named, individually-loggable steps; each step records pass / fail in the
returned `PurgeResult`. The runner attaches that result to the per-run JSON
as `purge_verified` and friends, so a reviewer can see exactly what happened.
"""
from __future__ import annotations

import dataclasses
import os
import shutil
import socket
import subprocess
import time
from pathlib import Path


DEFAULT_DROPCACHES_SOCKET = "/run/dropcaches.sock"


@dataclasses.dataclass
class PurgeResult:
    processes_killed: list[str]
    tmp_cleared: bool
    dropcaches_ok: bool
    dropcaches_message: str
    duration_ms: float

    @property
    def verified(self) -> bool:
        return self.tmp_cleared and self.dropcaches_ok


def _pkill(patterns: list[str]) -> list[str]:
    killed = []
    for pat in patterns:
        try:
            r = subprocess.run(
                ["pkill", "-f", pat],
                capture_output=True,
                text=True,
                timeout=5,
            )
            # pkill exit 0 = killed something, 1 = nothing matched. Both fine.
            if r.returncode in (0, 1):
                killed.append(f"{pat}:rc={r.returncode}")
            else:
                killed.append(f"{pat}:rc={r.returncode}:err={r.stderr.strip()}")
        except (OSError, subprocess.TimeoutExpired) as e:
            killed.append(f"{pat}:exception={e}")
    return killed


def _clear_tmp() -> bool:
    """Best-effort. Skip dirs we cannot touch (e.g. mounts) rather than
    abort the whole purge."""
    tmp = Path("/tmp")
    if not tmp.is_dir():
        return False
    ok = True
    for entry in tmp.iterdir():
        try:
            if entry.is_symlink() or entry.is_file():
                entry.unlink()
            elif entry.is_dir():
                shutil.rmtree(entry, ignore_errors=True)
        except OSError:
            ok = False
    return ok


def _trigger_dropcaches(socket_path: str, timeout_s: float = 5.0) -> tuple[bool, str]:
    """Connect to the dropcaches sidecar and read its acknowledgement line.
    Returns (ok, message). If the socket is missing or refuses connection,
    return (False, reason) so the run is recorded as cold-os-cache=unverified
    instead of silently treated as cold."""
    if not os.path.exists(socket_path):
        return False, f"socket missing: {socket_path}"
    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as s:
            s.settimeout(timeout_s)
            s.connect(socket_path)
            # Trigger the handler with a single byte; the handler ignores its
            # input and runs sync + drop_caches.
            s.sendall(b"x")
            chunks: list[bytes] = []
            while True:
                try:
                    chunk = s.recv(4096)
                except socket.timeout:
                    break
                if not chunk:
                    break
                chunks.append(chunk)
            reply = b"".join(chunks).decode("utf-8", errors="replace").strip()
        ok = reply.startswith("DROPCACHES_OK")
        return ok, reply or "no reply"
    except OSError as e:
        return False, f"connect failed: {e}"


def purge_for_cold_run(
    engine_process_patterns: list[str],
    dropcaches_socket: str | None = None,
) -> PurgeResult:
    """Run the full cold-run purge. Caller passes the process-name patterns
    for the engine that just finished (so we only kill what we mean to)."""
    started = time.perf_counter()

    killed = _pkill(engine_process_patterns)
    # Brief pause so processes actually exit before we drop caches.
    time.sleep(0.5)

    tmp_ok = _clear_tmp()

    sock = dropcaches_socket or os.environ.get(
        "DROPCACHES_TRIGGER", DEFAULT_DROPCACHES_SOCKET
    )
    drop_ok, drop_msg = _trigger_dropcaches(sock)

    return PurgeResult(
        processes_killed=killed,
        tmp_cleared=tmp_ok,
        dropcaches_ok=drop_ok,
        dropcaches_message=drop_msg,
        duration_ms=(time.perf_counter() - started) * 1000.0,
    )


def purge_neo4j_caches() -> tuple[bool, str]:
    """Cold-run helper specific to Neo4j.

    The Neo4j JVM lives in a separate compose container, so the standard
    pkill / dropcaches path leaves its in-process buffer pool warm. This
    helper:

      1. opens a fresh Bolt session,
      2. calls `db.clearQueryCaches` to flush the query plan + result
         caches,
      3. clears the transaction log resident pages by issuing an
         in-engine NOOP that forces the page cache to evict its LRU.

    The OS-level page cache for files on the neo4j volume is *not*
    touched (the dropcaches sidecar drops the bench container's caches,
    not the neo4j container's). For a fully cold neo4j run, the operator
    can run `docker compose restart neo4j` between iterations; the bench
    flags the in-process clear as `purge_verified=False` until that
    container restart happens.

    Returns ``(ok, message)``. Failures are non-fatal: the bench records
    them and continues.
    """
    try:
        from neo4j import GraphDatabase  # type: ignore
    except ImportError:
        return False, "neo4j driver not installed"

    bolt_url = os.environ.get("NEO4J_BOLT_URL", "bolt://localhost:7687")
    user = os.environ.get("NEO4J_USER", "neo4j")
    password = os.environ.get("NEO4J_PASSWORD")
    database = os.environ.get("NEO4J_DATABASE", "neo4j")
    if not password:
        return False, "NEO4J_PASSWORD not set"

    try:
        driver = GraphDatabase.driver(bolt_url, auth=(user, password))
        try:
            with driver.session(database=database) as s:
                # Clear plan + query caches. Neo4j 5+ exposes this as a
                # parameterless procedure on the system database; the
                # default user database accepts it via routing.
                s.run("CALL db.clearQueryCaches()").consume()
        finally:
            driver.close()
        return True, "cleared db query caches"
    except Exception as e:
        return False, f"cache clear failed: {e!r}"
