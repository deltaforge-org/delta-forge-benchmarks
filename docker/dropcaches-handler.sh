#!/bin/sh
# Single-purpose handler. Sync, then write 3 to drop_caches via the host's
# /proc bind-mounted at /host/proc. Print one status line on stdout so the
# bench harness can record `purge_verified` in the per-run JSON.
set -eu
sync
echo 3 > /host/proc/sys/vm/drop_caches
echo "DROPCACHES_OK $(date -u +%Y-%m-%dT%H:%M:%SZ)"
