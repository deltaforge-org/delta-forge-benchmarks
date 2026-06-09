#!/usr/bin/env bash
# driver-bench: tear down the host-side DeltaForge stack created by
# setup-host-stack.sh.
#
# Default behaviour: stop the worker + control plane + postgres but
# leave PGDATA + DELTA_FORGE_CONFIG_DIR + storage on disk so a re-run of
# setup-host-stack.sh is fast (skip bootstrap, skip initdb).
#
# Pass --purge to also wipe pg-data, config, zone storage, and the
# ~/.odbc.ini / ~/.odbcinst.ini backups setup-host-stack.sh wrote.
#
# Pass --restore-odbc to put back the user's pre-existing .odbc.ini /
# .odbcinst.ini that setup-host-stack.sh saved as *.bench-backup.

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

PURGE=0
RESTORE_ODBC=0
for arg in "$@"; do
    case "$arg" in
        --purge)        PURGE=1 ;;
        --restore-odbc) RESTORE_ODBC=1 ;;
        --help|-h)
            grep '^#' "$0" | sed 's/^# \{0,1\}//' | head -25
            exit 0 ;;
        *) echo "unknown arg: $arg" >&2; exit 2 ;;
    esac
done

log()  { printf "\033[1;36m[stack-down] %s\033[0m\n" "$*"; }
warn() { printf "\033[1;33m[stack-down] WARN %s\033[0m\n" "$*" >&2; }

DF_HOME="${DF_HOME:-/tmp/df-bench-stack}"
[ -f "$DF_HOME/stack.env" ] && set -a && . "$DF_HOME/stack.env" && set +a

# ----- 1. stop worker + control plane ---------------------------------------

for pidf in "$DF_HOME/worker.pid" "$DF_HOME/server.pid"; do
    if [ -f "$pidf" ]; then
        pid="$(cat "$pidf" 2>/dev/null || true)"
        if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
            log "stopping pid=$pid ($(basename "$pidf"))"
            kill "$pid" 2>/dev/null || true
            # Give it 5s to exit cleanly, then SIGKILL.
            for _ in 1 2 3 4 5; do
                kill -0 "$pid" 2>/dev/null || break
                sleep 1
            done
            kill -0 "$pid" 2>/dev/null && kill -9 "$pid" 2>/dev/null || true
        fi
        rm -f "$pidf"
    fi
done

# Catch any stragglers (e.g. crash-restarted worker).
for pname in deltaforge-compute deltaforge-server; do
    if pgrep -f "$pname" >/dev/null 2>&1; then
        log "killing leftover $pname processes"
        pkill -f "$pname" 2>/dev/null || true
    fi
done

# ----- 2. stop postgres ------------------------------------------------------

if [ -n "${DF_STACK_PG_BIN:-}" ] && [ -n "${DF_STACK_PG_DATA:-}" ] \
   && [ -f "$DF_STACK_PG_DATA/postmaster.pid" ]; then
    log "stopping postgres at $DF_STACK_PG_DATA"
    "$DF_STACK_PG_BIN/pg_ctl" -D "$DF_STACK_PG_DATA" -m fast -w stop >/dev/null 2>&1 || true
fi

# ----- 3. odbc restore -------------------------------------------------------

if [ "$RESTORE_ODBC" -eq 1 ]; then
    for f in "$HOME/.odbcinst.ini" "$HOME/.odbc.ini"; do
        if [ -f "$f.bench-backup" ]; then
            log "restoring $f from .bench-backup"
            mv "$f.bench-backup" "$f"
        elif [ -f "$f" ]; then
            log "no .bench-backup for $f; leaving in place"
        fi
    done
fi

# ----- 4. purge --------------------------------------------------------------

if [ "$PURGE" -eq 1 ]; then
    log "purging $DF_HOME (data + config + logs)"
    rm -rf "$DF_HOME"
    log "purge done. Next setup-host-stack.sh will re-bootstrap from scratch."
else
    log "kept $DF_HOME on disk. Re-run setup-host-stack.sh to bring the stack back up without re-bootstrapping."
fi
