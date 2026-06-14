#!/usr/bin/env bash
# driver-bench: tear down what setup-host-stack.sh set up.
#
# Under the platform model, driver-bench does not own a postgres/server/worker
# stack. The only process it may have started is the DeltaForge platform, and
# only when nothing was already serving (recorded as DF_STACK_PLATFORM_PID in
# stack.env). A platform that was already running (your desktop app or the
# parent ./bench) is left untouched.
#
# Default: stop the platform this bench launched (if any), keep $DF_HOME.
# --purge        also remove $DF_HOME (logs, zone storage, stack.env).
# --restore-odbc put back the ~/.odbc.ini / ~/.odbcinst.ini saved as *.bench-backup.

set -eu
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

PURGE=0; RESTORE_ODBC=0
for arg in "$@"; do
    case "$arg" in
        --purge)        PURGE=1 ;;
        --restore-odbc) RESTORE_ODBC=1 ;;
        --help|-h) grep '^#' "$0" | sed 's/^# \{0,1\}//' | head -20; exit 0 ;;
        *) echo "unknown arg: $arg" >&2; exit 2 ;;
    esac
done

log()  { printf '\033[1;36m[stack-down] %s\033[0m\n' "$*"; }

DF_HOME="${DF_HOME:-/tmp/df-bench-stack}"
[ -f "$DF_HOME/stack.env" ] && set -a && . "$DF_HOME/stack.env" && set +a

# ----- 1. stop the platform we launched (only if we launched it) -------------

PID="${DF_STACK_PLATFORM_PID:-}"
[ -z "$PID" ] && [ -f "$DF_HOME/platform.pid" ] && PID="$(cat "$DF_HOME/platform.pid" 2>/dev/null || true)"
if [ -n "$PID" ] && kill -0 "$PID" 2>/dev/null; then
    log "stopping the platform this bench started (pid $PID)"
    kill "$PID" 2>/dev/null || true
    for _ in 1 2 3 4 5; do kill -0 "$PID" 2>/dev/null || break; sleep 1; done
    kill -0 "$PID" 2>/dev/null && kill -9 "$PID" 2>/dev/null || true
else
    log "no bench-started platform to stop (an already-running instance is left untouched)."
fi
rm -f "$DF_HOME/platform.pid"

# ----- 2. odbc restore -------------------------------------------------------

if [ "$RESTORE_ODBC" -eq 1 ]; then
    for f in "$HOME/.odbcinst.ini" "$HOME/.odbc.ini"; do
        if [ -f "$f.bench-backup" ]; then
            log "restoring $f from .bench-backup"
            mv "$f.bench-backup" "$f"
        fi
    done
fi

# ----- 3. purge --------------------------------------------------------------

if [ "$PURGE" -eq 1 ]; then
    log "purging $DF_HOME"
    rm -rf "$DF_HOME"
else
    log "kept $DF_HOME on disk."
fi
