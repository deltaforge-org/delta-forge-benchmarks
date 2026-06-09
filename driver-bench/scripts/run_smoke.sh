#!/usr/bin/env bash
# driver-bench: quick smoke run on a self-provisioned stack.
#
# Scope: 100k rows, 1 warmup + 1 measured iter, C++ harness only,
# C++ all three modes (odbc-bound, odbc-getdata, adbc).
# Wall time: ~30s on a 16+ GB host once the stack is up.
#
# Use this immediately after install.sh + setup-host-stack.sh to confirm
# the toolchain works end-to-end before kicking off the canonical
# run_bench.sh.

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
BENCH_REPO="$(cd "$REPO_ROOT/.." && pwd)"
cd "$REPO_ROOT"

# ----- env -------------------------------------------------------------------

[ -f "$BENCH_REPO/.env" ] && set -a && . "$BENCH_REPO/.env" && set +a
[ -f "$REPO_ROOT/.env"   ] && set -a && . "$REPO_ROOT/.env"   && set +a

DF_HOME="${DF_HOME:-/tmp/df-bench-stack}"
if [ ! -f "$DF_HOME/stack.env" ]; then
    echo "[smoke] ERROR stack not provisioned. Run ./scripts/setup-host-stack.sh first." >&2
    exit 1
fi
set -a; . "$DF_HOME/stack.env"; set +a

log() { printf "\033[1;36m[smoke] %s\033[0m\n" "$*"; }

# ----- build harness if needed -----------------------------------------------

if [ ! -x "$REPO_ROOT/build/driver_bench" ]; then
    log "configuring + building driver_bench (first run)"
    cmake -S "$REPO_ROOT" -B "$REPO_ROOT/build" -DCMAKE_BUILD_TYPE=Release >/dev/null
    cmake --build "$REPO_ROOT/build" -j >/dev/null
fi

# ----- build fixture (small) -------------------------------------------------

DRIVER_BENCH_ROWS=100000 "$SCRIPT_DIR/build-fixture.sh"

# ----- run -------------------------------------------------------------------

mkdir -p "$REPO_ROOT/results"
TAG="smoke-$(date -u +%Y%m%dT%H%M%SZ)"
JSON_OUT="$REPO_ROOT/results/${TAG}.json"
LOG_OUT="$REPO_ROOT/results/${TAG}.log"

log "starting C++ bench: 100k rows, 1 warmup + 1 iter, all modes (~30s)"

"$REPO_ROOT/build/driver_bench" \
    --warmups 1 --iters 1 \
    --sql "SELECT * FROM ${DRIVER_BENCH_TABLE:-t_wide}" \
    --odbc-dsn "$DF_STACK_DSN" \
    --adbc-uri "$DF_STACK_CTRL_URL" \
    --adbc-compute "$DF_STACK_COMPUTE_URL" \
    --adbc-user "$DF_STACK_ADMIN_EMAIL" \
    --adbc-pwd  "$DF_STACK_ADMIN_PWD" \
    --adbc-so   "$DF_STACK_ADBC_SO" \
    --json-out  "$JSON_OUT" 2>&1 | tee "$LOG_OUT"

echo
log "done."
log "  JSON: $JSON_OUT"
log "  log : $LOG_OUT"
log "  next: ./scripts/run_bench.sh for the full publishable run"
