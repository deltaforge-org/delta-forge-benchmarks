#!/usr/bin/env bash
# driver-bench: canonical bench run on a self-provisioned stack.
#
# What it runs:
#   - C++ harness: odbc-bound + odbc-getdata + adbc, 1 warmup + 3 iters
#   - .NET harness: System.Data.Odbc.OdbcDataReader vs Apache.Arrow.Adbc,
#     1 warmup + 3 iters
# Both harnesses scan the same fixture table built by build-fixture.sh.
#
# Default workload: 1,000,000 rows x 22 mixed-type columns. Wall time:
# ~2-5 min on a 16+ GB host once the stack is up.
#
# Args (all optional, env vars override):
#   $1 ROWS    default 1000000   rebuild fixture at this row count
#   $2 ITERS   default 3         measured iterations per driver
#   $3 WARMUPS default 1         discarded warmup iterations
#   $4 TAG     default bench-<utc>
#
# Examples:
#   ./scripts/run_bench.sh                          # canonical run
#   ./scripts/run_bench.sh 10000000                 # 10M rows
#   ./scripts/run_bench.sh 1000000 5 2 perf-tag     # tweak iters

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
    echo "[bench] ERROR stack not provisioned. Run ./scripts/setup-host-stack.sh first." >&2
    exit 1
fi
set -a; . "$DF_HOME/stack.env"; set +a

# ----- args ------------------------------------------------------------------

ROWS="${1:-${DRIVER_BENCH_ROWS:-1000000}}"
ITERS="${2:-${DRIVER_BENCH_ITERS:-3}}"
WARMUPS="${3:-${DRIVER_BENCH_WARMUPS:-1}}"
TAG="${4:-bench-$(date -u +%Y%m%dT%H%M%SZ)}"

log()  { printf "\033[1;36m[bench] %s\033[0m\n" "$*"; }
fail() { printf "\033[1;31m[bench] ERROR %s\033[0m\n" "$*" >&2; exit 1; }

# ----- build harness ---------------------------------------------------------

if [ ! -x "$REPO_ROOT/build/driver_bench" ] || \
   [ "$REPO_ROOT/CMakeLists.txt" -nt "$REPO_ROOT/build/driver_bench" ] || \
   find "$REPO_ROOT/src" -newer "$REPO_ROOT/build/driver_bench" -print -quit 2>/dev/null | grep -q .; then
    log "configuring + building driver_bench"
    cmake -S "$REPO_ROOT" -B "$REPO_ROOT/build" -DCMAKE_BUILD_TYPE=Release >/dev/null
    cmake --build "$REPO_ROOT/build" -j >/dev/null
fi

if [ -d "$REPO_ROOT/dotnet" ]; then
    log "building .NET harness"
    (cd "$REPO_ROOT/dotnet" && dotnet build -c Release --nologo --verbosity quiet)
fi

# ----- fixture ---------------------------------------------------------------

DRIVER_BENCH_ROWS="$ROWS" "$SCRIPT_DIR/build-fixture.sh"

# ----- run C++ harness -------------------------------------------------------

mkdir -p "$REPO_ROOT/results"
SUBDIR="$REPO_ROOT/results/${TAG}"
mkdir -p "$SUBDIR"

CPP_JSON="$SUBDIR/cpp.json"
CPP_LOG="$SUBDIR/cpp.log"
log "C++ harness: $ROWS rows, $WARMUPS warmup + $ITERS iters, all modes"
"$REPO_ROOT/build/driver_bench" \
    --warmups "$WARMUPS" --iters "$ITERS" \
    --sql "SELECT * FROM ${DRIVER_BENCH_TABLE:-t_wide}" \
    --odbc-dsn "$DF_STACK_DSN" \
    --adbc-uri "$DF_STACK_CTRL_URL" \
    --adbc-compute "$DF_STACK_COMPUTE_URL" \
    --adbc-user "$DF_STACK_ADMIN_EMAIL" \
    --adbc-pwd  "$DF_STACK_ADMIN_PWD" \
    --adbc-so   "$DF_STACK_ADBC_SO" \
    --json-out  "$CPP_JSON" 2>&1 | tee "$CPP_LOG"

# ----- run .NET harness ------------------------------------------------------

if [ -d "$REPO_ROOT/dotnet" ] && command -v dotnet >/dev/null 2>&1; then
    NET_JSON="$SUBDIR/dotnet.json"
    NET_LOG="$SUBDIR/dotnet.log"
    log ".NET harness: $ROWS rows, $WARMUPS warmup + $ITERS iters"
    (cd "$REPO_ROOT/dotnet" && dotnet run -c Release --no-build --no-restore -- \
        --warmups "$WARMUPS" --iters "$ITERS" \
        --sql "SELECT * FROM ${DRIVER_BENCH_TABLE:-t_wide}" \
        --odbc-dsn "$DF_STACK_DSN" \
        --adbc-uri "$DF_STACK_CTRL_URL" \
        --adbc-compute "$DF_STACK_COMPUTE_URL" \
        --adbc-user "$DF_STACK_ADMIN_EMAIL" \
        --adbc-pwd  "$DF_STACK_ADMIN_PWD" \
        --adbc-so   "$DF_STACK_ADBC_SO" \
        --json-out  "$NET_JSON" 2>&1 | tee "$NET_LOG")
else
    log "WARN dotnet not present; skipping .NET harness"
fi

# ----- summary ---------------------------------------------------------------

# Resolve the ODBC driver path the DSN refers to. Read from
# ~/.odbcinst.ini under the bench's driver section -- setup-host-stack.sh
# wrote that entry. Default to "unknown" if missing so the manifest
# still emits.
ODBC_DRIVER_SO_VAL="$(awk -v sect="[DeltaForgeBench]" '
    $0 == sect { in_section = 1; next }
    /^\[/ { in_section = 0 }
    in_section && $1 == "Driver" { print $3; exit }
' "$HOME/.odbcinst.ini" 2>/dev/null || true)"

cat > "$SUBDIR/manifest.json" <<EOF
{
  "tag": "$TAG",
  "rows": $ROWS,
  "iters": $ITERS,
  "warmups": $WARMUPS,
  "table": "${DRIVER_BENCH_TABLE:-t_wide}",
  "control_url": "$DF_STACK_CTRL_URL",
  "compute_url": "$DF_STACK_COMPUTE_URL",
  "odbc_driver_so": "${ODBC_DRIVER_SO_VAL:-unknown}",
  "adbc_driver_so": "$DF_STACK_ADBC_SO",
  "utc": "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
}
EOF

cat <<EOF

[bench] done. Artifacts under $SUBDIR:
  manifest.json
  cpp.json     cpp.log     (C++ harness, three driver modes)
  dotnet.json  dotnet.log  (.NET harness, OdbcDataReader vs Apache.Arrow.Adbc)

Headline ratios (cat cpp.log | grep 'vs ADBC' for the C++ comparison,
                  cat dotnet.log | grep 'vs ADBC' for .NET).
EOF
