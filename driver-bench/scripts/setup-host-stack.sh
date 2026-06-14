#!/usr/bin/env bash
# driver-bench: point the bench at a DeltaForge platform and configure ODBC.
#
# The new model uses the released DeltaForge **platform** (which embeds the
# control plane, the compute node, and the database in one process) instead of
# provisioning a standalone postgres + server + worker stack. This script:
#
#   1. finds a reachable control plane (default http://127.0.0.1:3000); if none
#      is up and the parent installer staged a platform under ../.engine, it
#      launches that platform headless and waits for it to become healthy,
#   2. authenticates as the admin user,
#   3. ensures the bench zone exists,
#   4. writes the unixODBC driver + DSN entries, and
#   5. writes $DF_HOME/stack.env for run_smoke.sh / run_bench.sh / build-fixture.sh.
#
# Prereqs:
#   - ./scripts/install.sh            (unixODBC, cmake, .NET, jq)
#   - ./scripts/stage-driver-bins.sh  (downloads the released ODBC + ADBC .so)
#   - a DeltaForge platform: either already running (your desktop app, or the
#     parent repo's `./bench`), or installed by the parent `./install.sh` so
#     this script can launch it.
#
# Idempotent. Exit codes: 11 control plane unreachable, 13 auth failed, 14 zone.

set -eu
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
BENCH_REPO="$(cd "$REPO_ROOT/.." && pwd)"
cd "$REPO_ROOT"

log()  { printf '\033[1;36m[stack-up] %s\033[0m\n' "$*"; }
warn() { printf '\033[1;33m[stack-up] WARN %s\033[0m\n' "$*" >&2; }
fail() { printf '\033[1;31m[stack-up] ERROR %s\033[0m\n' "$*" >&2; exit "${2:-1}"; }

# Parent installer's .env carries DF_CONTROL_URL / DF_CLI_PATH / DF_PLATFORM_BIN
# / DF_USERNAME / DF_PASSWORD / DF_VERSION; driver-bench's own .env carries the
# host-tool paths. Source both (parent first so local can override).
[ -f "$BENCH_REPO/.env" ] && set -a && . "$BENCH_REPO/.env" && set +a
[ -f "$REPO_ROOT/.env"  ] && set -a && . "$REPO_ROOT/.env"  && set +a

DF_HOME="${DF_HOME:-/tmp/df-bench-stack}"
mkdir -p "$DF_HOME"/logs "$DF_HOME"/data

CTRL_URL="${DF_CTRL_URL:-${DF_CONTROL_URL:-http://127.0.0.1:3000}}"
# Compute API sits on the platform's compute port (3031 by default). Derive it
# from the control URL host, overridable via DF_COMPUTE_URL.
COMPUTE_URL="${DF_COMPUTE_URL:-$(printf '%s' "$CTRL_URL" | sed -E 's#:[0-9]+$#:3031#')}"

ADMIN_EMAIL="${DF_USERNAME:-${DELTA_FORGE_ADMIN_EMAIL:-admin@deltaforge.local}}"
ADMIN_PWD="${DF_PASSWORD:-${DELTA_FORGE_ADMIN_PASSWORD:-Benchmark_Admin1}}"
ZONE_NAME="${DRIVER_BENCH_ZONE:-bench}"
DSN_NAME="${DRIVER_BENCH_DSN:-deltaforge_bench}"
DRIVER_NAME="DeltaForgeBench"

CLI_BIN="${DF_CLI_PATH:-$BENCH_REPO/.engine/bin/deltaforge-cli}"

# ----- 0. drivers (subjects under test) --------------------------------------

DF_DRIVERS_DIR="${DF_DRIVERS_DIR:-$BENCH_REPO/build/df-drivers}"
ODBC_DRIVER_SO="$DF_DRIVERS_DIR/libdeltaforgeodbc.so"
ADBC_DRIVER_SO="$DF_DRIVERS_DIR/libdeltaforge_adbc.so"
if [ ! -f "$ODBC_DRIVER_SO" ] || [ ! -f "$ADBC_DRIVER_SO" ]; then
    fail "ODBC/ADBC driver .so files missing under $DF_DRIVERS_DIR. Run ./scripts/stage-driver-bins.sh first."
fi
log "drivers: $DF_DRIVERS_DIR"

# ----- 1. reach (or launch) the DeltaForge platform --------------------------

PLATFORM_PID=""
is_healthy() { curl -fsS "$CTRL_URL/api/v1/health" 2>/dev/null | grep -q healthy; }

if is_healthy; then
    log "using the DeltaForge platform already serving at $CTRL_URL"
else
    PLATFORM_BIN="${DF_PLATFORM_BIN:-$BENCH_REPO/.engine/squashfs-root/AppRun}"
    if [ ! -e "$PLATFORM_BIN" ]; then
        fail "no DeltaForge control plane at $CTRL_URL and no installed platform to launch.
  Start DeltaForge first (run the parent repo's ./bench, or open the desktop app),
  or set DF_CTRL_URL to a running instance, then re-run this script." 11
    fi
    log "launching the installed platform: $PLATFORM_BIN"
    # The platform is a desktop app: software-render (WebKit) and supply a
    # virtual display on a headless Linux host, mirroring the parent ./bench.
    if [ "$(uname -s)" = "Linux" ] && [ -z "${DISPLAY:-}" ] && command -v xvfb-run >/dev/null 2>&1; then
        env WEBKIT_DISABLE_DMABUF_RENDERER=1 WEBKIT_DISABLE_COMPOSITING_MODE=1 \
            xvfb-run -a "$PLATFORM_BIN" >>"$DF_HOME/logs/platform.log" 2>&1 < /dev/null &
    else
        env WEBKIT_DISABLE_DMABUF_RENDERER=1 WEBKIT_DISABLE_COMPOSITING_MODE=1 \
            "$PLATFORM_BIN" >>"$DF_HOME/logs/platform.log" 2>&1 < /dev/null &
    fi
    PLATFORM_PID=$!
    echo "$PLATFORM_PID" > "$DF_HOME/platform.pid"
    log "waiting for $CTRL_URL/api/v1/health (up to 120s)..."
    for i in $(seq 1 120); do
        if is_healthy; then log "platform healthy after ${i}s"; break; fi
        if ! kill -0 "$PLATFORM_PID" 2>/dev/null; then
            tail -30 "$DF_HOME/logs/platform.log" >&2 || true
            fail "platform exited during startup. See $DF_HOME/logs/platform.log" 11
        fi
        [ "$i" -eq 120 ] && { tail -30 "$DF_HOME/logs/platform.log" >&2 || true; fail "platform not healthy in 120s" 11; }
        sleep 1
    done
fi

# ----- 2. authenticate -------------------------------------------------------

if [ -n "${DF_TOKEN:-}" ]; then
    TOKEN="$DF_TOKEN"
    log "using DF_TOKEN for control-plane auth"
else
    command -v jq >/dev/null 2>&1 || fail "jq is required (run ./scripts/install.sh)" 13
    TOKEN="$(curl -fsS -X POST "$CTRL_URL/api/v1/auth/token" \
            -H 'Content-Type: application/json' \
            -d "{\"grant_type\":\"password\",\"username\":\"${ADMIN_EMAIL}\",\"password\":\"${ADMIN_PWD}\"}" \
        | jq -re '.access_token' 2>/dev/null || true)"
    [ -n "$TOKEN" ] && [ "$TOKEN" != "null" ] || fail \
        "could not authenticate as ${ADMIN_EMAIL} at $CTRL_URL.
  Set DF_USERNAME / DF_PASSWORD (or DF_TOKEN) for your instance and re-run." 13
    log "authenticated as ${ADMIN_EMAIL}"
fi

# ----- 3. bench zone ---------------------------------------------------------
# When this script launches a fresh platform, it self-activates at bootstrap from
# the DELTA_FORGE_LICENSE_KEY in the environment (the benchmark bundles no
# license), so no explicit activation step is needed here. When reusing an
# already-running platform, that instance is already licensed.

ZONE_PATH="$DF_HOME/data/${ZONE_NAME}"
mkdir -p "$ZONE_PATH"
ZONE_LIST="$(curl -fsS "$CTRL_URL/api/v1/catalog/zones" -H "Authorization: Bearer $TOKEN" 2>/dev/null || echo '[]')"
if printf '%s' "$ZONE_LIST" | jq -re --arg n "$ZONE_NAME" 'map(select(.name==$n)) | length' 2>/dev/null | grep -q '^0$'; then
    log "creating zone '$ZONE_NAME' at $ZONE_PATH"
    curl -fsS -X POST "$CTRL_URL/api/v1/catalog/zones" \
        -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
        -d "{\"name\":\"${ZONE_NAME}\",\"zone_type\":\"silver\",\"storage_root\":\"${ZONE_PATH}\"}" \
        >/dev/null || fail "could not create zone $ZONE_NAME" 14
else
    log "zone '$ZONE_NAME' already present"
fi

# ----- 4. unixODBC driver + DSN ----------------------------------------------

ODBCINST="$HOME/.odbcinst.ini"
ODBCDSN="$HOME/.odbc.ini"
[ -f "$ODBCINST" ] && [ ! -f "$ODBCINST.bench-backup" ] && cp "$ODBCINST" "$ODBCINST.bench-backup"
[ -f "$ODBCDSN"  ] && [ ! -f "$ODBCDSN.bench-backup"  ] && cp "$ODBCDSN"  "$ODBCDSN.bench-backup"

cat > "$ODBCINST" <<EOF
[ODBC]
Trace = No

[${DRIVER_NAME}]
Description    = DeltaForge ODBC Driver (driver-bench)
Driver         = ${ODBC_DRIVER_SO}
DriverODBCVer  = 03.80
Threading      = 2
EOF

cat > "$ODBCDSN" <<EOF
[${DSN_NAME}]
Description    = DeltaForge bench DSN
Driver         = ${DRIVER_NAME}
Server         = ${CTRL_URL}
ComputeServer  = ${COMPUTE_URL}
Uid            = ${ADMIN_EMAIL}
Pwd            = ${ADMIN_PWD}
TLS            = disabled
EOF
chmod 0600 "$ODBCDSN"
log "unixODBC: DSN=$DSN_NAME driver=$ODBC_DRIVER_SO"

# ----- 5. stack.env ----------------------------------------------------------

cat > "$DF_HOME/stack.env" <<EOF
# Generated by driver-bench/scripts/setup-host-stack.sh (platform model)
DF_STACK_HOME=$DF_HOME
DF_STACK_PLATFORM_PID=$PLATFORM_PID
DF_STACK_CTRL_URL=$CTRL_URL
DF_STACK_COMPUTE_URL=$COMPUTE_URL
DF_STACK_ADMIN_EMAIL=$ADMIN_EMAIL
DF_STACK_ADMIN_PWD=$ADMIN_PWD
DF_STACK_DSN=$DSN_NAME
DF_STACK_ZONE=$ZONE_NAME
DF_STACK_ODBC_SO=$ODBC_DRIVER_SO
DF_STACK_ADBC_SO=$ADBC_DRIVER_SO
DF_STACK_CLI_BIN=$CLI_BIN
EOF
log "stack env written to $DF_HOME/stack.env"
[ -n "$PLATFORM_PID" ] && log "platform launched by this script (pid $PLATFORM_PID); teardown stops it."
log "ready. Next: ./scripts/build-fixture.sh && ./scripts/run_bench.sh"
