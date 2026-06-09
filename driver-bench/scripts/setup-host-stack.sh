#!/usr/bin/env bash
# driver-bench: provision a self-contained DeltaForge stack on the host.
#
# Brings up Postgres (the embedded one that ships with DeltaForge), then
# delta-forge-server in two passes (headless bootstrap + serve), then
# delta-forge-compute (worker) against the control plane, activates the
# license, creates a zone for the bench fixture, and configures unixODBC.
#
# Mirrors docker/bench-entrypoint.sh in the parent benchmarks repo --
# same env-var contract (DELTA_FORGE_DB_URL, DELTA_FORGE_ADMIN_PASSWORD,
# DELTA_FORGE_LICENSE_KEY, etc.), same two-pass dance to work around the
# recursive instance-lock the server's cmd_run does after bootstrap.
#
# Idempotent: subsequent invocations skip steps that have already run.
# Exit codes:
#   0   stack healthy, ready for ./scripts/build-fixture.sh and run_bench.sh
#   10  postgres failed to come up
#   11  control plane failed /api/v1/health within 90s
#   12  worker failed /health within 60s
#   13  license activation failed
#   14  zone creation failed

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
BENCH_REPO="$(cd "$REPO_ROOT/.." && pwd)"
cd "$REPO_ROOT"

# ----- env contract ----------------------------------------------------------

# Source .env from the driver-bench AND the parent benchmarks repo. The
# parent's stage-local-bins.sh writes DF_VERSION there; install.sh here
# writes DOTNET_ROOT / ODBC_INC / ODBC_LIB.
[ -f "$BENCH_REPO/.env" ] && set -a && . "$BENCH_REPO/.env" && set +a
[ -f "$REPO_ROOT/.env"   ] && set -a && . "$REPO_ROOT/.env"   && set +a

DF_HOME="${DF_HOME:-/tmp/df-bench-stack}"
PG_DATA="${PG_DATA:-${DF_HOME}/pg-data}"
PG_SOCK="${PG_SOCK:-${DF_HOME}/pg-sock}"
PG_PORT="${PG_PORT:-55432}"
PG_PASSWORD="${PG_PASSWORD:-deltaforge_bench}"

CTRL_PORT="${CTRL_PORT:-13000}"
COMPUTE_PORT="${COMPUTE_PORT:-13031}"
CTRL_URL="http://127.0.0.1:${CTRL_PORT}"
COMPUTE_URL="http://127.0.0.1:${COMPUTE_PORT}"

ADMIN_EMAIL="${DELTA_FORGE_ADMIN_EMAIL:-admin@deltaforge.local}"
# Defaults must satisfy the engine's password policy: at least one digit,
# at least one uppercase letter, length >= 12. Operators wanting a
# different password set DELTA_FORGE_ADMIN_PASSWORD in their environment.
ADMIN_PWD="${DELTA_FORGE_ADMIN_PASSWORD:-Bench_admin_2026}"
ENGINEER_PWD="${DELTA_FORGE_ENGINEER_PASSWORD:-Bench_engineer_2026}"

NODE_SECRET="${NODE_SECRET:-bench_node_local}"
NODE_ID="${NODE_ID:-driver-bench-local}"

ZONE_NAME="${DRIVER_BENCH_ZONE:-bench}"

# Helper accessors --------------------------------------------------------------

log()  { printf "\033[1;36m[stack-up] %s\033[0m\n" "$*"; }
warn() { printf "\033[1;33m[stack-up] WARN %s\033[0m\n" "$*" >&2; }
fail() { printf "\033[1;31m[stack-up] ERROR %s\033[0m\n" "$*" >&2; exit "${2:-1}"; }

# ----- 0. preflight ----------------------------------------------------------

if [ -z "${DELTA_FORGE_LICENSE_KEY:-}" ]; then
    fail "DELTA_FORGE_LICENSE_KEY not set. Free key at https://console.deltaforge.org -- export it before re-running."
fi

# Engine binaries: prefer the parent benchmarks repo's build/df-bins/
# (populated by stage-local-bins.sh or by a release download), fall back
# to whatever the operator pointed DF_BINS_DIR at.
DF_BINS_DIR="${DF_BINS_DIR:-$BENCH_REPO/build/df-bins}"
for bin in deltaforge-server deltaforge-compute deltaforge-cli; do
    if [ ! -x "$DF_BINS_DIR/$bin" ]; then
        # Some release tarballs unpack with the "delta-forge-*" naming; try both.
        if [ -x "$DF_BINS_DIR/delta-forge-${bin#deltaforge-}" ]; then
            ln -sf "$DF_BINS_DIR/delta-forge-${bin#deltaforge-}" "$DF_BINS_DIR/$bin"
        fi
    fi
    if [ ! -x "$DF_BINS_DIR/$bin" ]; then
        fail "$bin not found under $DF_BINS_DIR. Run ../scripts/stage-local-bins.sh first, or set DF_BINS_DIR to point at a release-extracted dir."
    fi
done
SERVER_BIN="$DF_BINS_DIR/deltaforge-server"
COMPUTE_BIN="$DF_BINS_DIR/deltaforge-compute"
CLI_BIN="$DF_BINS_DIR/deltaforge-cli"
log "engine binaries: $DF_BINS_DIR"

# Driver .so files (the subjects under test).
DF_DRIVERS_DIR="${DF_DRIVERS_DIR:-$BENCH_REPO/build/df-drivers}"
ODBC_DRIVER_SO="${DF_DRIVERS_DIR}/libdeltaforgeodbc.so"
ADBC_DRIVER_SO="${DF_DRIVERS_DIR}/libdeltaforge_adbc.so"
if [ ! -f "$ODBC_DRIVER_SO" ] || [ ! -f "$ADBC_DRIVER_SO" ]; then
    fail "ODBC/ADBC driver .so files missing under $DF_DRIVERS_DIR. Run ./scripts/stage-driver-bins.sh."
fi
log "drivers: $DF_DRIVERS_DIR"

# Embedded postgres bundled with DeltaForge desktop installs.
PG_BIN="${PG_BIN:-/home/$(id -un)/.deltaforge/pg-install/18.3.0/bin}"
if [ ! -x "$PG_BIN/pg_ctl" ]; then
    # Fall back to system postgres if available.
    if command -v pg_ctl >/dev/null 2>&1; then
        PG_BIN="$(dirname "$(command -v pg_ctl)")"
    else
        fail "no embedded postgres at $PG_BIN and no system postgres on PATH"
    fi
fi
log "postgres: $PG_BIN ($("$PG_BIN/postgres" --version | head -1))"

mkdir -p "$DF_HOME"/{config,logs,data} "$PG_SOCK"

# ----- 1. postgres -----------------------------------------------------------

# Bring up an isolated postgres on $PG_PORT. Skip if already running with
# a server we control (PID file present + responsive).
if [ -f "$PG_DATA/postmaster.pid" ] \
   && "$PG_BIN/pg_isready" -h "$PG_SOCK" -p "$PG_PORT" >/dev/null 2>&1; then
    log "postgres already up on :${PG_PORT}"
else
    if [ ! -s "$PG_DATA/PG_VERSION" ]; then
        log "initdb $PG_DATA"
        "$PG_BIN/initdb" -D "$PG_DATA" --auth=trust --username=postgres \
            --encoding=UTF8 --locale=C >/dev/null
    fi
    log "starting postgres on :${PG_PORT}"
    "$PG_BIN/pg_ctl" -D "$PG_DATA" -l "$DF_HOME/logs/postgres.log" \
        -o "-c port=${PG_PORT} -c listen_addresses=127.0.0.1 -c unix_socket_directories=${PG_SOCK}" \
        -w start >/dev/null \
        || fail "postgres did not start; see $DF_HOME/logs/postgres.log" 10
fi

# Create the deltaforge role + database (idempotent).
PSQL() { "$PG_BIN/psql" -h "$PG_SOCK" -p "$PG_PORT" -U postgres -d postgres -tAc "$1" 2>/dev/null; }
if [ "$(PSQL "SELECT 1 FROM pg_roles WHERE rolname='deltaforge'")" != "1" ]; then
    PSQL "CREATE ROLE deltaforge WITH LOGIN PASSWORD '${PG_PASSWORD}' SUPERUSER" >/dev/null \
        || fail "could not create role deltaforge" 10
fi
if [ "$(PSQL "SELECT 1 FROM pg_database WHERE datname='deltaforge'")" != "1" ]; then
    PSQL "CREATE DATABASE deltaforge OWNER deltaforge" >/dev/null \
        || fail "could not create database deltaforge" 10
fi
log "postgres ready: postgres://deltaforge@127.0.0.1:${PG_PORT}/deltaforge"

# ----- 2. delta-forge-server (control plane) ---------------------------------

export DELTA_FORGE_CONFIG_DIR="$DF_HOME/config"
export DELTA_FORGE_DB_URL="postgres://deltaforge:${PG_PASSWORD}@127.0.0.1:${PG_PORT}/deltaforge"
export DELTA_FORGE_ADMIN_PASSWORD="$ADMIN_PWD"
export DELTA_FORGE_ENGINEER_PASSWORD="$ENGINEER_PWD"
export DELTA_FORGE_BIND_ADDR="127.0.0.1:${CTRL_PORT}"
export DELTA_FORGE_LICENSE_KEY="$DELTA_FORGE_LICENSE_KEY"

# Pass 1: bootstrap. The server writes config.toml then tries to recursively
# re-enter cmd_run while still holding the instance lock; the inner call
# fails to acquire it and exits non-zero. config.toml is already on disk
# at that point, so we just clear the stale lock and move on. Mirrors
# the comment block in ../docker/bench-entrypoint.sh.
if [ ! -f "$DELTA_FORGE_CONFIG_DIR/config.toml" ]; then
    log "pass 1: headless bootstrap"
    "$SERVER_BIN" >> "$DF_HOME/logs/control.log" 2>&1 || true
    if [ ! -f "$DELTA_FORGE_CONFIG_DIR/config.toml" ]; then
        tail -30 "$DF_HOME/logs/control.log" >&2
        fail "pass 1 did not write config.toml" 11
    fi
fi
rm -f "$DELTA_FORGE_CONFIG_DIR/bootstrap.lock"

# Pass 2: serve. Start in background, wait for /api/v1/health.
if ! pgrep -f "${SERVER_BIN}" >/dev/null 2>&1; then
    log "pass 2: starting server in serve mode"
    nohup "$SERVER_BIN" >> "$DF_HOME/logs/control.log" 2>&1 < /dev/null &
    echo $! > "$DF_HOME/server.pid"
fi

log "waiting for control plane /api/v1/health on :${CTRL_PORT}..."
for i in $(seq 1 90); do
    if curl -fsS "$CTRL_URL/api/v1/health" 2>/dev/null | grep -q healthy; then
        log "control plane healthy after ${i}s"
        break
    fi
    if [ "$i" -eq 90 ]; then
        tail -30 "$DF_HOME/logs/control.log" >&2
        fail "control plane did not become healthy in 90s" 11
    fi
    sleep 1
done

# ----- 3. delta-forge-compute (worker) ---------------------------------------

if ! pgrep -f "${COMPUTE_BIN}" >/dev/null 2>&1; then
    log "starting worker on :${COMPUTE_PORT}"
    CONTROL_PLANE_URL="$CTRL_URL" \
    BIND_PORT="${COMPUTE_PORT}" \
    NODE_SECRET="$NODE_SECRET" \
    NODE_ID="$NODE_ID" \
    DISPLAY_NAME="${DISPLAY_NAME:-Driver Bench Worker}" \
        nohup "$COMPUTE_BIN" >> "$DF_HOME/logs/worker.log" 2>&1 < /dev/null &
    echo $! > "$DF_HOME/worker.pid"
fi

log "waiting for worker /health on :${COMPUTE_PORT}..."
for i in $(seq 1 60); do
    if curl -fsS "$COMPUTE_URL/health" 2>/dev/null | grep -q healthy; then
        log "worker healthy after ${i}s"
        break
    fi
    if [ "$i" -eq 60 ]; then
        tail -30 "$DF_HOME/logs/worker.log" >&2
        fail "worker did not become healthy in 60s" 12
    fi
    sleep 1
done

# ----- 4. license activation -------------------------------------------------

log "activating license..."
TOKEN="$(curl -fsS -X POST "$CTRL_URL/api/v1/auth/token" \
        -H 'Content-Type: application/json' \
        -d "{\"grant_type\":\"password\",\"username\":\"${ADMIN_EMAIL}\",\"password\":\"${ADMIN_PWD}\"}" \
    | jq -re '.access_token')" \
    || fail "could not obtain admin access token" 13
ACT_RESP="$(curl -s -X POST "$CTRL_URL/api/v1/license/activate" \
    -H "Authorization: Bearer $TOKEN")"
ACT_STATUS="$(echo "$ACT_RESP" | jq -re '.activationStatus' 2>/dev/null || echo unknown)"
if [ "$ACT_STATUS" != "activated" ]; then
    echo "$ACT_RESP" | head -c 500 >&2; echo >&2
    fail "license activation returned status=$ACT_STATUS" 13
fi
log "license activated: $(echo "$ACT_RESP" | jq -r '"\(.tier) tier, \(.activationsUsed)/\(.activationsMax) seats, expires \(.expiresAt)"')"

# ----- 5. bench zone ---------------------------------------------------------

ZONE_PATH="$DF_HOME/data/${ZONE_NAME}"
mkdir -p "$ZONE_PATH"
ZONE_LIST="$(curl -fsS "$CTRL_URL/api/v1/catalog/zones" -H "Authorization: Bearer $TOKEN")"
if echo "$ZONE_LIST" | jq -re --arg n "$ZONE_NAME" 'map(select(.name==$n)) | length' | grep -q '^0$'; then
    log "creating zone '$ZONE_NAME' at $ZONE_PATH"
    curl -fsS -X POST "$CTRL_URL/api/v1/catalog/zones" \
        -H "Authorization: Bearer $TOKEN" \
        -H 'Content-Type: application/json' \
        -d "{\"name\":\"${ZONE_NAME}\",\"zone_type\":\"silver\",\"storage_root\":\"${ZONE_PATH}\"}" \
        >/dev/null \
        || fail "could not create zone $ZONE_NAME" 14
else
    log "zone '$ZONE_NAME' already exists"
fi

# ----- 6. unixODBC DSN -------------------------------------------------------

# Write driver registration + DSN entries to user-local config files. We
# back up any pre-existing config the FIRST time (idempotent: subsequent
# runs do not clobber the backup).

DSN_NAME="${DRIVER_BENCH_DSN:-deltaforge_bench}"
DRIVER_NAME="DeltaForgeBench"
ODBCINST="$HOME/.odbcinst.ini"
ODBCDSN="$HOME/.odbc.ini"
[ -f "$ODBCINST" ] && [ ! -f "$ODBCINST.bench-backup" ] && cp "$ODBCINST" "$ODBCINST.bench-backup"
[ -f "$ODBCDSN"  ] && [ ! -f "$ODBCDSN.bench-backup" ] && cp "$ODBCDSN" "$ODBCDSN.bench-backup"

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
Description    = DeltaForge bench DSN (self-provisioned)
Driver         = ${DRIVER_NAME}
Server         = ${CTRL_URL}
ComputeServer  = ${COMPUTE_URL}
Uid            = ${ADMIN_EMAIL}
Pwd            = ${ADMIN_PWD}
TLS            = disabled
EOF
chmod 0600 "$ODBCDSN"
log "unixODBC: DSN=$DSN_NAME driver=$ODBC_DRIVER_SO"

# ----- 7. summary ------------------------------------------------------------

cat > "$DF_HOME/stack.env" <<EOF
# Generated by driver-bench/scripts/setup-host-stack.sh
DF_STACK_HOME=$DF_HOME
DF_STACK_PIDS_SERVER=$(cat "$DF_HOME/server.pid" 2>/dev/null || echo "")
DF_STACK_PIDS_WORKER=$(cat "$DF_HOME/worker.pid" 2>/dev/null || echo "")
DF_STACK_CTRL_URL=$CTRL_URL
DF_STACK_COMPUTE_URL=$COMPUTE_URL
DF_STACK_PG_PORT=$PG_PORT
DF_STACK_PG_DATA=$PG_DATA
DF_STACK_PG_SOCK=$PG_SOCK
DF_STACK_PG_BIN=$PG_BIN
DF_STACK_ADMIN_EMAIL=$ADMIN_EMAIL
DF_STACK_ADMIN_PWD=$ADMIN_PWD
DF_STACK_DSN=$DSN_NAME
DF_STACK_ZONE=$ZONE_NAME
DF_STACK_ADBC_SO=$ADBC_DRIVER_SO
DF_STACK_CLI_BIN=$CLI_BIN
EOF
log "stack env written to $DF_HOME/stack.env"
log "ready. Next: ./scripts/build-fixture.sh && ./scripts/run_bench.sh"
