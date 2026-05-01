#!/bin/bash
# delta-forge-benchmarks container entrypoint.
#
# Sequence:
#   1. Start Postgres (apt-installed pg-${POSTGRES_MAJOR}, fresh data dir per run).
#   2. Create the deltaforge role + database.
#   3. Set the headless bootstrap env contract (DELTA_FORGE_DB_URL, ADMIN_PASSWORD,
#      ENGINEER_PASSWORD) and start delta-forge-server. The bootstrap orchestrator
#      provisions the schema and writes config.toml on first start.
#   4. Wait for control-plane /health on :3000.
#   5. Start delta-forge-worker. Wait for compute /health on :3031.
#   6. exec the CMD (defaults to `sleep infinity` so `docker compose exec bench ...`
#      works for interactive runs of bench_runner.py).
#
# Logs from each engine land in /var/log/deltaforge/ so the bench harness
# can copy them into per-run results/ directories.

set -euo pipefail

LOG_DIR="${LOG_DIR:-/var/log/deltaforge}"
mkdir -p "$LOG_DIR"

# ----- 1. Postgres ------------------------------------------------------------

PGBIN="/usr/lib/postgresql/${POSTGRES_MAJOR:-15}/bin"
export PGDATA="${PGDATA:-/var/lib/postgresql/data}"
export PGPORT="${PGPORT:-5432}"

if [ ! -s "${PGDATA}/PG_VERSION" ]; then
    echo "[entrypoint] initdb at ${PGDATA}"
    su -p -s /bin/bash postgres -c \
        "${PGBIN}/initdb -D ${PGDATA} --auth=trust --username=postgres --encoding=UTF8 --locale=C"
fi

echo "[entrypoint] starting postgres on :${PGPORT}"
su -p -s /bin/bash postgres -c \
    "${PGBIN}/pg_ctl -D ${PGDATA} -l ${LOG_DIR}/postgres.log -o '-c listen_addresses=127.0.0.1 -c port=${PGPORT}' -w start"

# Idempotent role + db creation. Fresh data dir means these will create on
# first start and be no-ops on subsequent restarts of the same container.
PGUSER_DF="${PGUSER:-deltaforge}"
PGDB_DF="${PGDATABASE:-deltaforge}"
PGPASSWORD_DF="${PGPASSWORD:-deltaforge_local}"

su -p -s /bin/bash postgres -c "${PGBIN}/psql -p ${PGPORT} -tAc \"SELECT 1 FROM pg_roles WHERE rolname='${PGUSER_DF}'\"" \
    | grep -q 1 || \
    su -p -s /bin/bash postgres -c \
        "${PGBIN}/psql -p ${PGPORT} -c \"CREATE ROLE ${PGUSER_DF} WITH LOGIN PASSWORD '${PGPASSWORD_DF}' SUPERUSER\""

su -p -s /bin/bash postgres -c "${PGBIN}/psql -p ${PGPORT} -tAc \"SELECT 1 FROM pg_database WHERE datname='${PGDB_DF}'\"" \
    | grep -q 1 || \
    su -p -s /bin/bash postgres -c \
        "${PGBIN}/psql -p ${PGPORT} -c \"CREATE DATABASE ${PGDB_DF} OWNER ${PGUSER_DF}\""

echo "[entrypoint] postgres ready: postgres://${PGUSER_DF}@127.0.0.1:${PGPORT}/${PGDB_DF}"

# ----- 2. DeltaForge headless bootstrap env contract -------------------------
#
# Source: delta-forge-bootstrap/src/inputs.rs:12-23
# Only the three required vars are set unconditionally; optional ones are
# threaded through if the user set them in docker/.env.

export DELTA_FORGE_DB_URL="postgres://${PGUSER_DF}:${PGPASSWORD_DF}@127.0.0.1:${PGPORT}/${PGDB_DF}"
export DELTA_FORGE_ADMIN_PASSWORD="${DELTA_FORGE_ADMIN_PASSWORD:-bench_admin_local}"
export DELTA_FORGE_ENGINEER_PASSWORD="${DELTA_FORGE_ENGINEER_PASSWORD:-bench_engineer_local}"
export DELTA_FORGE_CONFIG_DIR="${DELTA_FORGE_CONFIG_DIR:-/var/lib/deltaforge}"
export DELTA_FORGE_BIND_ADDR="${DELTA_FORGE_BIND_ADDR:-127.0.0.1:3000}"

mkdir -p "${DELTA_FORGE_CONFIG_DIR}"

# ----- 3. Start delta-forge-server (control plane) ----------------------------

CONTROL_HEALTH_URL="${CONTROL_HEALTH_URL:-http://127.0.0.1:3000/health}"
COMPUTE_HEALTH_URL="${COMPUTE_HEALTH_URL:-http://127.0.0.1:3031/health}"

echo "[entrypoint] starting delta-forge-server (bind ${DELTA_FORGE_BIND_ADDR})"
nohup delta-forge-server > "${LOG_DIR}/control.log" 2>&1 &
SERVER_PID=$!
echo $SERVER_PID > /var/run/delta-forge-server.pid

wait_for_health() {
    local name="$1" url="$2" timeout="${3:-90}"
    local started
    started=$(date +%s)
    while true; do
        if curl -fsS "$url" > /dev/null 2>&1; then
            echo "[entrypoint] ${name} healthy: ${url}"
            return 0
        fi
        if [ "$(($(date +%s) - started))" -ge "$timeout" ]; then
            echo "[entrypoint] ${name} did not become healthy within ${timeout}s" >&2
            echo "[entrypoint] last 40 lines of ${LOG_DIR}/${name}.log:" >&2
            tail -40 "${LOG_DIR}/${name}.log" >&2 || true
            return 1
        fi
        sleep 1
    done
}

wait_for_health control "$CONTROL_HEALTH_URL" 120

# ----- 4. Start delta-forge-worker (compute) ----------------------------------
#
# The released worker hardcodes ComputeMode::Remote and reads its env from
# delta-forge-compute/src/bin/worker.rs. Required:
#   CONTROL_PLANE_URL   -- where the control plane listens
#   NODE_SECRET         -- bootstrap auth token (or BOOTSTRAP_TOKEN alias)
#   BIND_HOST/BIND_PORT -- where the worker exposes its compute API
# Optional but commonly set:
#   NODE_ID, MAX_CONCURRENT_QUERIES, QUERY_TIMEOUT_SECS, WORKER_THREADS,
#   MEMORY_LIMIT_BYTES, ENABLE_RESULT_CACHE, HEARTBEAT_INTERVAL_SECS,
#   DISPLAY_NAME

export CONTROL_PLANE_URL="${CONTROL_PLANE_URL:-http://127.0.0.1:3000}"
export BIND_HOST="${BIND_HOST:-127.0.0.1}"
export BIND_PORT="${BIND_PORT:-3031}"
export NODE_SECRET="${NODE_SECRET:-${BOOTSTRAP_TOKEN:-bench_node_local}}"
export NODE_ID="${NODE_ID:-bench-local}"
export DISPLAY_NAME="${DISPLAY_NAME:-Bench Local}"

echo "[entrypoint] starting delta-forge-worker (control=${CONTROL_PLANE_URL}, bind=${BIND_HOST}:${BIND_PORT})"
nohup delta-forge-worker > "${LOG_DIR}/worker.log" 2>&1 &
WORKER_PID=$!
echo $WORKER_PID > /var/run/delta-forge-worker.pid

wait_for_health worker "$COMPUTE_HEALTH_URL" 60 || true
# Worker /health is best-effort: if the worker binary doesn't expose /health
# at this version, fall back to a port-listen probe.
if ! curl -fsS "$COMPUTE_HEALTH_URL" > /dev/null 2>&1; then
    if ss -tlnp 2>/dev/null | grep -q ":${BIND_PORT} "; then
        echo "[entrypoint] worker listening on :${BIND_PORT} (no /health endpoint)"
    else
        echo "[entrypoint] worker is not listening on :${BIND_PORT}" >&2
        tail -40 "${LOG_DIR}/worker.log" >&2 || true
        exit 1
    fi
fi

echo "[entrypoint] bootstrap complete"
echo "[entrypoint] control_url=http://127.0.0.1:3000"
echo "[entrypoint] compute_url=http://127.0.0.1:3031"
echo "[entrypoint] db_url=${DELTA_FORGE_DB_URL}"

# ----- 5. Hand off to CMD -----------------------------------------------------

exec "$@"
