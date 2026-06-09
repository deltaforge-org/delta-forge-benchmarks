#!/usr/bin/env bash
# driver-bench: runs INSIDE the docker container after the parent
# bench-entrypoint.sh has finished bringing up Postgres + the DeltaForge
# control plane + the worker on 127.0.0.1. Mirrors the host-side
# setup-host-stack.sh + build-fixture.sh + run_bench.sh flow but skips
# the postgres/server/worker boot because the parent entrypoint owns
# that lifecycle.
#
# Steps:
#   1. Smoke-check control-plane and worker /health (parent entrypoint
#      already waited, but defence-in-depth: we may have raced).
#   2. Use delta-forge-cli to verify the engine is accepting queries.
#   3. Activate the license through the HTTP API (the parent entrypoint
#      installs the license but does not activate it; activation needs
#      an admin bearer token, which only the post-bootstrap server can
#      issue).
#   4. Create the bench zone via the catalog HTTP API.
#   5. Configure unixODBC: /etc/odbcinst.ini (driver registration) +
#      /etc/odbc.ini (DSN with the bootstrap-provisioned admin password).
#   6. Configure-and-build driver_bench from the bind-mounted source.
#   7. Build the fixture table via ODBC CTAS. The standalone server's
#      CLI path cannot resolve user-created zones in DDL (returns
#      "failed to resolve catalog"); the ODBC driver routes CTAS through
#      a different planner entrypoint that does honor user zones, so
#      this is the load-bearing fixture path on a self-provisioned stack.
#   8. Run driver_bench (all three modes) and the .NET harness, emit
#      JSON + log per run into /workspace/driver-bench/results/.
#
# Exit codes:
#   10  control plane never returned healthy
#   11  delta-forge-cli smoke test failed (engine + auth + worker chain)
#   13  license activation failed
#   14  zone creation failed
#   20  driver_bench cmake / build failed
#   21  fixture CTAS failed
#   30  bad DRIVER_BENCH_DRIVER value

set -euo pipefail

# ----- knobs (overridable from docker-compose environment) -------------------

CTRL_URL="${CONTROL_PLANE_URL:-http://127.0.0.1:3000}"
COMPUTE_URL="${COMPUTE_URL:-http://127.0.0.1:3031}"
ADMIN_EMAIL="${DELTA_FORGE_ADMIN_EMAIL:-admin@deltaforge.local}"
ADMIN_PWD="${DELTA_FORGE_ADMIN_PASSWORD:-bench_admin_local}"
ROWS="${DRIVER_BENCH_ROWS:-1000000}"
WARMUPS="${DRIVER_BENCH_WARMUPS:-1}"
ITERS="${DRIVER_BENCH_ITERS:-3}"
WHICH="${DRIVER_BENCH_DRIVER:-both}"
TABLE="${DRIVER_BENCH_TABLE:-t_wide}"
ZONE_NAME="${DRIVER_BENCH_ZONE:-bench}"
DSN_NAME="${DRIVER_BENCH_DSN:-deltaforge_bench}"

BENCH_DIR="/workspace/driver-bench"
BUILD_DIR="${BENCH_DIR}/build"
RESULTS_DIR="${BENCH_DIR}/results"
ADBC_SO="${ADBC_SO:-/usr/local/lib/libdeltaforge_adbc.so}"

mkdir -p "$RESULTS_DIR"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
SUBDIR="$RESULTS_DIR/docker-${STAMP}"
mkdir -p "$SUBDIR"
LOG="$SUBDIR/run.log"
exec > >(tee -a "$LOG") 2>&1

echo "[run-in-container] DeltaForge driver-bench"
echo "  control = $CTRL_URL"
echo "  compute = $COMPUTE_URL"
echo "  driver  = $WHICH"
echo "  rows    = $ROWS"
echo "  iters   = $ITERS (+ $WARMUPS warmups)"
echo "  table   = $TABLE"
echo "  zone    = $ZONE_NAME"

# ----- 1. health checks ------------------------------------------------------

echo "[run-in-container] checking control-plane health..."
for i in $(seq 1 60); do
    if curl -fsS "$CTRL_URL/api/v1/health" 2>/dev/null | grep -q healthy; then
        echo "  control healthy after ${i}s"
        break
    fi
    [ "$i" -eq 60 ] && { echo "ERROR: control plane down on $CTRL_URL" >&2; exit 10; }
    sleep 1
done

echo "[run-in-container] checking worker health..."
for i in $(seq 1 30); do
    if curl -fsS "$COMPUTE_URL/health" 2>/dev/null | grep -q healthy; then
        echo "  worker healthy after ${i}s"
        break
    fi
    [ "$i" -eq 30 ] && echo "WARN: worker /health did not respond on $COMPUTE_URL after 30s; continuing"
    sleep 1
done

# ----- 2. activate license + smoke -------------------------------------------

echo "[run-in-container] obtaining admin token..."
TOKEN="$(curl -fsS -X POST "$CTRL_URL/api/v1/auth/token" \
        -H 'Content-Type: application/json' \
        -d "{\"grant_type\":\"password\",\"username\":\"${ADMIN_EMAIL}\",\"password\":\"${ADMIN_PWD}\"}" \
        | jq -re '.access_token')"
[ -z "$TOKEN" ] && { echo "ERROR: could not obtain access token" >&2; exit 11; }

echo "[run-in-container] activating license..."
ACT_RESP="$(curl -s -X POST "$CTRL_URL/api/v1/license/activate" \
    -H "Authorization: Bearer $TOKEN")"
ACT_STATUS="$(echo "$ACT_RESP" | jq -re '.activationStatus' 2>/dev/null || echo unknown)"
if [ "$ACT_STATUS" != "activated" ]; then
    echo "ERROR: license activation status=$ACT_STATUS" >&2
    echo "$ACT_RESP" | head -c 400 >&2
    exit 13
fi
echo "  license activated: $(echo "$ACT_RESP" | jq -r '"\(.tier) tier, expires \(.expiresAt)"')"

if command -v delta-forge-cli >/dev/null 2>&1; then
    echo "[run-in-container] CLI smoke..."
    DF_CONTROL_URL="$CTRL_URL" \
    DF_USERNAME="$ADMIN_EMAIL" \
    DF_PASSWORD="$ADMIN_PWD" \
    delta-forge-cli --format json query "SELECT 1 AS smoke" 2>&1 | tee /tmp/df-smoke.json
    if ! grep -q '"row_count"' /tmp/df-smoke.json; then
        echo "ERROR: CLI smoke did not return a row_count" >&2
        exit 11
    fi
fi

# ----- 3. zone --------------------------------------------------------------

echo "[run-in-container] ensuring zone '$ZONE_NAME' exists..."
ZONE_LIST="$(curl -fsS "$CTRL_URL/api/v1/catalog/zones" -H "Authorization: Bearer $TOKEN")"
if echo "$ZONE_LIST" | jq -re --arg n "$ZONE_NAME" 'map(select(.name==$n)) | length' | grep -q '^0$'; then
    mkdir -p "/var/lib/deltaforge/zones/${ZONE_NAME}"
    curl -fsS -X POST "$CTRL_URL/api/v1/catalog/zones" \
        -H "Authorization: Bearer $TOKEN" \
        -H 'Content-Type: application/json' \
        -d "{\"name\":\"${ZONE_NAME}\",\"zone_type\":\"silver\",\"storage_root\":\"/var/lib/deltaforge/zones/${ZONE_NAME}\"}" \
        >/dev/null \
        || { echo "ERROR: could not create zone" >&2; exit 14; }
    echo "  created zone $ZONE_NAME"
else
    echo "  zone $ZONE_NAME already exists"
fi

# ----- 4. unixODBC config ---------------------------------------------------

echo "[run-in-container] configuring unixODBC..."
cat > /etc/odbcinst.ini <<EOF
[ODBC]
Trace = No

[DeltaForge]
Description    = DeltaForge ODBC Driver
Driver         = /usr/local/lib/libdeltaforgeodbc.so
DriverODBCVer  = 03.80
Threading      = 2
EOF
cat > /etc/odbc.ini <<EOF
[${DSN_NAME}]
Description    = DeltaForge bench DSN (self-provisioned, in-container)
Driver         = DeltaForge
Server         = ${CTRL_URL}
ComputeServer  = ${COMPUTE_URL}
Uid            = ${ADMIN_EMAIL}
Pwd            = ${ADMIN_PWD}
TLS            = disabled
EOF
chmod 0600 /etc/odbc.ini

# ----- 5. build driver_bench -------------------------------------------------

cd "$BENCH_DIR"
if [ ! -x "$BUILD_DIR/driver_bench" ]; then
    echo "[run-in-container] building driver_bench..."
    cmake -S . -B "$BUILD_DIR" -DCMAKE_BUILD_TYPE=Release >> "$LOG" 2>&1 \
        || { echo "ERROR: cmake configure failed" >&2; exit 20; }
    cmake --build "$BUILD_DIR" -j >> "$LOG" 2>&1 \
        || { echo "ERROR: cmake build failed" >&2; exit 20; }
fi

# ----- 6. fixture via ODBC ---------------------------------------------------

echo "[run-in-container] building fixture $TABLE ($ROWS rows) via ODBC..."
FIX_SQL="$(cat <<EOF
DROP TABLE IF EXISTS ${TABLE};
CREATE TABLE ${TABLE} AS SELECT
  (i+0) AS id_i64, (i+0) * 2 AS doubled_i64,
  ((i+0) % 32767)::smallint AS small_i, ((i+0) % 127)::tinyint AS tiny_i,
  ((i+0) % 2 = 0) AS flag_bool,
  ((i+0) * 1.5)::double AS f64_a, ((i+0) / 7.0)::double AS f64_b,
  cast((i+0) AS decimal(18,4)) AS dec18,
  cast((i+0) * 0.01 AS decimal(28,8)) AS dec28,
  cast((i+0) * 1.0 AS decimal(10,2)) AS price,
  cast(timestamp '2026-01-01 00:00:00' + ((i+0) || ' seconds')::interval AS timestamp) AS ts_a,
  cast(date '2026-01-01' + ((i+0) % 365) AS date) AS d_a,
  md5((i+0)::text) AS md5_hex, ('row-' || (i+0)::text) AS label_v32,
  repeat('x', 16) AS pad_v16, repeat('y', 64) AS pad_v64, repeat('z', 128) AS pad_v128,
  cast((i+0) AS varchar(40)) AS i_as_text, upper(md5((i+0)::text)) AS md5_upper,
  concat('id-', lpad((i+0)::text, 10, '0')) AS padded_id,
  cast((i+0) % 127 AS tinyint) AS bucket, cast((i+0) % 1000 AS integer) AS dim_k
FROM generate_series(1, ${ROWS}) AS t(i);
EOF
)"
echo "$FIX_SQL" | tr '\n' ' ' | isql "$DSN_NAME" >> "$LOG" 2>&1 \
    || { echo "ERROR: fixture CTAS failed (see $LOG)" >&2; exit 21; }
echo "  fixture ready"

# ----- 7. run the bench ------------------------------------------------------

DRIVER_ARGS=()
case "$WHICH" in
    odbc) DRIVER_ARGS=(--driver odbc) ;;
    adbc) DRIVER_ARGS=(--driver adbc) ;;
    both) DRIVER_ARGS=(--driver both) ;;
    *) echo "ERROR: DRIVER_BENCH_DRIVER must be odbc|adbc|both, got '$WHICH'" >&2; exit 30 ;;
esac

echo
echo "[run-in-container] launching driver_bench..."
"$BUILD_DIR/driver_bench" \
    "${DRIVER_ARGS[@]}" \
    --warmups "$WARMUPS" --iters "$ITERS" \
    --sql "SELECT * FROM ${TABLE}" \
    --odbc-dsn  "$DSN_NAME" \
    --adbc-uri  "$CTRL_URL" \
    --adbc-compute "$COMPUTE_URL" \
    --adbc-user "$ADMIN_EMAIL" \
    --adbc-pwd  "$ADMIN_PWD" \
    --adbc-so   "$ADBC_SO" \
    --json-out  "$SUBDIR/cpp.json"

if [ -d "$BENCH_DIR/dotnet" ] && command -v dotnet >/dev/null 2>&1; then
    echo
    echo "[run-in-container] launching .NET bench..."
    (cd "$BENCH_DIR/dotnet" && dotnet build -c Release --nologo --verbosity quiet \
        && dotnet run -c Release --no-build --no-restore -- \
            --warmups "$WARMUPS" --iters "$ITERS" \
            --sql "SELECT * FROM ${TABLE}" \
            --odbc-dsn  "$DSN_NAME" \
            --adbc-uri  "$CTRL_URL" \
            --adbc-compute "$COMPUTE_URL" \
            --adbc-user "$ADMIN_EMAIL" \
            --adbc-pwd  "$ADMIN_PWD" \
            --adbc-so   "$ADBC_SO" \
            --json-out  "$SUBDIR/dotnet.json")
fi

cat > "$SUBDIR/manifest.json" <<EOF
{
  "harness": "in-container",
  "rows": ${ROWS},
  "iters": ${ITERS},
  "warmups": ${WARMUPS},
  "table": "${TABLE}",
  "zone": "${ZONE_NAME}",
  "control_url": "${CTRL_URL}",
  "compute_url": "${COMPUTE_URL}",
  "utc": "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
}
EOF

echo
echo "[run-in-container] done. Artifacts under $SUBDIR"
echo "  manifest.json"
echo "  cpp.json"
[ -f "$SUBDIR/dotnet.json" ] && echo "  dotnet.json"
echo "  run.log"
