#!/usr/bin/env bash
# Stage all SF100 + JOB fixtures the public concurrency / 100 GB bench
# needs in one pass. Runs inside the bench container (paths assume
# /workspace as REPO_ROOT, matching docker/Dockerfile and run_bench.sh).
#
# Order:
#   1. TPC-H SF100  parquet  -> tpch_sf100/
#   2. TPC-H SF100  Delta    -> tpch_sf100_delta/      (depends on 1)
#   3. TPC-DS SF100 parquet+Delta -> tpcds_sf100/ + tpcds_sf100_delta/
#   4. SSB SF100    Delta    -> ssb_sf100_delta/       (depends on 2)
#   5. JOB          Delta    -> job_delta/             (fixed size, independent)
#
# Each generator is idempotent: a finished output is skipped. Re-run
# after a partial failure and only the missing stages execute. Pass
# OVERWRITE=1 to force rebuild of every stage.
#
# Disk budget:
#   - Steady state: ~75 GB across all four datasets.
#   - Transient peak: ~150 GB during TPC-DS dsdgen (DuckDB spill +
#     parquet stage + Delta rewrite all live concurrently for a window).
#   - Pre-flight aborts if /workspace has less than 200 GB free, which
#     is the safe floor for the transient peak plus headroom.
#
# Env overrides:
#   DATA_DIR        default /workspace/data
#   DUCKDB_TEMP_DIR default $DATA_DIR/duckdb_tmp
#   DUCKDB_MEM      default 8GB         (DuckDB SET memory_limit)
#   OVERWRITE       default unset       (set to 1 to force --overwrite on every stage)
#   MIN_FREE_GB     default 200         (abort threshold for /workspace free space)
#
# Example:
#   ./scripts/prepare_sf100.sh
#   DUCKDB_MEM=16GB ./scripts/prepare_sf100.sh
#   OVERWRITE=1 ./scripts/prepare_sf100.sh

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

DATA_DIR="${DATA_DIR:-/workspace/data}"
DUCKDB_TEMP_DIR="${DUCKDB_TEMP_DIR:-$DATA_DIR/duckdb_tmp}"
DUCKDB_MEM="${DUCKDB_MEM:-8GB}"
MIN_FREE_GB="${MIN_FREE_GB:-200}"
SCALE=100

OVERWRITE_FLAG=""
if [ "${OVERWRITE:-0}" = "1" ]; then
    OVERWRITE_FLAG="--overwrite"
fi

mkdir -p "$DATA_DIR" "$DUCKDB_TEMP_DIR"

echo "[prep] data_dir       : $DATA_DIR"
echo "[prep] duckdb_temp_dir: $DUCKDB_TEMP_DIR"
echo "[prep] duckdb_memory  : $DUCKDB_MEM"
echo "[prep] overwrite      : ${OVERWRITE:-0}"
echo "[prep] scale          : $SCALE (TPC-H, TPC-DS, SSB); JOB fixed-size"
echo

# ----- disk pre-flight -------------------------------------------------------

FREE_GB=$(df -BG --output=avail "$DATA_DIR" | tail -1 | tr -dc '0-9')
echo "[prep] free on $(df --output=target "$DATA_DIR" | tail -1): ${FREE_GB} GB"
if [ "$FREE_GB" -lt "$MIN_FREE_GB" ]; then
    echo "[prep] ERROR free space ${FREE_GB} GB is below the ${MIN_FREE_GB} GB floor" >&2
    echo "[prep]       free up disk or override with MIN_FREE_GB=<smaller>" >&2
    exit 1
fi

# ----- timer -----------------------------------------------------------------

stage_start() {
    STAGE_NAME="$1"
    STAGE_T0=$(date +%s)
    echo
    echo "==[stage] $STAGE_NAME starting at $(date -u -Iseconds)"
}

stage_end() {
    local elapsed=$(( $(date +%s) - STAGE_T0 ))
    echo "==[stage] $STAGE_NAME done in ${elapsed}s"
}

# ----- 1. TPC-H parquet ------------------------------------------------------

stage_start "tpch parquet @ SF$SCALE"
python data_gen/generate_tpch.py --scale "$SCALE" \
    ${OVERWRITE_FLAG:+--force}
stage_end

# ----- 2. TPC-H Delta --------------------------------------------------------

stage_start "tpch Delta @ SF$SCALE"
python data_gen/generate_tpch_delta.py --scale "$SCALE" \
    --data-dir "$DATA_DIR" $OVERWRITE_FLAG
stage_end

# ----- 3. TPC-DS parquet + Delta ---------------------------------------------

stage_start "tpcds parquet+Delta @ SF$SCALE"
python data_gen/generate_tpcds_delta.py --scale "$SCALE" \
    --data-dir "$DATA_DIR" \
    --memory-limit "$DUCKDB_MEM" \
    --temp-dir "$DUCKDB_TEMP_DIR" \
    $OVERWRITE_FLAG
stage_end

# ----- 4. SSB Delta ----------------------------------------------------------

stage_start "ssb Delta @ SF$SCALE"
python data_gen/generate_ssb_delta.py --scale "$SCALE" \
    --data-dir "$DATA_DIR" $OVERWRITE_FLAG
stage_end

# ----- 5. JOB Delta ----------------------------------------------------------

stage_start "job Delta (fixed size)"
python data_gen/generate_job_delta.py \
    --data-dir "$DATA_DIR" $OVERWRITE_FLAG
stage_end

# ----- summary ---------------------------------------------------------------

echo
echo "[prep] staged datasets:"
for d in tpch_sf${SCALE} tpch_sf${SCALE}_delta \
         tpcds_sf${SCALE} tpcds_sf${SCALE}_delta \
         ssb_sf${SCALE}_delta job_delta; do
    if [ -d "$DATA_DIR/$d" ]; then
        sz=$(du -sh "$DATA_DIR/$d" 2>/dev/null | cut -f1)
        echo "  $DATA_DIR/$d  ($sz)"
    else
        echo "  $DATA_DIR/$d  (MISSING)"
    fi
done

# Auto-reclaim DuckDB spill scratch. It is only needed while dsdgen +
# COPY runs; downstream stages and the bench itself never touch it.
# Skip with KEEP_DUCKDB_TMP=1 if you want it preserved for debugging.
if [ "${KEEP_DUCKDB_TMP:-0}" != "1" ] && [ -d "$DUCKDB_TEMP_DIR" ]; then
    sz_h=$(du -sh "$DUCKDB_TEMP_DIR" 2>/dev/null | cut -f1)
    echo
    echo "[prep] reclaiming DuckDB temp dir ($sz_h): rm -rf $DUCKDB_TEMP_DIR"
    rm -rf -- "$DUCKDB_TEMP_DIR"
fi

echo
echo "[prep] free on $(df --output=target "$DATA_DIR" | tail -1): \
$(df -BG --output=avail "$DATA_DIR" | tail -1 | tr -dc '0-9') GB"

echo
echo "[prep] ready for: ./scripts/run_bench.sh 100 df,duckdb,spark-tuned \\"
echo "                       tpch_read_delta,tpcds_read_delta,ssb_read_delta,job_read_delta"
echo
echo "[prep] when the bench is done, reclaim the ~75 GB of staged data with:"
echo "       ./scripts/cleanup_sf100.sh --data         (interactive prompt)"
echo "       ./scripts/cleanup_sf100.sh --all --yes    (also drops scale-out stream results)"
