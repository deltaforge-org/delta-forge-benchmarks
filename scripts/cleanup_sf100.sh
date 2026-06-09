#!/usr/bin/env bash
# Reclaim disk after an SF100 bench run. Destructive; prompts before
# deleting unless --yes is passed.
#
# What it can remove (each opt-in via a flag; combine flags freely):
#
#   --data         The four staged datasets that prepare_sf100.sh built:
#                    $DATA_DIR/tpch_sf100/         (~25 GB parquet)
#                    $DATA_DIR/tpch_sf100_delta/   (~25 GB plain Delta)
#                    $DATA_DIR/tpcds_sf100/        (~30 GB parquet)
#                    $DATA_DIR/tpcds_sf100_delta/  (~30 GB plain Delta)
#                    $DATA_DIR/ssb_sf100_delta/    (~20 GB plain Delta)
#                    $DATA_DIR/job_delta/          (~1  GB plain Delta)
#
#   --duckdb-tmp   $DATA_DIR/duckdb_tmp/  (DuckDB spill dir; safe to drop
#                  any time the SF100 generator is not actively running)
#
#   --scale-out    $REPO_ROOT/scale_out/results/  (per-stream raw JSONLs +
#                  bench_runner result dirs from concurrency runs; the
#                  aggregated curve.{csv,json,md} stay)
#
#   --all          Equivalent to: --data --duckdb-tmp --scale-out
#
# Always:
#   --dry-run      Print what would be removed; do not touch anything
#   --yes          Skip the confirmation prompt (CI / scripted use)
#
# Env overrides:
#   DATA_DIR        default /workspace/data
#
# Examples:
#   ./scripts/cleanup_sf100.sh --dry-run --all
#   ./scripts/cleanup_sf100.sh --duckdb-tmp --yes
#   ./scripts/cleanup_sf100.sh --data           # interactive prompt

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

DATA_DIR="${DATA_DIR:-/workspace/data}"
SCALE=100

DO_DATA=0
DO_DUCKDB_TMP=0
DO_SCALE_OUT=0
DRY_RUN=0
ASSUME_YES=0

usage() {
    sed -n '2,/^set -euo/p' "$0" | sed -E '/^set -euo/d; s/^# ?//'
    exit "${1:-1}"
}

while [ $# -gt 0 ]; do
    case "$1" in
        --data)        DO_DATA=1 ;;
        --duckdb-tmp)  DO_DUCKDB_TMP=1 ;;
        --scale-out)   DO_SCALE_OUT=1 ;;
        --all)         DO_DATA=1; DO_DUCKDB_TMP=1; DO_SCALE_OUT=1 ;;
        --dry-run)     DRY_RUN=1 ;;
        --yes|-y)      ASSUME_YES=1 ;;
        -h|--help)     usage 0 ;;
        *) echo "[cleanup] unknown arg: $1" >&2; usage 2 ;;
    esac
    shift
done

if [ "$DO_DATA" -eq 0 ] && [ "$DO_DUCKDB_TMP" -eq 0 ] && [ "$DO_SCALE_OUT" -eq 0 ]; then
    echo "[cleanup] nothing to do; pass --data, --duckdb-tmp, --scale-out, or --all" >&2
    usage 2
fi

# Collect the actual paths to remove, with their on-disk sizes for the
# confirmation prompt. Skip anything that does not exist (re-runnable).
TARGETS=()
add_target() {
    local p="$1"
    if [ -e "$p" ]; then
        TARGETS+=("$p")
    fi
}

if [ "$DO_DATA" -eq 1 ]; then
    add_target "$DATA_DIR/tpch_sf${SCALE}"
    add_target "$DATA_DIR/tpch_sf${SCALE}_delta"
    add_target "$DATA_DIR/tpcds_sf${SCALE}"
    add_target "$DATA_DIR/tpcds_sf${SCALE}_delta"
    add_target "$DATA_DIR/ssb_sf${SCALE}_delta"
    add_target "$DATA_DIR/job_delta"
fi
if [ "$DO_DUCKDB_TMP" -eq 1 ]; then
    add_target "$DATA_DIR/duckdb_tmp"
fi
if [ "$DO_SCALE_OUT" -eq 1 ]; then
    # Only the per-stream result tree, not the aggregated curve files
    # next to it. orchestrate.sh defaults RESULTS_ROOT to this path.
    add_target "$REPO_ROOT/scale_out/results"
fi

if [ "${#TARGETS[@]}" -eq 0 ]; then
    echo "[cleanup] no matching paths exist; nothing to do."
    exit 0
fi

# Show targets and total reclaim size.
echo "[cleanup] would remove:"
TOTAL_KB=0
for p in "${TARGETS[@]}"; do
    sz_kb=$(du -sk "$p" 2>/dev/null | awk '{print $1}')
    TOTAL_KB=$(( TOTAL_KB + ${sz_kb:-0} ))
    sz_h=$(numfmt --to=iec --suffix=B --padding=10 "$((sz_kb * 1024))" 2>/dev/null \
           || echo "${sz_kb} KB")
    echo "   $sz_h  $p"
done
echo "[cleanup] total reclaim: $(numfmt --to=iec --suffix=B "$((TOTAL_KB * 1024))" 2>/dev/null \
                                 || echo "${TOTAL_KB} KB")"

if [ "$DRY_RUN" -eq 1 ]; then
    echo "[cleanup] --dry-run; not removing anything."
    exit 0
fi

if [ "$ASSUME_YES" -ne 1 ]; then
    echo
    read -r -p "[cleanup] proceed and remove these paths? [y/N] " ans
    case "$ans" in
        y|Y|yes|YES) ;;
        *) echo "[cleanup] aborted; nothing removed."; exit 0 ;;
    esac
fi

for p in "${TARGETS[@]}"; do
    echo "[cleanup] rm -rf $p"
    rm -rf -- "$p"
done

echo "[cleanup] done. Free on $(df --output=target "$DATA_DIR" | tail -1): \
$(df -BG --output=avail "$DATA_DIR" | tail -1 | tr -dc '0-9') GB"
