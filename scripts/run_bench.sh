#!/usr/bin/env bash
# Canonical SF=10 bench run. The v0.1 published-headline workload.
#
# What it runs:
#   Engines:   spark-default, spark-tuned (DF added when df_engine.py lands)
#   Workloads: tpch_read, bulk_load, merge_cdc, update_delete, optimize
#   Scale:     10  (60M-row lineitem, ~3.4 GB Parquet, ~10 GB Delta tables)
#   Protocol:  per-workload defaults (1 cold + 9 warm for reads, fewer for writes)
#
# Wall time: ~3-6 hours on a 16-32 GB host. Plan accordingly.
#
# Failure modes that are FINE and reported, not aborts:
#   - spark-default OOMing on lineitem at SF=10: that's a finding, not a bug.
#     The harness records exit_code != 0 and continues to the next workload.
#   - Any single query failing on one engine: the report shows it as a
#     failure and the cross-engine comparison gracefully degrades.
#
# Args (all optional, defaults shown):
#   $1 SCALE       - 10
#   $2 ENGINES     - spark-default,spark-tuned
#   $3 WORKLOADS   - tpch_read,bulk_load,merge_cdc,update_delete,optimize
#   $4 TAG         - bench-<utc>
#
# Examples:
#   ./scripts/run_bench.sh
#   ./scripts/run_bench.sh 10 spark-tuned                       # one engine
#   ./scripts/run_bench.sh 10 spark-default,spark-tuned tpch_read   # one workload
#   ./scripts/run_bench.sh 1  spark-default tpch_read smoke    # equivalent to run_smoke.sh

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

# ----- args ------------------------------------------------------------------

SCALE="${1:-10}"
ENGINES="${2:-spark-default,spark-tuned}"
WORKLOADS="${3:-tpch_read,bulk_load,merge_cdc,update_delete,optimize}"
TAG="${4:-bench-$(date -u +%Y%m%dT%H%M%SZ)}"

# ----- env -------------------------------------------------------------------

[ -f .env ] && set -a && . ./.env && set +a
[ -d .venv ] && . .venv/bin/activate

if [ -z "${JAVA_HOME:-}" ]; then
    echo "[bench] ERROR JAVA_HOME not set. Run scripts/install.sh first." >&2
    exit 1
fi

echo "[bench] scale     : $SCALE"
echo "[bench] engines   : $ENGINES"
echo "[bench] workloads : $WORKLOADS"
echo "[bench] tag       : $TAG"
echo "[bench] java_home : $JAVA_HOME"
echo

# ----- ensure data -----------------------------------------------------------

DATA_DIR="data/tpch_sf$SCALE"
if [ ! -f "$DATA_DIR/lineitem.parquet" ]; then
    echo "[bench] generating SF=$SCALE data (this can take a few minutes for SF=10+)"
    python data_gen/generate_tpch.py --scale "$SCALE"
fi

# ----- run -------------------------------------------------------------------

# Stream stdout + stderr to a logfile under the run dir AFTER the runner
# creates it. Until then, write to a pre-created sibling.
LOG_PRE="logs/run_${TAG}.log"
mkdir -p logs

echo "[bench] starting at $(date -u -Iseconds)"
echo "[bench] live log:  $LOG_PRE"
echo "[bench] tail with: tail -f $LOG_PRE"
echo

set +e
python bench_runner.py \
    --scale "$SCALE" \
    --engines "$ENGINES" \
    --workloads "$WORKLOADS" \
    --no-purge \
    --tag "$TAG" 2>&1 | tee "$LOG_PRE"
RC=${PIPESTATUS[0]}
set -e

echo
echo "[bench] runner exit code: $RC"

# ----- locate run dir + generate report --------------------------------------

RESULTS_DIR=$(ls -1dt results/*"$TAG"* 2>/dev/null | head -1)
if [ -z "$RESULTS_DIR" ]; then
    echo "[bench] WARN could not find results dir for tag $TAG; raw log at $LOG_PRE" >&2
    exit "$RC"
fi

# Move the live log into the run dir for easy archival.
mv "$LOG_PRE" "$RESULTS_DIR/logs/runner.log"

echo "[bench] generating report under $RESULTS_DIR"
python reports/generate_report.py --results-dir "$RESULTS_DIR" || \
    echo "[bench] WARN report generation failed; raw records still in $RESULTS_DIR/raw/"

# ----- tarball for export ----------------------------------------------------

TARBALL="$RESULTS_DIR.tar.gz"
echo "[bench] packaging $TARBALL"
tar -czf "$TARBALL" -C results "$(basename "$RESULTS_DIR")"

# ----- summary ---------------------------------------------------------------

echo
echo "[bench] done at $(date -u -Iseconds)"
echo "[bench] results dir : $RESULTS_DIR"
echo "[bench] tarball     : $TARBALL"
echo "[bench] manifest    : $RESULTS_DIR/manifest.json"
echo "[bench] summary csv : $RESULTS_DIR/summary.csv"
echo "[bench] report md   : $RESULTS_DIR/report.md"
echo "[bench] raw records : $RESULTS_DIR/raw/"
echo
echo "Copy the tarball off the server with:"
echo "  scp <user>@<server>:$REPO_ROOT/$TARBALL ."

exit "$RC"
