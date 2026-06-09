#!/usr/bin/env bash
# Quick smoke run on a fresh server. Validates the harness works end to end
# without burning hours of wall time.
#
# Scope: SF=1 (~1 GB), spark-default only, tpch_read workload only.
# Wall time: ~10-15 minutes.
#
# Use this immediately after install.sh to confirm the host is healthy
# before kicking off the canonical SF=10 run.

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

# ----- env -------------------------------------------------------------------

[ -f .env ] && set -a && . ./.env && set +a
[ -d .venv ] && . .venv/bin/activate

if [ -z "${JAVA_HOME:-}" ]; then
    echo "[smoke] ERROR JAVA_HOME not set. Run scripts/install.sh first." >&2
    exit 1
fi

# ----- ensure SF=1 data ------------------------------------------------------

if [ ! -f data/tpch_sf1/lineitem.parquet ]; then
    echo "[smoke] generating SF=1 data (~10s)"
    python data_gen/generate_tpch.py --scale 1
fi

# ----- run -------------------------------------------------------------------

TAG="smoke-$(date -u +%Y%m%dT%H%M%SZ)"
echo "[smoke] starting bench: SF=1, spark-default, tpch_read, tag=$TAG"
echo "[smoke] approx wall time: 10-15 min on a 16+ GB host"

python bench_runner.py \
    --scale 1 \
    --engines spark-default \
    --workloads tpch_read \
    --no-purge \
    --tag "$TAG"

# ----- report ----------------------------------------------------------------

# The newest run dir wins. results/<timestamp>-<host>-<tag>/
RESULTS_DIR=$(ls -1dt results/*"$TAG"* 2>/dev/null | head -1)
if [ -z "$RESULTS_DIR" ]; then
    echo "[smoke] WARN could not find results dir for tag $TAG" >&2
    exit 1
fi

echo "[smoke] generating report under $RESULTS_DIR"
python reports/generate_report.py --results-dir "$RESULTS_DIR"

echo
echo "[smoke] done. Artifacts:"
echo "  $RESULTS_DIR/manifest.json"
echo "  $RESULTS_DIR/summary.csv"
echo "  $RESULTS_DIR/report.md"
echo "  $RESULTS_DIR/raw/spark-default.jsonl"
