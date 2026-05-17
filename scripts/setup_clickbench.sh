#!/usr/bin/env bash
# Opt-in: download the ClickBench dataset (14 GB hits.parquet) and the 43
# canonical queries into the bench's data volume.
#
# Why this is a separate script, not part of the default bench build:
#   - 14 GB is too large to ship in the docker image.
#   - Most users only run the TPC-H workload; they should not pay the
#     download cost or disk footprint of ClickBench.
#   - Running this script is purely additive: it touches only the
#     bench_data docker volume + the ClickBench queries dir under the
#     bench source tree. No other workload depends on it.
#
# Usage (from the host):
#   ./scripts/setup_clickbench.sh
#
# After this, run the bench with:
#   docker compose exec bench python bench_runner.py --workloads clickbench
#
# Re-running this script is idempotent: it skips the download if the
# parquet is already present at the expected byte count.

set -euo pipefail

if ! docker ps --filter "name=^bench$" --format "{{.Names}}" | grep -q "^bench$"; then
    echo "ERROR: the 'bench' container is not running." >&2
    echo "Bring the stack up first:  docker compose up -d" >&2
    exit 1
fi

echo "[setup_clickbench] fetching hits.parquet (~14 GB) + queries.sql into bench_data volume..."
docker exec bench python /workspace/data_gen/get_clickbench.py "$@"

echo
echo "[setup_clickbench] done."
echo "[setup_clickbench] run the workload with:"
echo "    docker compose exec bench python bench_runner.py \\"
echo "      --engines df,duckdb,spark-default --workloads clickbench --no-purge"
