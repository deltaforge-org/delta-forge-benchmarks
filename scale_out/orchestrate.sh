#!/usr/bin/env bash
# Launch N concurrent TPC-DS SF100 Power streams under cgroup-isolated
# unit workers (4 CPU + 4 GB RAM each), repeat across multiple N values,
# and persist per-stream results for aggregate.py to chart.
#
# Methodology details live in scale_out/README.md; the short version:
# each stream is a `bench_runner.py` invocation pinned to a disjoint
# 4-CPU block via cgroup v2 (AllowedCPUs), capped at 4 GB RAM
# (MemoryMax) with swap disabled (MemorySwapMax=0). Stream i uses
# DF_BENCH_STREAM_SEED=$i so the 99 queries hit the dataset in a
# stream-specific order, matching the TPC-DS Throughput Test
# methodology.
#
# Env overrides:
#   N_VALUES        default "1 2 4 8"   (concurrency points to measure)
#   N_MAX           default 8           (hard cap; raise only on bigger iron)
#   CORES_PER_WORKER default 4          (must divide host nproc)
#   MEM_PER_WORKER  default 4G          (cgroup MemoryMax per stream)
#   SCALE           default 100         (TPC-DS scale factor; SF100 is the target)
#   ENGINES         default df          (scale-out only validates df today)
#   RESULTS_ROOT    default $REPO_ROOT/scale_out/results
#   SKIP_WARMUP     default unset       (set to 1 to skip the N=1 warmup pass)
#   SKIP_PREREQS    default unset       (set to 1 to bypass scale_out/prereqs.sh)
#   CLEAN_STREAMS   default unset       (set to 1 to rm -rf the per-stream raw
#                                        results after aggregate.py succeeds;
#                                        the curve.{csv,json,md} headline
#                                        files in $REPO_ROOT/scale_out/ are
#                                        kept either way)
#
# Example:
#   ./scale_out/orchestrate.sh
#   N_VALUES="1 4" ./scale_out/orchestrate.sh
#   MEM_PER_WORKER=8G CORES_PER_WORKER=8 N_VALUES="1 2" ./scale_out/orchestrate.sh

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

N_VALUES="${N_VALUES:-1 2 4 8}"
N_MAX="${N_MAX:-8}"
CORES_PER_WORKER="${CORES_PER_WORKER:-4}"
MEM_PER_WORKER="${MEM_PER_WORKER:-4G}"
SCALE="${SCALE:-100}"
ENGINES="${ENGINES:-df}"
RESULTS_ROOT="${RESULTS_ROOT:-$REPO_ROOT/scale_out/results}"

mkdir -p "$RESULTS_ROOT"

# ----- pre-flight -----------------------------------------------------------

if [ "${SKIP_PREREQS:-0}" != "1" ]; then
    DATA_DIR="${DATA_DIR:-/workspace/data}" \
    SCALE="$SCALE" \
        bash "$SCRIPT_DIR/prereqs.sh"
fi

# Validate N values against the cap and against host nproc.
HOST_CPUS=$(nproc)
echo "[orch] host nproc: $HOST_CPUS"
echo "[orch] cores/worker: $CORES_PER_WORKER, mem/worker: $MEM_PER_WORKER"
echo "[orch] N values: $N_VALUES (cap $N_MAX)"

for n in $N_VALUES; do
    if [ "$n" -gt "$N_MAX" ]; then
        echo "[orch] ERROR N=$n exceeds N_MAX=$N_MAX; raise N_MAX only if this \
host has the headroom (need n * cores_per_worker <= nproc and \
n * mem_per_worker <= host RAM with swap=0)" >&2
        exit 2
    fi
    needed_cpus=$(( n * CORES_PER_WORKER ))
    if [ "$needed_cpus" -gt "$HOST_CPUS" ]; then
        echo "[orch] ERROR N=$n * cores_per_worker=$CORES_PER_WORKER = $needed_cpus \
exceeds nproc=$HOST_CPUS; would oversubscribe" >&2
        exit 2
    fi
done

# ----- helpers --------------------------------------------------------------

# Compute the AllowedCPUs range for stream $1 at N=$2.
# Stream i (1-indexed) gets cpus [(i-1)*K, i*K-1] where K = CORES_PER_WORKER.
allowed_cpus() {
    local stream_id="$1"
    local k="$CORES_PER_WORKER"
    local lo=$(( (stream_id - 1) * k ))
    local hi=$(( lo + k - 1 ))
    echo "${lo}-${hi}"
}

# Bash arrays cannot be exported across functions cleanly, so the
# launcher pushes PIDs and stream tags into globals that wait_streams
# drains. Each orchestration phase (warmup, then each N value) resets
# them before launching.
STREAM_PIDS=()
STREAM_TAGS=()

reset_streams() {
    STREAM_PIDS=()
    STREAM_TAGS=()
}

# Launch one stream under a cgroup scope, in the background. Pushes
# (pid, stream_tag) onto STREAM_PIDS / STREAM_TAGS so wait_streams can
# join exit codes back to stream identities.
run_stream() {
    local stream_id="$1"
    local n="$2"
    local tag="$3"

    local cpus
    cpus=$(allowed_cpus "$stream_id")
    local stream_tag="${tag}_s${stream_id}"
    local stream_results="$RESULTS_ROOT/$stream_tag"
    mkdir -p "$stream_results/logs"

    echo "[orch] launch  stream=$stream_id n=$n cpus=$cpus mem=$MEM_PER_WORKER tag=$stream_tag"

    systemd-run --user --scope --quiet \
        --unit="df-scaleout-${stream_tag}" \
        -p "AllowedCPUs=$cpus" \
        -p "MemoryMax=$MEM_PER_WORKER" \
        -p "MemorySwapMax=0" \
        -- env DF_BENCH_STREAM_SEED="$stream_id" \
            python bench_runner.py \
                --scale "$SCALE" \
                --engines "$ENGINES" \
                --workloads tpcds_read_delta \
                --cold-runs 0 \
                --warm-runs 1 \
                --no-purge \
                --results-dir "$stream_results" \
                --tag "$stream_tag" \
                > "$stream_results/logs/runner.log" 2>&1 &

    STREAM_PIDS+=("$!")
    STREAM_TAGS+=("$stream_tag")
}

# Wait for every launched stream. Reports per-stream exit codes and
# sets STREAMS_FAILED to the count of failed streams in this batch.
# Returns 0 iff every stream exited 0 (callers may still choose to
# continue past a non-zero return; the matrix loop does, the warmup
# does not).
STREAMS_FAILED=0
wait_streams() {
    STREAMS_FAILED=0
    local rc=0
    local i
    for i in "${!STREAM_PIDS[@]}"; do
        local pid="${STREAM_PIDS[$i]}"
        local tag="${STREAM_TAGS[$i]}"
        if wait "$pid"; then
            echo "[orch] done    $tag (pid $pid)"
        else
            local code=$?
            echo "[orch] FAIL    $tag (pid $pid exit $code); \
log: $RESULTS_ROOT/$tag/logs/runner.log" >&2
            rc=1
            STREAMS_FAILED=$(( STREAMS_FAILED + 1 ))
        fi
    done
    return "$rc"
}

# ----- optional warmup pass -------------------------------------------------
#
# Reading SF100 cold from disk on the very first query inflates that
# query's timing well past steady-state. We do one N=1 pass to warm the
# OS page cache before the measured matrix. The result of this pass is
# kept on disk (under scale_out/results/warmup) but NOT fed to
# aggregate.py.

if [ "${SKIP_WARMUP:-0}" != "1" ]; then
    echo
    echo "[orch] === warmup pass (N=1, results discarded by aggregate.py) ==="
    WARMUP_TAG="warmup_$(date -u +%Y%m%dT%H%M%SZ)"
    reset_streams
    run_stream 1 1 "$WARMUP_TAG"
    if ! wait_streams; then
        echo "[orch] warmup failed" >&2
        exit 1
    fi
    echo "[orch] warmup done"
fi

# ----- measured matrix ------------------------------------------------------

RUN_TS=$(date -u +%Y%m%dT%H%M%SZ)
echo
echo "[orch] === measured matrix at $RUN_TS ==="

# Per-N failure tracking. The matrix DOES NOT abort on per-stream
# failure: at SF100 with a 4 GB MemoryMax the heaviest TPC-DS queries
# (q14, q24, q72, q64) are expected to OOM-kill some streams,
# especially at higher N. Each surviving stream's per-step jsonl is
# durable (bench_runner appends + fsyncs per record), so the
# aggregator sees partial completion rather than a missing stream.
MATRIX_FAIL_SUMMARY=()

for n in $N_VALUES; do
    echo
    echo "[orch] -- N=$n --"
    tag="scaleout_n${n}_${RUN_TS}"

    reset_streams
    for i in $(seq 1 "$n"); do
        run_stream "$i" "$n" "$tag"
    done

    wait_streams || true  # tolerate per-stream failure; record and continue
    if [ "$STREAMS_FAILED" -gt 0 ]; then
        MATRIX_FAIL_SUMMARY+=("N=$n: $STREAMS_FAILED/$n streams failed")
        echo "[orch] N=$n had $STREAMS_FAILED/$n stream failures; continuing"
    fi

    echo "[orch] N=$n complete; $n streams under $RESULTS_ROOT/${tag}_s*/"
done

if [ "${#MATRIX_FAIL_SUMMARY[@]}" -gt 0 ]; then
    echo
    echo "[orch] === per-stream failure summary ==="
    for line in "${MATRIX_FAIL_SUMMARY[@]}"; do
        echo "[orch]   $line"
    done
    echo "[orch] aggregate.py will report per-(query, N) completion counts; \
queries with samples < N at a given N were affected."
fi

# ----- aggregate ------------------------------------------------------------

echo
echo "[orch] === aggregating curve ==="
python "$SCRIPT_DIR/aggregate.py" \
    --results-root "$RESULTS_ROOT" \
    --run-ts "$RUN_TS" \
    --n-values "$N_VALUES" \
    --out-dir "$REPO_ROOT/scale_out"

echo
echo "[orch] done. Curve artifacts:"
echo "       $REPO_ROOT/scale_out/curve.csv"
echo "       $REPO_ROOT/scale_out/curve.json"
echo "       $REPO_ROOT/scale_out/curve.md"

if [ "${CLEAN_STREAMS:-0}" = "1" ]; then
    echo
    echo "[orch] CLEAN_STREAMS=1; removing per-stream raw results under $RESULTS_ROOT"
    rm -rf -- "$RESULTS_ROOT"
    echo "[orch] curve.{csv,json,md} in $REPO_ROOT/scale_out/ are untouched."
fi
