#!/usr/bin/env bash
# Run the three read-perf queries against both DuckDB and DeltaForge,
# time each, and append one row per (engine, query) to results.csv.
#
# Each query is run twice and the second run is recorded (warm-cache).
# The first run primes the OS page cache so we're measuring the engine,
# not disk seek noise. If you want cold-cache numbers, restart the
# compute engine and run once with WARM_RUNS=1.
#
# Usage:   ./run.sh
#
# Env overrides:
#   DF_CLI         path to delta-forge-cli.exe
#   DUCKDB         path to duckdb.exe
#   CRED_FILE      democred.txt path
#   DF_USERNAME / DF_PASSWORD     bypass CRED_FILE
#   WARM_RUNS      number of warm runs to record (default 1; total = 1 prime + N warm)
#   NOTE           free-text note recorded in results.csv

set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
RESULTS="$HERE/results.csv"

DF_CLI="${DF_CLI:-/a/delta-forge/target/release/delta-forge-cli.exe}"
DUCKDB="${DUCKDB:-/a/tmp/duckdb/duckdb.exe}"
CRED_FILE="${CRED_FILE:-A:/delta-forge/.deltaforge/democred.txt}"
WARM_RUNS="${WARM_RUNS:-1}"
NOTE="${NOTE:-}"

if [[ -z "${DF_USERNAME:-}" || -z "${DF_PASSWORD:-}" ]]; then
    if [[ -r "$CRED_FILE" ]]; then
        DF_USERNAME=$(awk -F': ' '/^user:/ {print $2}' "$CRED_FILE")
        DF_PASSWORD=$(awk '/^pwd:/ {sub(/^pwd: /, ""); print}' "$CRED_FILE")
    fi
fi

if [[ -z "${DF_USERNAME:-}" || -z "${DF_PASSWORD:-}" ]]; then
    echo "ERROR: set DF_USERNAME and DF_PASSWORD, or provide $CRED_FILE" >&2
    exit 1
fi

GIT_SHA=$(git -C "$HERE" rev-parse --short HEAD 2>/dev/null || echo unknown)
TS=$(date +%Y-%m-%dT%H:%M:%S)

[[ -f "$RESULTS" ]] || echo "timestamp,git_sha,engine,query,run,seconds,note" > "$RESULTS"

now_ms() { python -c 'import time; print(int(time.time()*1000))' 2>/dev/null || date +%s%3N; }

run_df_once() {
    local script="$1"
    local start end
    start=$(now_ms)
    "$DF_CLI" --username "$DF_USERNAME" --password "$DF_PASSWORD" -y run "$HERE/$script" >/dev/null 2>&1 || return 1
    end=$(now_ms)
    awk -v a="$start" -v b="$end" 'BEGIN{printf "%.3f", (b-a)/1000.0}'
}

run_duck_once() {
    local script="$1"
    local start end
    start=$(now_ms)
    "$DUCKDB" <"$HERE/$script" >/dev/null 2>&1 || return 1
    end=$(now_ms)
    awk -v a="$start" -v b="$end" 'BEGIN{printf "%.3f", (b-a)/1000.0}'
}

bench_pair() {
    local query_label="$1" df_script="$2" duck_script="$3"

    # prime cache
    run_df_once   "$df_script"   >/dev/null
    run_duck_once "$duck_script" >/dev/null

    for i in $(seq 1 "$WARM_RUNS"); do
        local df_t duck_t
        df_t=$(run_df_once   "$df_script")
        duck_t=$(run_duck_once "$duck_script")
        echo "$TS,$GIT_SHA,df,$query_label,$i,$df_t,${NOTE//,/;}"   >> "$RESULTS"
        echo "$TS,$GIT_SHA,duck,$query_label,$i,$duck_t,${NOTE//,/;}" >> "$RESULTS"
        printf "  %-12s  run %d   df=%6.3fs   duck=%6.3fs   ratio=%.2fx\n" \
            "$query_label" "$i" "$df_t" "$duck_t" \
            "$(awk -v a="$df_t" -v b="$duck_t" 'BEGIN{ if (b > 0) printf "%.2f", a/b; else print "n/a" }')"
    done
}

echo "read-perf bench: git=$GIT_SHA  ts=$TS  warm_runs=$WARM_RUNS"
echo ""
printf "  %-12s  %-5s   %s   %s   %s\n" "query" "run" "df(s)" "duck(s)" "df/duck"
printf "  %-12s  %-5s   %s   %s   %s\n" "------------" "-----" "------" "--------" "---------"

bench_pair "q1_count"  "df_q1_count.sql"  "duck_q1_count.sql"
bench_pair "q2_agg"    "df_q2_agg.sql"    "duck_q2_agg.sql"
bench_pair "q3_topk"   "df_q3_topk.sql"   "duck_q3_topk.sql"

echo ""
echo "appended to: $RESULTS"
