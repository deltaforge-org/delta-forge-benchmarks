#!/usr/bin/env bash
# read-perf-rowgroup: ad-hoc bench that isolates *row-group quality* as a
# read-side performance variable. The hypothesis is that DuckDB's row groups
# are denser / better-pruned than what DeltaForge currently writes, and that
# DF's read times against the same logical data improve materially when the
# parquet was authored by DuckDB.
#
# Setup (one-shot per invocation):
#   1. DuckDB writes 5M rows of dim_customer to
#      B:/odbc_df/df-demo/perf_test/dim_customer_duck/data.parquet
#   2. DF wraps that directory in place via CONVERT TO DELTA, then registers
#      it in the catalog as pbi.bench.dim_customer_duck
#
# Bench:
#   For each of q1_count / q2_agg / q3_topk we time three reads of the same
#   logical 5M rows:
#     A. DF reads pbi.bench.dim_customer_insert     (DF wrote the parquet)
#     B. DF reads pbi.bench.dim_customer_duck       (DuckDB wrote the parquet)
#     C. DuckDB reads the duck parquet directly     (engine sanity reference)
#
# A vs B isolates "DF read engine on DF parquet" vs "DF read engine on duck
# parquet" with everything else held constant: same rows, same query, same
# CLI, same _delta_log layout (CONVERT TO DELTA collects stats by default).
# B vs C is informational, not the headline.
#
# Prereq: write-perf must have been run at least once so that
# pbi.bench.dim_customer_insert exists.
#
# Usage:   ./run.sh
#          NOTE="row-group hypothesis check" ./run.sh
#          WARM_RUNS=3 ./run.sh                       # 3 warm runs per slot
#          SKIP_SETUP=1 ./run.sh                      # skip duck write + DF register

set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
RESULTS="$HERE/results.csv"
READ_PERF="$HERE/../read-perf"

DF_CLI="${DF_CLI:-/a/delta-forge/target/release/delta-forge-cli.exe}"
DUCKDB="${DUCKDB:-/a/tmp/duckdb/duckdb.exe}"
CRED_FILE="${CRED_FILE:-A:/delta-forge/.deltaforge/democred.txt}"
WARM_RUNS="${WARM_RUNS:-1}"
NOTE="${NOTE:-}"
SKIP_SETUP="${SKIP_SETUP:-0}"

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

[[ -f "$RESULTS" ]] || echo "timestamp,git_sha,engine,table_source,query,run,seconds,note" > "$RESULTS"

now_ms() { python -c 'import time; print(int(time.time()*1000))' 2>/dev/null || date +%s%3N; }

run_df_once() {
    local script="$1"
    local start end
    start=$(now_ms)
    "$DF_CLI" --username "$DF_USERNAME" --password "$DF_PASSWORD" -y run "$script" >/dev/null 2>&1 || return 1
    end=$(now_ms)
    awk -v a="$start" -v b="$end" 'BEGIN{printf "%.3f", (b-a)/1000.0}'
}

run_duck_once() {
    local script="$1"
    local start end
    start=$(now_ms)
    "$DUCKDB" <"$script" >/dev/null 2>&1 || return 1
    end=$(now_ms)
    awk -v a="$start" -v b="$end" 'BEGIN{printf "%.3f", (b-a)/1000.0}'
}

setup_one_shot() {
    if [[ "$SKIP_SETUP" == "1" ]]; then
        echo "[setup] skipped (SKIP_SETUP=1)"
        return
    fi
    echo "[setup] DuckDB: writing 5M rows to dim_customer_duck/ ..."
    "$DUCKDB" <"$HERE/duck_write.sql"
    echo "[setup] DeltaForge: CONVERT TO DELTA + REGISTER TABLE ..."
    "$DF_CLI" --username "$DF_USERNAME" --password "$DF_PASSWORD" -y run "$HERE/df_register.sql"
}

bench_query() {
    local query_label="$1" df_insert_script="$2" df_duck_script="$3" duck_script="$4"

    # prime page cache for all three reads before the recorded warm runs
    run_df_once   "$df_insert_script" >/dev/null || true
    run_df_once   "$df_duck_script"   >/dev/null
    run_duck_once "$duck_script"      >/dev/null

    for i in $(seq 1 "$WARM_RUNS"); do
        local df_insert_t df_duck_t duck_t
        df_insert_t=$(run_df_once   "$df_insert_script")
        df_duck_t=$(run_df_once     "$df_duck_script")
        duck_t=$(run_duck_once      "$duck_script")
        echo "$TS,$GIT_SHA,df,df_written,$query_label,$i,$df_insert_t,${NOTE//,/;}"   >> "$RESULTS"
        echo "$TS,$GIT_SHA,df,duck_written,$query_label,$i,$df_duck_t,${NOTE//,/;}"   >> "$RESULTS"
        echo "$TS,$GIT_SHA,duck,duck_written,$query_label,$i,$duck_t,${NOTE//,/;}"    >> "$RESULTS"
        printf "  %-9s  run %d   df-on-df=%6.3fs   df-on-duck=%6.3fs   duck=%6.3fs   df-on-df / df-on-duck = %.2fx\n" \
            "$query_label" "$i" "$df_insert_t" "$df_duck_t" "$duck_t" \
            "$(awk -v a="$df_insert_t" -v b="$df_duck_t" 'BEGIN{ if (b > 0) printf "%.2f", a/b; else print "n/a" }')"
    done
}

setup_one_shot

echo ""
echo "rowgroup-bench: git=$GIT_SHA  ts=$TS  warm_runs=$WARM_RUNS"
echo ""
printf "  %-9s  %-5s   %-14s   %-15s   %-7s   %s\n" "query" "run" "df-on-df(s)" "df-on-duck(s)" "duck(s)" "df-df/df-duck"
printf "  %-9s  %-5s   %-14s   %-15s   %-7s   %s\n" "---------" "-----" "--------------" "---------------" "-------" "-------------"

bench_query "q1_count" "$READ_PERF/df_q1_count.sql" "$HERE/df_q1_count.sql" "$HERE/duck_q1_count.sql"
bench_query "q2_agg"   "$READ_PERF/df_q2_agg.sql"   "$HERE/df_q2_agg.sql"   "$HERE/duck_q2_agg.sql"
bench_query "q3_topk"  "$READ_PERF/df_q3_topk.sql"  "$HERE/df_q3_topk.sql"  "$HERE/duck_q3_topk.sql"

echo ""
echo "appended to: $RESULTS"
