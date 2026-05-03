#!/usr/bin/env bash
# read-perf-10m: 10M-row sibling of read-perf. Same three queries, same
# engines (DeltaForge vs DuckDB), but against pbi.bench.dim_customer_insert_10m
# (43 cols, ~450 MB across 5-6 parquet files at this scale) so we can see
# whether the DF/DuckDB ratio scales with rows.
#
# DuckDB reads the same DF-written parquet files directly via read_parquet,
# so both engines see identical physical bytes (apples-to-apples).
#
# Usage:
#   ./run.sh                          # write data (if missing) and bench
#   SKIP_WRITE=1 ./run.sh             # skip the write, reuse existing 10M table
#   WARM_RUNS=3 ./run.sh              # 3 warm runs per query
#   NOTE="ratio at 10M" ./run.sh      # tag results.csv

set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
RESULTS="$HERE/results.csv"

DF_CLI="${DF_CLI:-/a/delta-forge/target/release/delta-forge-cli.exe}"
DUCKDB="${DUCKDB:-/a/tmp/duckdb/duckdb.exe}"
CRED_FILE="${CRED_FILE:-A:/delta-forge/.deltaforge/democred.txt}"
WARM_RUNS="${WARM_RUNS:-1}"
NOTE="${NOTE:-}"
SKIP_WRITE="${SKIP_WRITE:-0}"
TABLE_DIR="B:/odbc_df/df-demo/perf_test/dim_customer_insert_10m"

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

write_one_shot() {
    if [[ "$SKIP_WRITE" == "1" ]]; then
        echo "[setup] skipping write (SKIP_WRITE=1)"
        return
    fi
    if [[ -d "$TABLE_DIR/_delta_log" ]]; then
        echo "[setup] $TABLE_DIR already populated, skipping write (delete the dir or unset SKIP_WRITE to re-write)"
        return
    fi
    echo "[setup] writing 10M rows to pbi.bench.dim_customer_insert_10m (this takes a few minutes) ..."
    local start end
    start=$(now_ms)
    "$DF_CLI" --username "$DF_USERNAME" --password "$DF_PASSWORD" -y run "$HERE/df_write.sql"
    end=$(now_ms)
    printf "[setup] write done in %.1fs\n" \
        "$(awk -v a="$start" -v b="$end" 'BEGIN{printf "%.1f", (b-a)/1000.0}')"
}

bench_pair() {
    local query_label="$1" df_script="$2" duck_script="$3"

    # prime cache (same warm-cache convention as read-perf)
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

write_one_shot

echo ""
echo "read-perf-10m bench: git=$GIT_SHA  ts=$TS  warm_runs=$WARM_RUNS"
echo ""
printf "  %-12s  %-5s   %s   %s   %s\n" "query" "run" "df(s)" "duck(s)" "df/duck"
printf "  %-12s  %-5s   %s   %s   %s\n" "------------" "-----" "------" "--------" "---------"

bench_pair "q1_count"  "df_q1_count.sql"  "duck_q1_count.sql"
bench_pair "q2_agg"    "df_q2_agg.sql"    "duck_q2_agg.sql"
bench_pair "q3_topk"   "df_q3_topk.sql"   "duck_q3_topk.sql"

echo ""
echo "appended to: $RESULTS"
