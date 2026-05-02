#!/usr/bin/env bash
# Run TPC-H queries against DeltaForge and DuckDB, time each, append rows to
# results.csv. Each query is primed once (warm cache) then recorded WARM_RUNS
# times per engine.
#
# Usage:
#   ./run.sh
#   WARM_RUNS=5 ./run.sh
#   QUERIES="q01 q06" ./run.sh
#   SCALE_FACTOR=10 ./run.sh
#
# Env overrides:
#   SCALE_FACTOR / SF   TPC-H scale factor to query (default 1)
#   QUERIES             whitespace-separated query IDs (default: q01 q03 q06 q10)
#   WARM_RUNS           recorded warm runs per (engine, query); default 3
#   NOTE                free-text note recorded in results.csv
#   DF_CLI / DUCKDB / CRED_FILE / DF_USERNAME / DF_PASSWORD: same as setup.sh

set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
RESULTS="$HERE/results.csv"

SF="${SCALE_FACTOR:-${SF:-1}}"
QUERIES="${QUERIES:-q01 q03 q06 q10}"
WARM_RUNS="${WARM_RUNS:-3}"
NOTE="${NOTE:-}"

DF_CLI="${DF_CLI:-/a/delta-forge/target/release/delta-forge-cli.exe}"
DUCKDB="${DUCKDB:-/a/tmp/duckdb/duckdb.exe}"
CRED_FILE="${CRED_FILE:-/a/delta-forge/.deltaforge/democred.txt}"

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

if [[ ! -f "$RESULTS" ]]; then
    echo "timestamp,git_sha,scale_factor,engine,query,run,seconds,note" > "$RESULTS"
fi

now_ms() { python -c 'import time; print(int(time.time()*1000))' 2>/dev/null || date +%s%3N; }

prepare_template() {
    local src="$1" dst="$2"
    sed "s/{{SF}}/$SF/g" "$src" > "$dst"
}

run_df_once() {
    local query_file="$1"
    local start end
    start=$(now_ms)
    if ! "$DF_CLI" --username "$DF_USERNAME" --password "$DF_PASSWORD" -y run "$query_file" >/dev/null 2>&1; then
        return 1
    fi
    end=$(now_ms)
    awk -v a="$start" -v b="$end" 'BEGIN{printf "%.3f", (b-a)/1000.0}'
}

run_duck_once() {
    local query_file="$1"
    local start end
    start=$(now_ms)
    if ! "$DUCKDB" :memory: < "$query_file" >/dev/null 2>&1; then
        return 1
    fi
    end=$(now_ms)
    awk -v a="$start" -v b="$end" 'BEGIN{printf "%.3f", (b-a)/1000.0}'
}

bench_query() {
    local q="$1"
    local df_src="$HERE/queries/df/$q.sql"
    local duck_src="$HERE/queries/duck/$q.sql"

    if [[ ! -f "$df_src" ]]; then echo "MISSING: $df_src" >&2; return 1; fi
    if [[ ! -f "$duck_src" ]]; then echo "MISSING: $duck_src" >&2; return 1; fi

    local df_tmp duck_tmp
    df_tmp=$(mktemp /tmp/tpch_df_${q}_XXXXXX.sql)
    duck_tmp=$(mktemp /tmp/tpch_duck_${q}_XXXXXX.sql)
    prepare_template "$df_src"   "$df_tmp"
    prepare_template "$duck_src" "$duck_tmp"

    # Prime caches; record nothing.
    if ! run_df_once "$df_tmp" >/dev/null; then
        echo "  $q  PRIME df FAIL (skipping)"
        echo "$TS,$GIT_SHA,$SF,df,$q,prime,FAIL,${NOTE//,/;}" >> "$RESULTS"
        rm -f "$df_tmp" "$duck_tmp"
        return 0
    fi
    if ! run_duck_once "$duck_tmp" >/dev/null; then
        echo "  $q  PRIME duck FAIL (skipping)"
        echo "$TS,$GIT_SHA,$SF,duck,$q,prime,FAIL,${NOTE//,/;}" >> "$RESULTS"
        rm -f "$df_tmp" "$duck_tmp"
        return 0
    fi

    local i df_t duck_t ratio_s
    for i in $(seq 1 "$WARM_RUNS"); do
        df_t=$(run_df_once   "$df_tmp")   || df_t="FAIL"
        duck_t=$(run_duck_once "$duck_tmp") || duck_t="FAIL"
        echo "$TS,$GIT_SHA,$SF,df,$q,$i,$df_t,${NOTE//,/;}"   >> "$RESULTS"
        echo "$TS,$GIT_SHA,$SF,duck,$q,$i,$duck_t,${NOTE//,/;}" >> "$RESULTS"
        if [[ "$df_t" != "FAIL" && "$duck_t" != "FAIL" ]]; then
            ratio_s=$(awk -v a="$df_t" -v b="$duck_t" 'BEGIN{ if (b > 0) printf "%.2fx", a/b; else print "n/a" }')
            printf "  %-6s  run %d   df=%6.3fs   duck=%6.3fs   ratio=%s\n" \
                "$q" "$i" "$df_t" "$duck_t" "$ratio_s"
        else
            printf "  %-6s  run %d   df=%-7s   duck=%-7s\n" "$q" "$i" "$df_t" "$duck_t"
        fi
    done

    rm -f "$df_tmp" "$duck_tmp"
}

echo "tpch bench: git=$GIT_SHA  ts=$TS  sf=$SF  warm_runs=$WARM_RUNS"
echo "queries:    $QUERIES"
echo ""
printf "  %-6s  %-5s   %-7s   %-7s   %s\n" "query" "run" "df(s)" "duck(s)" "df/duck"
printf "  %-6s  %-5s   %-7s   %-7s   %s\n" "------" "-----" "-------" "-------" "-------"

for q in $QUERIES; do
    bench_query "$q"
done

echo ""
echo "appended to: $RESULTS"
echo "report:      python report.py   (writes report.md and report.png)"
