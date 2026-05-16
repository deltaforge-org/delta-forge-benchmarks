#!/usr/bin/env bash
# index-perf: measure read speedup from a row-level index at two scales
# (10M rows and 30M rows) to show how the speedup grows as table size increases.
#
# The indexed path reads a fixed number of row pointers regardless of table
# size. The no-index path must scan the entire column, so its cost grows
# linearly with rows. Running both scales in one pass makes that contrast
# directly visible in the results CSV.
#
# Auth uses a Personal Access Token (PAT) from $DF_TOKEN or the cred file.
#
# Timings come from SHOW STATS ACTUAL total_time_ms (engine-internal, excludes
# auth and binary startup). DuckDB uses .timer on for comparable timing.
#
# Usage:
#   ./run.sh                       # 1 measured run per query
#   WARM_RUNS=3 ./run.sh           # 3 measured runs per query
#   SKIP_WRITE_10M=1 ./run.sh      # reuse existing 10M fixture
#   SKIP_WRITE_30M=1 ./run.sh      # reuse existing 30M fixture
#   SKIP_DUCK=1 ./run.sh           # skip DuckDB baseline
#   NOTE="X" ./run.sh              # tag results.csv

set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
RESULTS="$HERE/results.csv"

DF_CLI="${DF_CLI:-/a/delta-forge/target/release/delta-forge-cli.exe}"
DUCKDB="${DUCKDB:-/a/tmp/duckdb/duckdb.exe}"
CRED_FILE="${CRED_FILE:-A:/delta-forge/.deltaforge/democred.txt}"
WARM_RUNS="${WARM_RUNS:-1}"
NOTE="${NOTE:-}"
SKIP_DUCK="${SKIP_DUCK:-0}"
SKIP_WRITE_10M="${SKIP_WRITE_10M:-0}"
SKIP_WRITE_30M="${SKIP_WRITE_30M:-0}"
TABLE_DIR_10M="${TABLE_DIR_10M:-B:/odbc_df/tmp/index-perf-data/idx_perf_customer_10m}"
TABLE_DIR_30M="${TABLE_DIR_30M:-B:/odbc_df/tmp/index-perf-data/idx_perf_customer_30m}"

if [[ -z "${DF_TOKEN:-}" ]]; then
    if [[ -r "$CRED_FILE" ]]; then
        DF_TOKEN=$(awk -F': ' '/PATH FOR CLI:/ {print $2}' "$CRED_FILE")
    fi
fi
if [[ -z "${DF_TOKEN:-}" ]]; then
    echo "ERROR: set DF_TOKEN, or provide a PAT line in $CRED_FILE" >&2
    exit 1
fi

GIT_SHA=$(git -C "$HERE" rev-parse --short HEAD 2>/dev/null || echo unknown)
TS=$(date +%Y-%m-%dT%H:%M:%S)

# CSV gains a 'scale' column so 10M and 30M rows are cleanly separable.
[[ -f "$RESULTS" ]] || echo "timestamp,git_sha,scale,mode,query,run,sql_ms,wall_ms,note" > "$RESULTS"

now_ms() { python -c 'import time; print(int(time.time()*1000))' 2>/dev/null || date +%s%3N; }

# Run a DF script and emit "<sql_ms>\t<wall_ms>".
# sql_ms = SHOW STATS ACTUAL total_time_ms (pure engine time).
# wall_ms = full process wall-clock.
run_df_once() {
    local script="$1"
    local out total_ms start end wall_ms
    start=$(now_ms)
    if ! out=$( { printf 'SHOW STATS ACTUAL '; cat "$HERE/$script"; } \
            | "$DF_CLI" --token "$DF_TOKEN" --format json 2>&1); then
        echo "DF script $script failed:" >&2
        echo "$out" >&2
        return 1
    fi
    end=$(now_ms)
    wall_ms=$((end - start))
    total_ms=$(echo "$out" | grep -A1 '"metric": "total_time_ms"' \
        | grep '"value"' | sed 's/.*"value": "\([^"]*\)".*/\1/' \
        | awk '{printf "%d", $1}')
    [[ -n "$total_ms" ]] || { echo "DF script $script: failed to parse SHOW STATS output" >&2; return 1; }
    printf "%d\t%d" "$total_ms" "$wall_ms"
}

run_duck_once() {
    local script="$1"
    local out start end wall_ms sql_ms
    start=$(now_ms)
    if ! out=$( { printf '.timer on\n'; cat "$HERE/$script"; } | "$DUCKDB" 2>/dev/null); then
        echo "DuckDB script $script failed:" >&2
        return 1
    fi
    end=$(now_ms)
    wall_ms=$((end - start))
    sql_ms=$(echo "$out" | awk 'match($0, /real ([0-9.]+)/, m) { printf "%d", m[1] * 1000; exit }')
    if [[ -z "$sql_ms" ]]; then sql_ms="$wall_ms"; fi
    printf "%d\t%d" "$sql_ms" "$wall_ms"
}

run_admin_once() {
    "$DF_CLI" --token "$DF_TOKEN" -y run "$HERE/$1"
}

bench_one_df() {
    local scale="$1" mode="$2" query="$3" script="$4"
    run_df_once "$script" >/dev/null   # prime
    for i in $(seq 1 "$WARM_RUNS"); do
        local result sql_ms wall_ms
        result=$(run_df_once "$script")
        sql_ms=$(echo "$result" | cut -f1)
        wall_ms=$(echo "$result" | cut -f2)
        echo "$TS,$GIT_SHA,$scale,$mode,$query,$i,$sql_ms,$wall_ms,${NOTE//,/;}" >> "$RESULTS"
        eval "${mode//-/_}_${query}_${i}_sql_${scale}=\"$sql_ms\""
        eval "${mode//-/_}_${query}_${i}_wall_${scale}=\"$wall_ms\""
    done
}

bench_one_duck() {
    local scale="$1" query="$2" script="$3"
    if [[ "$SKIP_DUCK" == "1" ]]; then
        for i in $(seq 1 "$WARM_RUNS"); do
            eval "duck_${query}_${i}_sql_${scale}=\"n/a\""
            eval "duck_${query}_${i}_wall_${scale}=\"n/a\""
        done
        return
    fi
    run_duck_once "$script" >/dev/null
    for i in $(seq 1 "$WARM_RUNS"); do
        local result sql_ms wall_ms
        result=$(run_duck_once "$script")
        sql_ms=$(echo "$result" | cut -f1)
        wall_ms=$(echo "$result" | cut -f2)
        echo "$TS,$GIT_SHA,$scale,duck,$query,$i,$sql_ms,$wall_ms,${NOTE//,/;}" >> "$RESULTS"
        eval "duck_${query}_${i}_sql_${scale}=\"$sql_ms\""
        eval "duck_${query}_${i}_wall_${scale}=\"$wall_ms\""
    done
}

# Run the full index cycle for one scale (10m or 30m).
# suffix: "" for 10M (existing file names), "_30m" for 30M.
run_scale() {
    local scale="$1" suffix="$2" table_dir="$3" skip_write="$4"

    echo ""
    echo "========================================"
    echo "  scale=$scale   table_dir=$table_dir"
    echo "========================================"

    # Phase 0: write fixture
    local write_sql="df_write${suffix}.sql"
    if [[ "$skip_write" == "1" ]]; then
        echo "[0] SKIP_WRITE — assuming fixture at $table_dir"
    elif [[ -d "$table_dir/_delta_log" ]]; then
        echo "[0] fixture present at $table_dir"
    else
        echo "[0] writing $scale fixture (may take several minutes)..."
        local w0 w1
        w0=$(now_ms)
        run_admin_once "$write_sql"
        w1=$(now_ms)
        printf "[0] fixture written in %.1fs\n" \
            "$(awk -v a="$w0" -v b="$w1" 'BEGIN{printf "%.1f", (b-a)/1000.0}')"
        echo "$TS,$GIT_SHA,$scale,write,fixture,1,$w1,$((w1-w0)),${NOTE//,/;}" >> "$RESULTS"
    fi

    # Phase 1: register, drop existing index, run no-index queries
    echo "[1] registering table..."
    run_admin_once "df_open_table${suffix}.sql"

    echo "[1] dropping existing index (no-op if absent)..."
    run_admin_once "df_drop_index${suffix}.sql"

    echo "[1] no-index queries..."
    local queries=(
        "point        df_q_point${suffix}.sql        duck_q_point${suffix}.sql"
        "in_list      df_q_in_list${suffix}.sql      duck_q_in_list${suffix}.sql"
        "narrow_range df_q_narrow_range${suffix}.sql duck_q_narrow_range${suffix}.sql"
        "medium_range df_q_medium_range${suffix}.sql duck_q_medium_range${suffix}.sql"
    )
    for line in "${queries[@]}"; do
        read -r label df_script duck_script <<<"$line"
        bench_one_df   "$scale" "no_index" "$label" "$df_script"
        bench_one_duck "$scale" "$label" "$duck_script"
    done

    # Phase 2: build index
    echo "[2] creating index on customer_id..."
    local idx_start idx_end
    idx_start=$(now_ms)
    run_admin_once "df_create_index${suffix}.sql"
    idx_end=$(now_ms)
    printf "[2] index built in %.1fs\n" \
        "$(awk -v a="$idx_start" -v b="$idx_end" 'BEGIN{printf "%.1f", (b-a)/1000.0}')"
    echo "$TS,$GIT_SHA,$scale,index_build,idx_customer_id,1,$idx_end,$((idx_end-idx_start)),${NOTE//,/;}" >> "$RESULTS"

    # Phase 3: indexed queries
    echo "[3] with-index queries..."
    for line in "${queries[@]}"; do
        read -r label df_script _ <<<"$line"
        bench_one_df "$scale" "with_index" "$label" "$df_script"
    done

    # Summary table
    echo ""
    printf "  %-15s %-5s %12s %12s %12s %10s %10s\n" \
        "query" "run" "idx(ms)" "no_idx(ms)" "duck(ms)" "vs no_idx" "vs duck"
    printf "  %-15s %-5s %12s %12s %12s %10s %10s\n" \
        "---------------" "---" "------------" "------------" "------------" "----------" "----------"
    for line in "${queries[@]}"; do
        read -r label _ _ <<<"$line"
        for i in $(seq 1 "$WARM_RUNS"); do
            local ni_var wi_var dk_var
            # Bash variable names can't have hyphens; bench_one_df normalises them
            ni_var="no_index_${label}_${i}_sql_${scale}"
            wi_var="with_index_${label}_${i}_sql_${scale}"
            dk_var="duck_${label}_${i}_sql_${scale}"
            local ni_t="${!ni_var}" wi_t="${!wi_var}" dk_t="${!dk_var}"
            local r_no r_dk
            r_no=$(awk -v a="$ni_t" -v b="$wi_t" 'BEGIN{ if (b>0) printf "%.2fx", a/b; else print "n/a" }')
            if [[ "$dk_t" == "n/a" ]]; then r_dk="n/a"
            else r_dk=$(awk -v a="$dk_t" -v b="$wi_t" 'BEGIN{ if (b>0) printf "%.2fx", a/b; else print "n/a" }')
            fi
            printf "  %-15s %-5s %12s %12s %12s %10s %10s\n" \
                "$label" "$i" "$wi_t" "$ni_t" "$dk_t" "$r_no" "$r_dk"
        done
    done
}

echo "index-perf bench: git=$GIT_SHA  ts=$TS  warm_runs=$WARM_RUNS  skip_duck=$SKIP_DUCK"

run_scale "10m" ""     "$TABLE_DIR_10M" "$SKIP_WRITE_10M"
run_scale "30m" "_30m" "$TABLE_DIR_30M" "$SKIP_WRITE_30M"

echo ""
echo "appended to: $RESULTS"
echo "scale column separates 10m vs 30m rows for direct speedup comparison."
