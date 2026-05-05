#!/usr/bin/env bash
# index-perf: measure read speedup from a row-level index against two
# baselines:
#   - DeltaForge with no index (file-stats pruning + column scan)
#   - DuckDB reading the same parquet files via read_parquet (no Delta
#     log; no secondary index — DuckDB has none for parquet)
#
# Auth uses a Personal Access Token (PAT) so each invocation skips the
# password grant entirely. The cred file format is read via awk; a PAT
# has no special characters so it survives bash quoting cleanly.
#
# Timings come from the CLI's per-statement output (`OK (NNNms)`) NOT
# from wall-clock around the invocation, so the ~1s auth + binary
# startup overhead is excluded from query measurements. Wall-clock is
# still recorded as a secondary signal.
#
# Usage:
#   ./run.sh                       # 1 measured run per query
#   WARM_RUNS=3 ./run.sh           # 3 measured runs per query
#   SKIP_BUILD_INDEX=1 ./run.sh    # reuse the index from a prior run
#   SKIP_DUCK=1 ./run.sh           # skip the DuckDB baseline
#   SKIP_WRITE=1 ./run.sh          # assume the fixture exists
#   NOTE="X" ./run.sh              # tag results.csv

set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
RESULTS="$HERE/results.csv"

DF_CLI="${DF_CLI:-/a/delta-forge/target/release/delta-forge-cli.exe}"
DUCKDB="${DUCKDB:-/a/tmp/duckdb/duckdb.exe}"
CRED_FILE="${CRED_FILE:-A:/delta-forge/.deltaforge/democred.txt}"
WARM_RUNS="${WARM_RUNS:-1}"
NOTE="${NOTE:-}"
SKIP_BUILD_INDEX="${SKIP_BUILD_INDEX:-0}"
SKIP_DUCK="${SKIP_DUCK:-0}"
SKIP_WRITE="${SKIP_WRITE:-0}"
TABLE_DIR="${TABLE_DIR:-B:/odbc_df/tmp/index-perf-data/idx_perf_customer_10m}"

# Resolve PAT from env or the cred file. PAT only — no password fallback,
# because the password contains a double-quote that breaks bash escaping
# across nested invocations.
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

[[ -f "$RESULTS" ]] || echo "timestamp,git_sha,mode,query,run,sql_ms,wall_ms,note" > "$RESULTS"

now_ms() { python -c 'import time; print(int(time.time()*1000))' 2>/dev/null || date +%s%3N; }

# Run a DF script and emit "<sql_ms>\t<wall_ms>" on stdout.
# `sql_ms` is the sum of all per-statement timings reported by the CLI
# (excludes auth + binary startup). `wall_ms` is the full process time.
# Errors are surfaced — we no longer swallow stderr.
run_df_once() {
    local script="$1"
    local out start end wall_ms sql_ms
    start=$(now_ms)
    if ! out=$("$DF_CLI" --token "$DF_TOKEN" -y run "$HERE/$script" 2>&1); then
        echo "DF script $script failed:" >&2
        echo "$out" >&2
        return 1
    fi
    end=$(now_ms)
    wall_ms=$((end - start))
    # Parse "(NNNms)" timings from each "[i/N] ... OK (NNNms)" line and
    # sum them. The CLI reports per-statement, so for a single-query
    # script there's one match.
    sql_ms=$(echo "$out" | awk 'match($0, /\(([0-9]+)ms\)/, m) { s += m[1] } END { print s+0 }')
    printf "%d\t%d" "$sql_ms" "$wall_ms"
}

# Run a DuckDB script. `.timer on` directive is injected so DuckDB
# reports its own per-query time. We grep the "Run Time (s):" line
# and convert to ms; if absent, fall back to wall clock.
run_duck_once() {
    local script="$1"
    local out start end wall_ms sql_ms
    start=$(now_ms)
    if ! out=$(printf ".timer on\n%s" "$(cat "$HERE/$script")" | "$DUCKDB" 2>&1); then
        echo "DuckDB script $script failed:" >&2
        echo "$out" >&2
        return 1
    fi
    end=$(now_ms)
    wall_ms=$((end - start))
    # DuckDB prints e.g. "Run Time (s): real 0.034 user 0.000 sys 0.000"
    # Take the first "real X" value across statements.
    sql_ms=$(echo "$out" | awk 'match($0, /real ([0-9.]+)/, m) { printf "%d", m[1] * 1000; exit }')
    if [[ -z "$sql_ms" ]]; then sql_ms="$wall_ms"; fi
    printf "%d\t%d" "$sql_ms" "$wall_ms"
}

run_admin_once() {
    "$DF_CLI" --token "$DF_TOKEN" -y run "$HERE/$1"
}

bench_one_df() {
    local mode="$1" query="$2" script="$3"
    run_df_once "$script" >/dev/null   # prime
    for i in $(seq 1 "$WARM_RUNS"); do
        local result sql_ms wall_ms
        result=$(run_df_once "$script")
        sql_ms=$(echo "$result" | cut -f1)
        wall_ms=$(echo "$result" | cut -f2)
        echo "$TS,$GIT_SHA,$mode,$query,$i,$sql_ms,$wall_ms,${NOTE//,/;}" >> "$RESULTS"
        eval "${mode}_${query}_${i}_sql=\"$sql_ms\""
        eval "${mode}_${query}_${i}_wall=\"$wall_ms\""
    done
}

bench_one_duck() {
    local query="$1" script="$2"
    if [[ "$SKIP_DUCK" == "1" ]]; then
        for i in $(seq 1 "$WARM_RUNS"); do
            eval "duck_${query}_${i}_sql=\"n/a\""
            eval "duck_${query}_${i}_wall=\"n/a\""
        done
        return
    fi
    run_duck_once "$script" >/dev/null
    for i in $(seq 1 "$WARM_RUNS"); do
        local result sql_ms wall_ms
        result=$(run_duck_once "$script")
        sql_ms=$(echo "$result" | cut -f1)
        wall_ms=$(echo "$result" | cut -f2)
        echo "$TS,$GIT_SHA,duck,$query,$i,$sql_ms,$wall_ms,${NOTE//,/;}" >> "$RESULTS"
        eval "duck_${query}_${i}_sql=\"$sql_ms\""
        eval "duck_${query}_${i}_wall=\"$wall_ms\""
    done
}

queries=(
    "point          df_q_point.sql          duck_q_point.sql"
    "in_list        df_q_in_list.sql        duck_q_in_list.sql"
    "narrow_range   df_q_narrow_range.sql   duck_q_narrow_range.sql"
    "medium_range   df_q_medium_range.sql   duck_q_medium_range.sql"
)

echo "index-perf bench: git=$GIT_SHA  ts=$TS  warm_runs=$WARM_RUNS  skip_duck=$SKIP_DUCK"
echo ""

if [[ "$SKIP_WRITE" == "1" ]]; then
    echo "[phase 0/3] SKIP_WRITE=1 — assuming fixture exists at $TABLE_DIR"
elif [[ -d "$TABLE_DIR/_delta_log" ]]; then
    echo "[phase 0/3] fixture already present at $TABLE_DIR (delete the dir or set SKIP_WRITE=1 to skip)"
else
    echo "[phase 0/3] writing 10M-row fixture to $TABLE_DIR ..."
    write_start=$(now_ms)
    run_admin_once "df_write_10m.sql"
    write_end=$(now_ms)
    write_s=$(awk -v a="$write_start" -v b="$write_end" 'BEGIN{printf "%.1f", (b-a)/1000.0}')
    printf "[phase 0/3] fixture written in %ss\n" "$write_s"
    echo "$TS,$GIT_SHA,write,fixture_10m,1,${write_end},$((write_end - write_start)),${NOTE//,/;}" >> "$RESULTS"
fi
echo ""

echo "[phase 1/3] dropping existing index (no-op if absent)"
run_admin_once "df_drop_index.sql"

echo "[phase 1/3] running queries (DF without index + DuckDB) ..."
for line in "${queries[@]}"; do
    read -r label df_script duck_script <<<"$line"
    bench_one_df   "no_index" "$label" "$df_script"
    bench_one_duck "$label" "$duck_script"
done

if [[ "$SKIP_BUILD_INDEX" == "1" ]]; then
    echo "[phase 2/3] SKIP_BUILD_INDEX=1 — assuming index already exists"
else
    echo "[phase 2/3] creating index on customer_id (this scans 10M rows once) ..."
    idx_start=$(now_ms)
    run_admin_once "df_create_index.sql"
    idx_end=$(now_ms)
    idx_s=$(awk -v a="$idx_start" -v b="$idx_end" 'BEGIN{printf "%.1f", (b-a)/1000.0}')
    printf "[phase 2/3] index built in %ss\n" "$idx_s"
    echo "$TS,$GIT_SHA,index_build,idx_customer_id,1,${idx_end},$((idx_end - idx_start)),${NOTE//,/;}" >> "$RESULTS"
fi

echo "[phase 3/3] running queries (DF with index) ..."
for line in "${queries[@]}"; do
    read -r label df_script _ <<<"$line"
    bench_one_df "with_index" "$label" "$df_script"
done

echo ""
printf "  %-15s  %-3s  %-10s  %-10s  %-10s  %-10s  %s\n" \
    "query" "run" "df_idx(ms)" "df_no(ms)" "duck(ms)" "vs df_no" "vs duck"
printf "  %-15s  %-3s  %-10s  %-10s  %-10s  %-10s  %s\n" \
    "---------------" "---" "----------" "----------" "----------" "----------" "----------"

for line in "${queries[@]}"; do
    read -r label _ _ <<<"$line"
    for i in $(seq 1 "$WARM_RUNS"); do
        no_var="no_index_${label}_${i}_sql"
        wi_var="with_index_${label}_${i}_sql"
        dk_var="duck_${label}_${i}_sql"
        no_t="${!no_var}"
        wi_t="${!wi_var}"
        dk_t="${!dk_var}"
        ratio_no=$(awk -v a="$no_t" -v b="$wi_t" 'BEGIN{ if (b > 0) printf "%.2fx", a/b; else print "n/a" }')
        if [[ "$dk_t" == "n/a" ]]; then
            ratio_dk="n/a"
        else
            ratio_dk=$(awk -v a="$dk_t" -v b="$wi_t" 'BEGIN{ if (b > 0) printf "%.2fx", a/b; else print "n/a" }')
        fi
        printf "  %-15s  %-3s  %-10s  %-10s  %-10s  %-10s  %s\n" \
            "$label" "$i" "$wi_t" "$no_t" "$dk_t" "$ratio_no" "$ratio_dk"
    done
done

echo ""
echo "appended to: $RESULTS"
echo ""
echo "columns:"
echo "  df_idx(ms)  DF with index, parsed from CLI per-statement timing"
echo "  df_no(ms)   DF without index, same timing source"
echo "  duck(ms)    DuckDB Run Time, parsed from .timer on output"
echo "  wall_ms     additional column in results.csv: full process time"
echo "              including auth + binary startup (~1000ms overhead per call)"
