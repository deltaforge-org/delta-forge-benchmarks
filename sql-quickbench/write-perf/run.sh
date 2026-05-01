#!/usr/bin/env bash
# Run all three write-perf scripts back-to-back, time each, append one row
# per script to results.csv, and print a comparison table at the end.
#
# Usage:
#   ./run.sh
#
# Env overrides:
#   DF_CLI         path to delta-forge-cli.exe (default: /a/delta-forge/target/release/delta-forge-cli.exe)
#   DUCKDB         path to duckdb.exe          (default: /a/tmp/duckdb/duckdb.exe)
#   OUT_DIR        write target dir            (default: B:/odbc_df/df-demo/perf_test)
#   CRED_FILE      democred.txt file           (default: A:/delta-forge/.deltaforge/democred.txt)
#   DF_USERNAME    bypasses CRED_FILE
#   DF_PASSWORD    bypasses CRED_FILE
#   NOTE           free-text note recorded in results.csv (e.g. "after writer fix")

set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
RESULTS="$HERE/results.csv"

DF_CLI="${DF_CLI:-/a/delta-forge/target/release/delta-forge-cli.exe}"
DUCKDB="${DUCKDB:-/a/tmp/duckdb/duckdb.exe}"
OUT_DIR="${OUT_DIR:-B:/odbc_df/df-demo/perf_test}"
CRED_FILE="${CRED_FILE:-A:/delta-forge/.deltaforge/democred.txt}"
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

mkdir -p "$OUT_DIR"

if [[ ! -f "$RESULTS" ]]; then
    echo "timestamp,git_sha,script,seconds,bytes,rows,files,note" > "$RESULTS"
fi

# ---- helpers ----------------------------------------------------------------

now_ms() { python -c 'import time; print(int(time.time()*1000))' 2>/dev/null || date +%s%3N; }

dir_size_bytes() {
    local p="$1"
    if [[ -d "$p" ]]; then
        find "$p" -type f -name '*.parquet' -printf '%s\n' 2>/dev/null | awk '{s+=$1} END {print s+0}'
    elif [[ -f "$p" ]]; then
        stat -c%s "$p" 2>/dev/null || wc -c <"$p" | tr -d ' '
    else
        echo 0
    fi
}

# ---- runs -------------------------------------------------------------------

run_df() {
    local script="$1" label="$2" out_path="$3"
    local start end secs
    start=$(now_ms)
    "$DF_CLI" --username "$DF_USERNAME" --password "$DF_PASSWORD" -y run "$HERE/$script" >/dev/null 2>&1 \
        || { echo "FAIL: $label"; return 1; }
    end=$(now_ms)
    secs=$(awk -v a="$start" -v b="$end" 'BEGIN{printf "%.3f", (b-a)/1000.0}')
    local bytes; bytes=$(dir_size_bytes "$out_path")
    echo "$TS,$GIT_SHA,$label,$secs,$bytes,5000000,,${NOTE//,/;}" >> "$RESULTS"
    printf "  %-15s  %7.2fs  %8.1f MB\n" "$label" "$secs" "$(awk -v b="$bytes" 'BEGIN{printf "%.1f", b/1048576}')"
}

run_duck() {
    local script="$1" label="$2" out_path="$3"
    local start end secs
    start=$(now_ms)
    "$DUCKDB" <"$HERE/$script" >/dev/null 2>&1 \
        || { echo "FAIL: $label"; return 1; }
    end=$(now_ms)
    secs=$(awk -v a="$start" -v b="$end" 'BEGIN{printf "%.3f", (b-a)/1000.0}')
    local bytes; bytes=$(dir_size_bytes "$out_path")
    echo "$TS,$GIT_SHA,$label,$secs,$bytes,5000000,1,${NOTE//,/;}" >> "$RESULTS"
    printf "  %-15s  %7.2fs  %8.1f MB\n" "$label" "$secs" "$(awk -v b="$bytes" 'BEGIN{printf "%.1f", b/1048576}')"
}

# ---- main -------------------------------------------------------------------

echo "write-perf bench: git=$GIT_SHA  ts=$TS"
echo ""
printf "  %-15s  %7s   %8s\n" "script" "wall(s)" "size(MB)"
printf "  %-15s  %7s   %8s\n" "---------------" "-------" "--------"

run_duck "duckdb_copy.sql" "duckdb_copy" "$OUT_DIR/duck_dim_customer.parquet"
run_df   "df_ctas.sql"     "df_ctas"     "$OUT_DIR/dim_customer_ctas"
run_df   "df_insert.sql"   "df_insert"   "$OUT_DIR/dim_customer_insert"

echo ""
echo "appended to: $RESULTS"
echo "compare against BASELINE.md to spot regressions."
