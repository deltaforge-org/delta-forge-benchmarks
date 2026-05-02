#!/usr/bin/env bash
# TPC-H setup: generate dataset via DuckDB's tpch extension, write each
# table to its own parquet directory, then register one external table per
# table in DeltaForge under tpch.sf{SF}.{table}.
#
# Usage:
#   ./setup.sh
#   SCALE_FACTOR=10 ./setup.sh
#
# Env overrides:
#   SCALE_FACTOR / SF   TPC-H scale factor (default 1; 1 = ~1 GB raw)
#   OUT_DIR             parquet root in Windows form (default B:/odbc_df/df-demo/tpch)
#   DUCKDB              path to duckdb.exe
#   DF_CLI              path to delta-forge-cli.exe
#   CRED_FILE           democred.txt path
#   DF_USERNAME / DF_PASSWORD   bypass CRED_FILE
#
# Re-running with the same SCALE_FACTOR overwrites the parquet files and
# re-registers the external tables (CREATE OR REPLACE), so this script is
# idempotent.

set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"

SF="${SCALE_FACTOR:-${SF:-1}}"
OUT_DIR="${OUT_DIR:-B:/odbc_df/df-demo/tpch}"
DUCKDB="${DUCKDB:-/a/tmp/duckdb/duckdb.exe}"
DF_CLI="${DF_CLI:-/a/delta-forge/target/release/delta-forge-cli.exe}"
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

# Convert Windows-form OUT_DIR (B:/...) to a bash-form path (/b/...) for mkdir / stat.
win_to_bash() {
    local p="$1"
    if [[ "$p" =~ ^[A-Za-z]:/ ]]; then
        local drive="${p:0:1}"
        local rest="${p:2}"
        # Lowercase the drive letter via tr; works in Git Bash without locale tricks.
        drive=$(printf '%s' "$drive" | tr '[:upper:]' '[:lower:]')
        printf '/%s%s\n' "$drive" "$rest"
    else
        printf '%s\n' "$p"
    fi
}

SF_WIN="$OUT_DIR/sf$SF"
SF_BASH=$(win_to_bash "$SF_WIN")

TABLES=(nation region part supplier partsupp customer orders lineitem)

echo "==> [1/4] preparing target directories under $SF_BASH"
mkdir -p "$SF_BASH"
for t in "${TABLES[@]}"; do
    mkdir -p "$SF_BASH/$t"
    rm -f "$SF_BASH/$t"/*.parquet 2>/dev/null || true
done

echo "==> [2/4] generating TPC-H SF=$SF data via DuckDB tpch extension"
GEN_SQL="$(mktemp /tmp/tpch_dbgen_XXXXXX.sql)"
{
    echo "INSTALL tpch;"
    echo "LOAD tpch;"
    echo "CALL dbgen(sf=$SF);"
    for t in "${TABLES[@]}"; do
        echo "COPY $t TO '$SF_WIN/$t/${t}.parquet' (FORMAT PARQUET, COMPRESSION 'snappy');"
    done
} > "$GEN_SQL"

"$DUCKDB" :memory: < "$GEN_SQL"
rm -f "$GEN_SQL"

echo "==> [3/4] verifying parquet output"
total=0
for t in "${TABLES[@]}"; do
    f="$SF_BASH/$t/${t}.parquet"
    if [[ ! -f "$f" ]]; then
        echo "MISSING: $f" >&2
        exit 1
    fi
    sz=$(stat -c%s "$f")
    total=$((total + sz))
    printf "    %-10s  %12d bytes  %s\n" "$t" "$sz" "$f"
done
printf "    %-10s  %12d bytes (%.1f MB)\n" "TOTAL" "$total" "$(awk -v b=$total 'BEGIN{printf "%.1f", b/1048576}')"

echo "==> [4/4] registering external tables in DeltaForge (zone=tpch, schema=sf$SF)"
# DeltaForge resolves every CREATE TABLE LOCATION into the zone's storage_root
# (see arch_location_resolution). For an absolute LOCATION whose prefix matches
# the zone root, the prefix is stripped and the rest is rejoined under the
# root. So the zone's storage_root must equal $OUT_DIR for our absolute
# parquet paths to resolve correctly. The default zone root (in AppData) would
# fold our paths into AppData and the readers would not find any files.
#
# This block wipes and re-registers the tpch zone so its storage_root always
# matches $OUT_DIR. Side effect: any other schemas that happen to live under
# the tpch zone are dropped too. For a multi-SF setup, run setup.sh once per
# SF before running any benchmark; the last invocation defines the final zone.
DDL="$(mktemp /tmp/tpch_ddl_XXXXXX.sql)"
{
    for t in "${TABLES[@]}"; do
        echo "DROP EXTERNAL TABLE IF EXISTS tpch.sf$SF.$t;"
    done
    echo "DROP SCHEMA IF EXISTS tpch.sf$SF;"
    echo "DROP ZONE IF EXISTS tpch;"
    echo "CREATE ZONE tpch STORAGE_ROOT = '$OUT_DIR' COMMENT 'TPC-H benchmark data';"
    echo "CREATE SCHEMA tpch.sf$SF COMMENT 'TPC-H scale factor $SF';"
    for t in "${TABLES[@]}"; do
        echo "CREATE EXTERNAL TABLE tpch.sf$SF.$t USING PARQUET LOCATION '$SF_WIN/$t/';"
    done
} > "$DDL"

"$DF_CLI" --username "$DF_USERNAME" --password "$DF_PASSWORD" -y run "$DDL"
rm -f "$DDL"

echo ""
echo "DONE."
echo "    parquet root:  $SF_WIN"
echo "    DF schema:     tpch.sf$SF"
echo ""
echo "Sanity check:"
echo "    $DF_CLI --username \$user --password \$pwd -y run -c \"SELECT COUNT(*) FROM tpch.sf$SF.lineitem\""
