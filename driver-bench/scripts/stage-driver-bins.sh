#!/usr/bin/env bash
# Stage locally compiled DeltaForge ODBC + ADBC driver shared objects for
# a BUILD_MODE=local docker build of the driver-bench image. Run this
# from the repo root before `docker compose -f driver-bench/docker/docker-compose.yml build`.
#
# Usage:
#   ./driver-bench/scripts/stage-driver-bins.sh
#
# Default sources:
#   ODBC: ../delta-forge/delta-forge-odbc/build/libdeltaforgeodbc.so
#   ADBC: ../delta-forge/build/adbc-linux-x64/libdeltaforge_adbc.so.1.0.0
#
# Overrides:
#   DF_ODBC_SO=/abs/path/libdeltaforgeodbc.so
#   DF_ADBC_SO=/abs/path/libdeltaforge_adbc.so.1.0.0
#
# The .so files must be Linux ELF (not Windows .dll). Build them in WSL or CI.

set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
BENCH_REPO="$(cd "$HERE/../.." && pwd)"
DEST="$BENCH_REPO/build/df-drivers"

DF_REPO="$(cd "$BENCH_REPO/../delta-forge" 2>/dev/null && pwd || true)"

DF_ODBC_SO="${DF_ODBC_SO:-${DF_REPO}/delta-forge-odbc/build/libdeltaforgeodbc.so}"
DF_ADBC_SO="${DF_ADBC_SO:-${DF_REPO}/build/adbc-linux-x64/libdeltaforge_adbc.so.1.0.0}"

if [[ ! -f "$DF_ODBC_SO" ]]; then
    echo "ERROR: ODBC driver .so not found at $DF_ODBC_SO" >&2
    echo "  Build it: cd $DF_REPO/delta-forge-odbc && cmake -S . -B build && cmake --build build -j" >&2
    echo "  Or set DF_ODBC_SO=/abs/path/libdeltaforgeodbc.so" >&2
    exit 1
fi
if [[ ! -f "$DF_ADBC_SO" ]]; then
    echo "ERROR: ADBC driver .so not found at $DF_ADBC_SO" >&2
    echo "  Build it from the main delta-forge repo's ADBC build path" >&2
    echo "  Or set DF_ADBC_SO=/abs/path/libdeltaforge_adbc.so.1.0.0" >&2
    exit 1
fi

# Sanity-check that we are not staging Windows DLLs.
if command -v file &>/dev/null; then
    for so in "$DF_ODBC_SO" "$DF_ADBC_SO"; do
        ftype=$(file -b "$so" 2>/dev/null || true)
        if echo "$ftype" | grep -qi "windows\|PE32"; then
            echo "ERROR: $so appears to be a Windows binary ($ftype)" >&2
            echo "  driver-bench containers require Linux ELF .so files." >&2
            exit 2
        fi
    done
fi

mkdir -p "$DEST"
install -m 0644 "$DF_ODBC_SO" "$DEST/libdeltaforgeodbc.so"
install -m 0644 "$DF_ADBC_SO" "$DEST/libdeltaforge_adbc.so"
echo "[stage] staged ODBC: $DEST/libdeltaforgeodbc.so ($(du -sh "$DEST/libdeltaforgeodbc.so" | cut -f1))"
echo "[stage] staged ADBC: $DEST/libdeltaforge_adbc.so ($(du -sh "$DEST/libdeltaforge_adbc.so" | cut -f1))"
echo "[stage] done."
