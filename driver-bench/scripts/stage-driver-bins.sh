#!/usr/bin/env bash
# Stage the DeltaForge ODBC + ADBC driver shared objects for driver-bench.
#
# Default: download the official, signed driver releases from deltaforge-org
# (no source build), matching the engine version the parent installer pinned in
# ../.env (DF_VERSION), or the latest if that is unset. Extracts the two .so
# files into build/df-drivers/ where setup-host-stack.sh expects them.
#
# Local-build override (for driver developers): point at your own .so files
#   DF_ODBC_SO=/abs/path/libdeltaforgeodbc.so \
#   DF_ADBC_SO=/abs/path/libdeltaforge_adbc.so \
#       ./scripts/stage-driver-bins.sh
#
# Other knobs:
#   DF_VERSION=1.0.5     pin the driver release version
#   DF_DRIVERS_DIR=...   where the staged .so files land (default build/df-drivers)

set -eu

HERE="$(cd "$(dirname "$0")" && pwd)"
BENCH_REPO="$(cd "$HERE/../.." && pwd)"
DEST="${DF_DRIVERS_DIR:-$BENCH_REPO/build/df-drivers}"
CACHE="$BENCH_REPO/build/df-drivers-cache"

log()  { printf '\033[1;36m[stage] %s\033[0m\n' "$*"; }
fail() { printf '\033[1;31m[stage] ERROR %s\033[0m\n' "$*" >&2; exit 1; }

mkdir -p "$DEST" "$CACHE"

# ---------------------------------------------------------------------------
# Local-build path: stage the operator's own .so files, skip downloading.
# ---------------------------------------------------------------------------
if [ -n "${DF_ODBC_SO:-}" ] || [ -n "${DF_ADBC_SO:-}" ]; then
    [ -f "${DF_ODBC_SO:-}" ] || fail "DF_ODBC_SO not a file: ${DF_ODBC_SO:-<unset>}"
    [ -f "${DF_ADBC_SO:-}" ] || fail "DF_ADBC_SO not a file: ${DF_ADBC_SO:-<unset>}"
    install -m 0644 "$DF_ODBC_SO" "$DEST/libdeltaforgeodbc.so"
    install -m 0644 "$DF_ADBC_SO" "$DEST/libdeltaforge_adbc.so"
    log "staged local ODBC: $DEST/libdeltaforgeodbc.so"
    log "staged local ADBC: $DEST/libdeltaforge_adbc.so"
    exit 0
fi

# ---------------------------------------------------------------------------
# Release path: download the official driver tarballs.
# ---------------------------------------------------------------------------
command -v curl >/dev/null 2>&1 || fail "curl is required"
command -v tar  >/dev/null 2>&1 || fail "tar is required"

# OS/arch -> release platform tag (drivers publish linux-x64 / windows-x64).
case "$(uname -s)" in
    Linux)  [ "$(uname -m)" = "x86_64" ] || fail "released drivers are linux-x64 only (this is $(uname -m))." ;;
    *)      fail "stage-driver-bins.sh runs on Linux x64; on other OSes install the driver package for your platform." ;;
esac
PLAT="linux-x64"

# Version: prefer the parent installer's pin, else the ODBC repo's latest.
VER="${DF_VERSION:-}"
if [ -z "$VER" ] && [ -f "$BENCH_REPO/.env" ]; then
    VER="$(grep -m1 '^DF_VERSION=' "$BENCH_REPO/.env" 2>/dev/null | cut -d= -f2- || true)"
fi
if [ -z "$VER" ]; then
    VER="$(curl -fsSL "https://api.github.com/repos/deltaforge-org/delta-forge-odbc/releases/latest" 2>/dev/null \
        | grep -m1 '"tag_name"' | sed -E 's/.*"tag_name": *"v?([^"]+)".*/\1/')"
    [ -n "$VER" ] || fail "could not resolve a driver version. Set DF_VERSION."
fi
log "driver version: $VER"

# repo, asset name, .so path inside the tarball, staged name
fetch_driver() {
    local repo="$1" asset="$2" so_glob="$3" staged="$4"
    local url="https://github.com/deltaforge-org/${repo}/releases/download/v${VER}/${asset}"
    local tgz="$CACHE/$asset"
    if [ ! -f "$tgz" ]; then
        log "downloading $asset"
        curl -fsSL --retry 3 -o "$tgz" "$url" || fail "download failed: $url"
    fi
    rm -rf "$CACHE/x-$staged"; mkdir -p "$CACHE/x-$staged"
    tar -xzf "$tgz" -C "$CACHE/x-$staged" || fail "could not extract $asset"
    local so
    so="$(find "$CACHE/x-$staged" -type f -name "$so_glob" | head -1)"
    [ -n "$so" ] || fail "$so_glob not found inside $asset"
    install -m 0644 "$so" "$DEST/$staged"
    log "staged $staged ($(du -sh "$DEST/$staged" | cut -f1))"
}

fetch_driver delta-forge-odbc "deltaforge-odbc-${VER}-${PLAT}.tar.gz" "libdeltaforgeodbc.so" "libdeltaforgeodbc.so"
fetch_driver delta-forge-adbc "deltaforge-adbc-${VER}-${PLAT}.tar.gz" "libdeltaforge_adbc.so" "libdeltaforge_adbc.so"
log "done. drivers in $DEST"
