#!/usr/bin/env bash
# Stage locally compiled DeltaForge Linux binaries for a BUILD_MODE=local
# Docker build. Run this from the repo root before `docker build`.
#
# Usage:
#   ./scripts/stage-local-bins.sh [path-to-df-target-dir]
#
# Default target dir: ../delta-forge/target/release  (sibling repo)
# Override:          DF_TARGET=/path/to/df/target/release ./scripts/stage-local-bins.sh
#
# The binaries must be Linux ELF executables (not Windows .exe).
# Build them in WSL or CI before running this script.

set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
REPO="$(cd "$HERE/.." && pwd)"
DEST="$REPO/build/df-bins"

# Allow override via env or positional arg
DF_TARGET="${DF_TARGET:-${1:-$(cd "$REPO/../delta-forge" 2>/dev/null && echo "$(pwd)/target/release" || echo "")}}"

if [[ -z "$DF_TARGET" || ! -d "$DF_TARGET" ]]; then
    echo "ERROR: cannot find DeltaForge target/release dir." >&2
    echo "  Set DF_TARGET=/path/to/df/target/release or pass it as arg 1." >&2
    exit 1
fi

echo "[stage] source: $DF_TARGET"
echo "[stage] dest:   $DEST"

mkdir -p "$DEST"

for bin in delta-forge-cli delta-forge-server delta-forge-worker; do
    src="$DF_TARGET/$bin"
    if [[ ! -f "$src" ]]; then
        # Also try without path (in case target is already the bin dir)
        src="$DF_TARGET/release/$bin"
    fi
    if [[ ! -f "$src" ]]; then
        echo "ERROR: $bin not found under $DF_TARGET" >&2
        echo "  Build the release target first: cargo build --release -p $bin" >&2
        exit 1
    fi
    # Verify it's a Linux ELF binary (file command check, soft)
    if command -v file &>/dev/null; then
        ftype=$(file -b "$src" 2>/dev/null || true)
        if echo "$ftype" | grep -qi "windows\|PE32"; then
            echo "WARN: $bin appears to be a Windows binary ($ftype)" >&2
            echo "  Docker containers require Linux ELF binaries." >&2
        fi
    fi
    cp "$src" "$DEST/$bin"
    echo "[stage] staged $bin ($(du -sh "$DEST/$bin" | cut -f1))"
done

echo "[stage] done. Build with:"
echo "  docker build --build-arg BUILD_MODE=local -t deltaforge/bench:local ."
