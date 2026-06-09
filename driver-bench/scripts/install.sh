#!/usr/bin/env bash
# driver-bench: idempotent host setup for the BI driver bench.
#
# What this installs:
#   - System: unixodbc, unixodbc-dev, cmake, build-essential, pkg-config,
#     curl, jq, .NET SDK 8 (apt-installed where available, dotnet-install.sh
#     as fallback).
#   - Persists DOTNET_ROOT, DOTNET_SDK_VERSION, ODBC_INC, ODBC_LIB to .env
#     so run_smoke.sh / run_bench.sh inherit them.
#
# What it does NOT install:
#   - Postgres. The driver-bench can either spin up the embedded postgres
#     that ships with DeltaForge (the default, used by setup-host-stack.sh)
#     or assume the operator pointed --control-url at a running stack.
#     Either way, install.sh does not touch postgres.
#   - DeltaForge engine binaries (server, worker, cli). Use
#     scripts/stage-driver-bins.sh to stage the ODBC + ADBC .so files and
#     ../scripts/stage-local-bins.sh (the parent benchmarks repo's helper)
#     to stage the engine binaries.
#
# Tested distros: Ubuntu 22.04 / 24.04, Debian 12.
#
# Re-runnable: every step checks "already done" before touching anything,
# so calling install.sh twice is a no-op the second time.

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

# ----- helpers ---------------------------------------------------------------

log()  { printf "\033[1;36m[install] %s\033[0m\n" "$*"; }
warn() { printf "\033[1;33m[install] WARN %s\033[0m\n" "$*" >&2; }
fail() { printf "\033[1;31m[install] ERROR %s\033[0m\n" "$*" >&2; exit 1; }

need_sudo() {
    if [ "$(id -u)" -eq 0 ]; then echo ""; else echo "sudo"; fi
}

apt_install() {
    local sudo_cmd
    sudo_cmd=$(need_sudo)
    log "apt install $*"
    DEBIAN_FRONTEND=noninteractive $sudo_cmd apt-get install -y --no-install-recommends "$@"
}

# ----- 1. distro check -------------------------------------------------------

if ! command -v apt-get >/dev/null 2>&1; then
    fail "this script targets Debian/Ubuntu (apt-get not found). Adapt the pkg list for RHEL/Fedora manually."
fi
if [ -r /etc/os-release ]; then
    . /etc/os-release
    log "detected: ${PRETTY_NAME:-$ID $VERSION_ID}"
fi

# ----- 2. system packages ----------------------------------------------------

# Refresh apt cache once. Skip if recent (24h).
APT_CACHE_AGE=$(stat -c %Y /var/lib/apt/lists/ 2>/dev/null || echo 0)
NOW=$(date +%s)
if [ "$((NOW - APT_CACHE_AGE))" -gt 86400 ]; then
    log "apt-get update (cache > 24h old)"
    $(need_sudo) apt-get update
fi

# The bench harness needs:
#   unixodbc / unixodbc-dev / odbcinst -- driver manager + sql.h headers
#   cmake / build-essential / pkg-config -- to compile driver_bench
#   curl -- to drive the control plane's HTTP API during stack setup
#   jq -- to parse JSON responses in the wrapper scripts
#   psmisc -- pkill / killall used by teardown scripts
PKGS=(
    unixodbc
    unixodbc-dev
    odbcinst
    cmake
    build-essential
    pkg-config
    curl
    jq
    procps
    psmisc
)
NEED=()
for p in "${PKGS[@]}"; do
    if ! dpkg -s "$p" >/dev/null 2>&1; then
        NEED+=("$p")
    fi
done
if [ ${#NEED[@]} -gt 0 ]; then
    apt_install "${NEED[@]}"
else
    log "all base system packages already present"
fi

# ----- 3. .NET SDK 8 ---------------------------------------------------------

# Required for the dotnet/ sub-bench (System.Data.Odbc + Apache.Arrow.Adbc).
# Ubuntu / Debian ship dotnet-sdk-8.0 in apt. On older distros fall back to
# the Microsoft-provided dotnet-install.sh script.

if dpkg -s dotnet-sdk-8.0 >/dev/null 2>&1; then
    log ".NET SDK 8 already present via apt"
elif command -v dotnet >/dev/null 2>&1 && dotnet --list-sdks 2>/dev/null | grep -q '^8\.'; then
    log ".NET SDK 8 already present at $(command -v dotnet)"
else
    if apt-cache show dotnet-sdk-8.0 >/dev/null 2>&1; then
        apt_install dotnet-sdk-8.0
    else
        log "dotnet-sdk-8.0 not in apt; falling back to dotnet-install.sh"
        DOTNET_DIR="$REPO_ROOT/.dotnet"
        mkdir -p "$DOTNET_DIR"
        if [ ! -x "$DOTNET_DIR/dotnet" ]; then
            curl -fsSL https://dot.net/v1/dotnet-install.sh -o "$DOTNET_DIR/dotnet-install.sh"
            chmod +x "$DOTNET_DIR/dotnet-install.sh"
            "$DOTNET_DIR/dotnet-install.sh" --channel 8.0 --install-dir "$DOTNET_DIR"
        fi
        export DOTNET_ROOT="$DOTNET_DIR"
        export PATH="$DOTNET_DIR:$PATH"
    fi
fi

DOTNET_BIN="$(command -v dotnet || true)"
if [ -z "$DOTNET_BIN" ]; then
    fail "dotnet not on PATH after install attempts"
fi
DOTNET_SDK_VERSION="$($DOTNET_BIN --list-sdks | awk '$1 ~ /^8\./ {print $1; exit}')"
[ -z "$DOTNET_SDK_VERSION" ] && fail ".NET SDK 8.x not visible to $DOTNET_BIN"
log ".NET SDK $DOTNET_SDK_VERSION at $DOTNET_BIN"

# Resolve DOTNET_ROOT for the .env contract. When apt-installed the path is
# /usr/share/dotnet (or /usr/lib/dotnet) and may not be in env. Probe.
if [ -z "${DOTNET_ROOT:-}" ]; then
    for cand in /usr/share/dotnet /usr/lib/dotnet "$REPO_ROOT/.dotnet"; do
        if [ -x "$cand/dotnet" ]; then DOTNET_ROOT="$cand"; break; fi
    done
fi

# ----- 4. ODBC discovery -----------------------------------------------------

# Resolve the unixODBC include + lib paths so the bench's CMakeLists can
# find them deterministically across distros (Debian/Ubuntu install
# x86_64-linux-gnu-multiarched, Fedora installs lib64, etc.).

ODBC_INC=""
for c in /usr/include /usr/local/include; do
    [ -f "$c/sql.h" ] && ODBC_INC="$c" && break
done
[ -z "$ODBC_INC" ] && fail "sql.h not found after installing unixodbc-dev"

ODBC_LIB=""
for c in /usr/lib/x86_64-linux-gnu /usr/lib /usr/local/lib /usr/lib64; do
    [ -f "$c/libodbc.so" ] || [ -f "$c/libodbc.so.2" ] && ODBC_LIB="$c" && break
done
[ -z "$ODBC_LIB" ] && fail "libodbc.so not found after installing unixodbc"

log "ODBC: include=$ODBC_INC lib=$ODBC_LIB"

# ----- 5. .env contract ------------------------------------------------------

# Persist resolved paths so the bench scripts pick them up without re-probing.
# The .env file is read by run_smoke.sh / run_bench.sh / setup-host-stack.sh.
ENV_FILE="$REPO_ROOT/.env"
touch "$ENV_FILE"
upsert_env() {
    local k="$1" v="$2"
    if grep -q "^${k}=" "$ENV_FILE"; then
        sed -i "s|^${k}=.*|${k}=${v}|" "$ENV_FILE"
    else
        echo "${k}=${v}" >> "$ENV_FILE"
    fi
}
upsert_env DOTNET_ROOT "${DOTNET_ROOT:-}"
upsert_env DOTNET_SDK_VERSION "$DOTNET_SDK_VERSION"
upsert_env ODBC_INC "$ODBC_INC"
upsert_env ODBC_LIB "$ODBC_LIB"
upsert_env DRIVER_BENCH_HOME "$REPO_ROOT"

log "host facts:"
printf "  dotnet     : %s (%s)\n" "$DOTNET_BIN" "$DOTNET_SDK_VERSION"
printf "  cmake      : %s\n" "$(cmake --version | head -1)"
printf "  gcc        : %s\n" "$(gcc --version | head -1)"
printf "  unixODBC   : %s\n" "$($ODBC_LIB/libodbc.so 2>&1 | head -1 || isql --version 2>&1 | head -1 || echo 'unknown')"

# ----- 6. closing notes ------------------------------------------------------

cat <<EOF

[install] done. Next steps:

  # Stage the engine binaries (delta-forge-cli + server + worker) into the
  # parent benchmarks repo's build/df-bins/. Either build them locally with
  # cargo and run scripts/stage-local-bins.sh from the parent repo, or
  # download the release artefacts from:
  #   https://github.com/deltaforge-org/delta-forge/releases
  #
  # Stage the ODBC + ADBC drivers (the bench's subjects-under-test):
  ./scripts/stage-driver-bins.sh

  # Export your license key (free, no credit card):
  #   https://console.deltaforge.org
  export DELTA_FORGE_LICENSE_KEY=dfk_...

  # Smoke (~30s, 100k rows):
  ./scripts/run_smoke.sh

  # Full bench (~2-5 min, 1M rows, both C++ and .NET, all driver modes):
  ./scripts/run_bench.sh

Resolved paths have been written to .env; run_*.sh source it automatically.
EOF
