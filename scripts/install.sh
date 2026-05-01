#!/usr/bin/env bash
# Idempotent setup for a fresh Linux server.
#
# What this installs:
#   - System: openjdk-17-jdk, python3-venv, python3-pip, build-essential, jq
#   - Python venv at .venv/ with pinned deps from requirements.txt
#
# What it does NOT install:
#   - Postgres (only the DeltaForge engine path needs it; install separately
#     if/when you wire DF in).
#   - DeltaForge engine binaries themselves.
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
    fail "this script targets Debian/Ubuntu (apt-get not found). For RHEL/CentOS adapt the pkg list manually."
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

PKGS=(
    openjdk-17-jdk-headless
    python3-venv
    python3-pip
    python3-dev
    build-essential
    curl
    jq
    procps
    psmisc
)

# Filter to packages not already installed for speed + idempotency.
NEED=()
for p in "${PKGS[@]}"; do
    if ! dpkg -s "$p" >/dev/null 2>&1; then
        NEED+=("$p")
    fi
done
if [ ${#NEED[@]} -gt 0 ]; then
    apt_install "${NEED[@]}"
else
    log "all system packages already present"
fi

# ----- 3. JAVA_HOME ----------------------------------------------------------

# PySpark 4.0 requires JDK 17. Resolve via update-alternatives or the
# standard Debian path. We export it from this script so subsequent
# scripts (run_bench.sh) inherit it.

if [ -z "${JAVA_HOME:-}" ]; then
    if [ -d /usr/lib/jvm/java-17-openjdk-amd64 ]; then
        export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
    elif command -v java >/dev/null 2>&1; then
        # Resolve via the binary's path. /usr/lib/jvm/java-17-* on Debian.
        JAVA_BIN=$(readlink -f "$(command -v java)")
        export JAVA_HOME="$(dirname "$(dirname "$JAVA_BIN")")"
    fi
fi
if [ -z "${JAVA_HOME:-}" ] || [ ! -x "$JAVA_HOME/bin/java" ]; then
    fail "JAVA_HOME could not be resolved after installing openjdk-17. Check apt logs."
fi
log "JAVA_HOME=$JAVA_HOME"
"$JAVA_HOME/bin/java" -version 2>&1 | sed 's/^/[install]   /'

# Persist JAVA_HOME for run_bench.sh and friends. Written to .env so
# operators can inspect / modify without re-running install.sh.
ENV_FILE="$REPO_ROOT/.env"
if [ -f "$ENV_FILE" ] && grep -q "^JAVA_HOME=" "$ENV_FILE"; then
    sed -i "s|^JAVA_HOME=.*|JAVA_HOME=$JAVA_HOME|" "$ENV_FILE"
else
    echo "JAVA_HOME=$JAVA_HOME" >> "$ENV_FILE"
fi

# ----- 4. Python venv --------------------------------------------------------

VENV_DIR="$REPO_ROOT/.venv"
if [ ! -x "$VENV_DIR/bin/python" ]; then
    log "creating venv at $VENV_DIR"
    python3 -m venv "$VENV_DIR"
fi

log "upgrading pip"
"$VENV_DIR/bin/pip" install --upgrade --quiet pip

log "installing pinned requirements"
"$VENV_DIR/bin/pip" install --quiet -r "$REPO_ROOT/requirements.txt"

# ----- 5. sanity ------------------------------------------------------------

log "host facts:"
"$VENV_DIR/bin/python" -m engines.host_facts --short || \
    warn "host_facts module didn't run; check Python deps."

# Print the activation command for an interactive operator.
cat <<EOF

[install] done. To run the bench:

    source $VENV_DIR/bin/activate
    ./scripts/run_smoke.sh                 # ~10 min, SF=1, spark-default, tpch_read
    ./scripts/run_bench.sh                 # ~3-6 h,  SF=10, both spark engines, all workloads

JAVA_HOME has been written to .env; scripts source it automatically.
EOF
