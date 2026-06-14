#!/usr/bin/env bash
# DeltaForge benchmark: one-command installer (Linux + macOS).
#
# A public, platform-agnostic proof of DeltaForge performance and correctness.
# This single script takes a fresh machine to a runnable benchmark using ONLY
# official, signed release artifacts from deltaforge-org. Nothing is built from
# source.
#
#   curl -fsSL https://deltaforge.org/bench/install.sh | bash
#
# It installs exactly two DeltaForge artifacts plus the harness around them:
#   1. deltaforge        - the platform (embeds the compute node + control plane)
#   2. deltaforge-cli     - the SQL client the harness drives
#   3. the bench harness   - this repo (cloned if you piped the script in)
#   4. a Python venv       - DuckDB + Spark comparison engines and reporting
#   5. a pinned JRE         - only if Java is absent (Spark needs a JVM)
#
# There is no separate compute node, no Postgres to install, and no Docker
# requirement: the platform bootstraps an embedded database and an embedded
# compute node in-process.
#
# Windows: use install.ps1 (PowerShell) instead.
#
# Every step explains itself, and every failure tells you exactly what to do
# next. If something here fails, that is a bug we want to hear about:
# https://github.com/deltaforge-org/delta-forge-benchmarks/issues
#
# Environment overrides (all optional):
#   DF_VERSION                engine version (default: latest release)
#   BENCH_HOME                where the harness lives (default: ./delta-forge-benchmarks)
#   DELTA_FORGE_LICENSE_KEY   required license key (else you are prompted for one)
#   DF_PREFIX                 where engine binaries land (default: $BENCH_HOME/.engine)
#   SKIP_SPARK=1              do not provision Java / Spark (df + DuckDB only)
#   SKIP_RUN=1                install only; do not offer a smoke run
#   ASSUME_YES=1              non-interactive; skip the post-install smoke prompt

# NOTE: deliberately `set -eu` WITHOUT `pipefail`. Several pipelines here end in
# an early-closing reader (`grep -m1`, `head -1`, `find ... | head -1`); under
# `pipefail` the upstream producer takes SIGPIPE (exit 141) and the whole script
# would abort silently. Every value captured from such a pipeline is validated
# explicitly below (empty -> fail with a message), so dropping pipefail is safe.
set -eu

# ===========================================================================
# Presentation helpers
# ===========================================================================

if [ -t 1 ]; then
    BOLD=$'\033[1m'; DIM=$'\033[2m'; RED=$'\033[1;31m'; GRN=$'\033[1;32m'
    YEL=$'\033[1;33m'; CYN=$'\033[1;36m'; RST=$'\033[0m'
else
    BOLD=""; DIM=""; RED=""; GRN=""; YEL=""; CYN=""; RST=""
fi

STEP_N=0
STEP_TOTAL=9
step() { STEP_N=$((STEP_N + 1)); printf '\n%s[%d/%d] %s%s\n' "$CYN" "$STEP_N" "$STEP_TOTAL" "$*" "$RST"; }
info() { printf '      %s\n' "$*"; }
ok()   { printf '      %s✓%s %s\n' "$GRN" "$RST" "$*"; }
warn() { printf '      %s!%s %s\n' "$YEL" "$RST" "$*" >&2; }

# fail <message> [hint...] : print the error, any actionable hint lines, then exit.
fail() {
    local msg="$1"; shift || true
    printf '\n%s✗ %s%s\n' "$RED" "$msg" "$RST" >&2
    while [ "$#" -gt 0 ]; do printf '  %s→%s %s\n' "$YEL" "$RST" "$1" >&2; shift; done
    printf '\n  %sNeed help? %shttps://github.com/deltaforge-org/delta-forge-benchmarks/issues\n' "$DIM" "$RST" >&2
    exit 1
}

banner() {
    printf '%s\n' "$BOLD"
    cat <<'EOF'
   ____       _ _        _____
  |  _ \  ___| | |_ __ _|  ___|__  _ __ __ _  ___
  | | | |/ _ \ | __/ _` | |_ / _ \| '__/ _` |/ _ \
  | |_| |  __/ | || (_| |  _| (_) | | | (_| |  __/
  |____/ \___|_|\__\__,_|_|  \___/|_|  \__, |\___|
                                       |___/  benchmark
EOF
    printf '%s' "$RST"
    printf '  %sA public proof of DeltaForge performance and correctness.%s\n' "$DIM" "$RST"
    printf '  %sOfficial signed releases only. Nothing built from source.%s\n' "$DIM" "$RST"
}

# ===========================================================================
# Constants
# ===========================================================================

REPO_OWNER="deltaforge-org"
ENGINE_REPO="delta-forge"
BENCH_REPO="delta-forge-benchmarks"
RELEASE_BASE="https://github.com/${REPO_OWNER}/${ENGINE_REPO}/releases"
API_LATEST="https://api.github.com/repos/${REPO_OWNER}/${ENGINE_REPO}/releases/latest"
BENCH_GIT_URL="https://github.com/${REPO_OWNER}/${BENCH_REPO}.git"

# DeltaForge requires a license key to run the engine; the benchmark does NOT
# bundle one. Each user supplies their own (a free key takes a minute at
# console.deltaforge.org). The key is collected in step 9, from the
# DELTA_FORGE_LICENSE_KEY env var or an interactive prompt, and written into .env.
CONSOLE_URL="https://console.deltaforge.org"

# Rough space needed for binaries + venv + SF=1 fixtures.
MIN_DISK_MB=4096
MIN_RAM_MB=8192

banner

# ===========================================================================
# Step 1: Detect this machine
# ===========================================================================

step "Checking your machine"

UNAME_S="$(uname -s)"; UNAME_M="$(uname -m)"
case "$UNAME_S" in
    Linux)  OS="linux";  OS_PRETTY="Linux" ;;
    Darwin) OS="macos";  OS_PRETTY="macOS" ;;
    *)      fail "Unsupported operating system: $UNAME_S" \
                 "This installer supports Linux and macOS." \
                 "On Windows, run install.ps1 in PowerShell instead." ;;
esac
case "$UNAME_M" in
    x86_64|amd64)  ARCH="x64" ;;
    arm64|aarch64) ARCH="arm64" ;;
    *)             fail "Unsupported CPU architecture: $UNAME_M" \
                        "DeltaForge ships x64 and arm64 builds only." ;;
esac
PLAT="${OS}-${ARCH}"

if [ "$OS" = "linux" ] && [ "$ARCH" = "arm64" ]; then
    fail "No published DeltaForge platform build for linux-arm64 (yet)." \
         "Use an x64 Linux host, or macOS on Apple Silicon (arm64 is published there)."
fi
ok "$OS_PRETTY on $ARCH"

# ===========================================================================
# Step 2: Check required tools
# ===========================================================================

step "Checking required tools"

pkg_hint() {
    # Best-effort per-platform install hint for a missing tool.
    case "$OS" in
        macos) echo "Install it with Homebrew:  brew install $1" ;;
        linux)
            if   command -v apt-get >/dev/null 2>&1; then echo "Install it with:  sudo apt-get install -y $1"
            elif command -v dnf     >/dev/null 2>&1; then echo "Install it with:  sudo dnf install -y $1"
            elif command -v pacman  >/dev/null 2>&1; then echo "Install it with:  sudo pacman -S $1"
            elif command -v zypper  >/dev/null 2>&1; then echo "Install it with:  sudo zypper install -y $1"
            else echo "Install '$1' with your distribution's package manager." ; fi ;;
    esac
}

for tool in curl tar python3; do
    if command -v "$tool" >/dev/null 2>&1; then
        ok "found $tool"
    else
        fail "Required tool not found: $tool" "$(pkg_hint "$tool")"
    fi
done

# Python must be new enough for the harness + PySpark 4 (needs 3.9+).
PYV="$(python3 -c 'import sys; print("%d.%d" % sys.version_info[:2])' 2>/dev/null || echo 0.0)"
PYV_OK="$(python3 -c 'import sys; print(1 if sys.version_info[:2] >= (3,9) else 0)' 2>/dev/null || echo 0)"
if [ "$PYV_OK" != "1" ]; then
    fail "Python 3.9 or newer is required (found $PYV)." \
         "$(pkg_hint python3)" \
         "macOS users can also use python.org or 'brew install python@3.12'."
fi
ok "python $PYV"

sha256_of() {
    if command -v sha256sum >/dev/null 2>&1; then sha256sum "$1" | awk '{print $1}'
    else shasum -a 256 "$1" | awk '{print $1}'; fi
}

# ===========================================================================
# Step 3: Check network reachability
# ===========================================================================

step "Checking network access to the official release host"

if curl -fsS --connect-timeout 10 -o /dev/null "https://github.com" 2>/dev/null \
   || curl -fsS --connect-timeout 10 -o /dev/null "$API_LATEST" 2>/dev/null; then
    ok "github.com reachable"
else
    fail "Cannot reach github.com to download the official release artifacts." \
         "This installer must download signed binaries from the DeltaForge release page." \
         "Check your internet connection, VPN, or corporate proxy and try again." \
         "Behind a proxy? Export HTTPS_PROXY=http://host:port before re-running."
fi

# ===========================================================================
# Step 4: Resolve the version + locate the harness
# ===========================================================================

step "Resolving the DeltaForge release to install"

if [ -z "${DF_VERSION:-}" ]; then
    DF_VERSION="$(curl -fsSL "$API_LATEST" 2>/dev/null \
        | grep -m1 '"tag_name"' \
        | sed -E 's/.*"tag_name": *"v?([^"]+)".*/\1/')"
    [ -n "$DF_VERSION" ] || fail "Could not determine the latest release version." \
        "GitHub may be rate-limiting unauthenticated API calls." \
        "Pin a version explicitly:  DF_VERSION=1.0.5 bash install.sh"
fi
ok "engine version $DF_VERSION"
DL_BASE="${RELEASE_BASE}/download/v${DF_VERSION}"

case "$OS" in
    linux) PLATFORM_ASSET="deltaforge-${DF_VERSION}-linux-x64.AppImage" ;;
    macos) PLATFORM_ASSET="deltaforge-${DF_VERSION}-${PLAT}.dmg" ;;
esac
CLI_ASSET="deltaforge-cli-${DF_VERSION}-${PLAT}.tar.gz"

# Locate (or fetch) the bench harness, then anchor every path to it.
if [ -f "bench_runner.py" ] && [ -d "engines" ]; then
    BENCH_HOME="$(pwd)"
    info "using the harness in the current directory"
elif [ -n "${BENCH_HOME:-}" ] && [ -f "${BENCH_HOME}/bench_runner.py" ]; then
    BENCH_HOME="$(cd "$BENCH_HOME" && pwd)"
else
    command -v git >/dev/null 2>&1 || fail "git is required to fetch the bench harness." "$(pkg_hint git)"
    BENCH_HOME="${BENCH_HOME:-$(pwd)/${BENCH_REPO}}"
    if [ ! -d "$BENCH_HOME/.git" ]; then
        info "cloning the bench harness into $BENCH_HOME"
        git clone --depth 1 "$BENCH_GIT_URL" "$BENCH_HOME" >/dev/null 2>&1 \
            || fail "Failed to clone the bench harness from $BENCH_GIT_URL"
    fi
fi
cd "$BENCH_HOME"
ok "bench home: $BENCH_HOME"

DF_PREFIX="${DF_PREFIX:-$BENCH_HOME/.engine}"
BIN_DIR="$DF_PREFIX/bin"; CACHE_DIR="$DF_PREFIX/cache"
mkdir -p "$BIN_DIR" "$CACHE_DIR"

# ===========================================================================
# Step 5: Check disk + memory headroom (warn, do not block)
# ===========================================================================

step "Checking disk and memory headroom"

AVAIL_MB="$(df -Pk "$BENCH_HOME" 2>/dev/null | awk 'NR==2 {print int($4/1024)}')"
if [ -n "${AVAIL_MB:-}" ] && [ "$AVAIL_MB" -lt "$MIN_DISK_MB" ]; then
    warn "only ${AVAIL_MB} MB free here; ~${MIN_DISK_MB} MB recommended for binaries + SF=1 data."
    warn "larger scales need much more (SF=10 ≈ 40 GB). Free up space before a big run."
else
    ok "disk: ${AVAIL_MB:-?} MB free"
fi

if [ "$OS" = "linux" ]; then
    RAM_MB="$(awk '/MemTotal/ {print int($2/1024)}' /proc/meminfo 2>/dev/null || echo "")"
else
    RAM_MB="$(( $(sysctl -n hw.memsize 2>/dev/null || echo 0) / 1048576 ))"
fi
if [ -n "${RAM_MB:-}" ] && [ "$RAM_MB" -gt 0 ] && [ "$RAM_MB" -lt "$MIN_RAM_MB" ]; then
    warn "only ${RAM_MB} MB RAM detected; ${MIN_RAM_MB} MB recommended. Spark engines may struggle. df and DuckDB are fine."
else
    ok "memory: ${RAM_MB:-?} MB"
fi

# Warn early if the control-plane port is already taken (a running DeltaForge,
# or anything else on :3000). The run would otherwise fail confusingly later.
if python3 - 3000 <<'PY' 2>/dev/null
import socket, sys
s = socket.socket()
try:
    s.bind(("127.0.0.1", int(sys.argv[1]))); s.close()
except OSError:
    sys.exit(1)
PY
then
    ok "control-plane port 3000 is free"
else
    warn "port 3000 is already in use. If DeltaForge is already running, stop it before benchmarking,"
    warn "or set DELTA_FORGE_BIND_ADDR / DF_CONTROL_URL to a free port in .env after install."
fi

# ===========================================================================
# Step 6: Download + verify the official artifacts
# ===========================================================================

step "Downloading and verifying official release artifacts"

SUMS_FILE="$CACHE_DIR/SHA256SUMS"
curl -fsSL --retry 3 -o "$SUMS_FILE" "${DL_BASE}/SHA256SUMS" \
    || fail "Could not download SHA256SUMS for v${DF_VERSION}." \
            "Does this version exist? Check ${RELEASE_BASE}" \
            "Pin a known-good version:  DF_VERSION=1.0.5 bash install.sh"
ok "fetched checksum manifest"

# Optional signature check, only when a real public key is in the keyring.
if command -v gpg >/dev/null 2>&1 \
   && curl -fsSL --retry 3 -o "$SUMS_FILE.sig" "${DL_BASE}/SHA256SUMS.sig" 2>/dev/null; then
    if gpg --verify "$SUMS_FILE.sig" "$SUMS_FILE" >/dev/null 2>&1; then
        ok "GPG signature verified"
    else
        info "GPG signature present but no trusted key imported; verifying by SHA-256 over TLS instead."
    fi
fi

download_and_verify() {
    local asset="$1" dest="$CACHE_DIR/$1"
    if [ ! -f "$dest" ]; then
        curl -fsSL --retry 3 -o "$dest" "${DL_BASE}/${asset}" \
            || fail "Download failed: ${asset}" "URL: ${DL_BASE}/${asset}"
    fi
    local want got
    want="$(grep -E "[ *]${asset}\$" "$SUMS_FILE" | awk '{print $1}' | head -1)"
    [ -n "$want" ] || fail "No checksum for ${asset} in SHA256SUMS (release layout changed?)."
    got="$(sha256_of "$dest")"
    [ "$want" = "$got" ] || fail "Checksum mismatch for ${asset}." \
        "expected $want" "got      $got" \
        "Delete $dest and re-run. If it persists, the download is being tampered with or truncated."
    ok "verified ${asset}"
}

info "platform: ${PLATFORM_ASSET}"
download_and_verify "$PLATFORM_ASSET"
info "cli:      ${CLI_ASSET}"
download_and_verify "$CLI_ASSET"

# ===========================================================================
# Step 7: Install the platform + CLI
# ===========================================================================

step "Installing the DeltaForge platform and CLI"

# CLI: a single executable in the tarball.
tar -xzf "$CACHE_DIR/$CLI_ASSET" -C "$CACHE_DIR" \
    || fail "Could not extract ${CLI_ASSET} (corrupt download?)." "Delete $CACHE_DIR/$CLI_ASSET and re-run."
CLI_SRC="$(find "$CACHE_DIR" -maxdepth 2 -type f -name 'deltaforge-cli' | head -1)"
[ -n "$CLI_SRC" ] || fail "deltaforge-cli not found inside ${CLI_ASSET}."
install -m 0755 "$CLI_SRC" "$BIN_DIR/deltaforge-cli"
DF_CLI_PATH="$BIN_DIR/deltaforge-cli"
ok "deltaforge-cli installed"

# Platform.
case "$OS" in
    linux)
        install -m 0755 "$CACHE_DIR/$PLATFORM_ASSET" "$DF_PREFIX/deltaforge.AppImage"
        # Extract rather than run the AppImage in place: extraction never needs
        # FUSE (absent on many servers/CI), and AppRun then launches without it.
        # This is the reliable path across the widest range of machines.
        info "extracting the platform AppImage (runs without FUSE)"
        ( cd "$DF_PREFIX" && rm -rf squashfs-root && ./deltaforge.AppImage --appimage-extract >/dev/null 2>&1 ) \
            || fail "Could not extract the platform AppImage." \
                    "The download may be corrupt: delete $DF_PREFIX/deltaforge.AppImage and re-run." \
                    "If extraction is blocked, install FUSE and retry:  $(pkg_hint fuse)"
        DF_PLATFORM_BIN="$DF_PREFIX/squashfs-root/AppRun"
        [ -x "$DF_PLATFORM_BIN" ] || fail "AppRun not found after extracting ${PLATFORM_ASSET}."
        ok "platform installed"
        # On a headless Linux box the platform (a desktop app) needs a virtual
        # display at run time. Tell the user now, not at first run.
        if [ -z "${DISPLAY:-}" ] && ! command -v xvfb-run >/dev/null 2>&1; then
            warn "no graphical display detected. On a headless host the launcher needs xvfb."
            warn "install it ahead of time:  $(pkg_hint xvfb)"
        fi
        ;;
    macos)
        MNT="$(mktemp -d)"
        hdiutil attach -nobrowse -quiet -mountpoint "$MNT" "$CACHE_DIR/$PLATFORM_ASSET" \
            || fail "Could not mount ${PLATFORM_ASSET}." "The .dmg download may be corrupt; delete it and re-run."
        APP="$(find "$MNT" -maxdepth 1 -name '*.app' | head -1)"
        if [ -z "$APP" ]; then hdiutil detach -quiet "$MNT" || true; fail "No .app found inside ${PLATFORM_ASSET}."; fi
        rm -rf "$DF_PREFIX/DeltaForge.app"
        cp -R "$APP" "$DF_PREFIX/DeltaForge.app"
        hdiutil detach -quiet "$MNT" || true
        DF_PLATFORM_BIN="$DF_PREFIX/DeltaForge.app/Contents/MacOS/deltaforge"
        [ -x "$DF_PLATFORM_BIN" ] || fail "Platform binary missing at ${DF_PLATFORM_BIN}."
        # Strip the quarantine flag so first launch is not blocked by Gatekeeper.
        xattr -dr com.apple.quarantine "$DF_PREFIX/DeltaForge.app" 2>/dev/null || true
        ok "platform installed (.app)"
        ;;
esac

# ===========================================================================
# Step 8: Python harness + (optional) Java for Spark
# ===========================================================================

step "Setting up the Python harness and comparison engines"

VENV_DIR="$BENCH_HOME/.venv"
[ -x "$VENV_DIR/bin/python" ] || python3 -m venv "$VENV_DIR" \
    || fail "Could not create a Python virtual environment." \
            "On Debian/Ubuntu the venv module is a separate package:  sudo apt-get install -y python3-venv"
"$VENV_DIR/bin/pip" install --quiet --upgrade pip >/dev/null 2>&1 || true
# --prefer-binary so pip never silently source-builds a C-extension package when
# a wheel exists for this Python (the floors in requirements.txt guarantee one).
info "installing core engine dependencies (this can take a minute)"
"$VENV_DIR/bin/pip" install --quiet --prefer-binary -r "$BENCH_HOME/requirements.txt" \
    || fail "Core Python dependency install failed." \
            "Inspect:  $VENV_DIR/bin/pip install --prefer-binary -r requirements.txt" \
            "If a package has no wheel for Python $PYV, try a Python in the 3.9-3.13 range."
ok "core engine dependencies ready (DuckDB + Spark)"

# Reporting / plotting extras are optional: nothing in the run path imports them.
# Install best-effort so a missing wheel on a brand-new Python never blocks a run.
if [ -f "$BENCH_HOME/requirements-report.txt" ]; then
    if "$VENV_DIR/bin/pip" install --quiet --prefer-binary -r "$BENCH_HOME/requirements-report.txt" >/dev/null 2>&1; then
        ok "reporting extras ready (pandas + charts)"
    else
        warn "reporting extras have no wheel for Python $PYV; skipping them. Results still generate."
    fi
fi

JAVA_HOME_RESOLVED=""
if [ "${SKIP_SPARK:-0}" = "1" ]; then
    info "SKIP_SPARK=1: skipping Java; df and DuckDB will run, Spark engines will not."
elif command -v java >/dev/null 2>&1; then
    JAVA_HOME_RESOLVED="$(dirname "$(dirname "$(command -v java)")")"
    ok "found system Java: $JAVA_HOME_RESOLVED"
else
    info "no Java found; fetching a pinned Temurin 17 JRE so Spark works out of the box"
    case "$OS" in linux) AOS="linux";; macos) AOS="mac";; esac
    case "$ARCH" in x64) AARCH="x64";; arm64) AARCH="aarch64";; esac
    JRE_URL="https://api.adoptium.net/v3/binary/latest/17/ga/${AOS}/${AARCH}/jre/hotspot/normal/eclipse"
    if curl -fsSL --retry 3 -o "$CACHE_DIR/jre.tgz" "$JRE_URL" 2>/dev/null; then
        rm -rf "$DF_PREFIX/jre"; mkdir -p "$DF_PREFIX/jre"
        tar -xzf "$CACHE_DIR/jre.tgz" -C "$DF_PREFIX/jre" --strip-components=1 2>/dev/null \
            || tar -xzf "$CACHE_DIR/jre.tgz" -C "$DF_PREFIX/jre"
        if   [ -x "$DF_PREFIX/jre/Contents/Home/bin/java" ]; then JAVA_HOME_RESOLVED="$DF_PREFIX/jre/Contents/Home"
        elif [ -x "$DF_PREFIX/jre/bin/java" ];               then JAVA_HOME_RESOLVED="$DF_PREFIX/jre"
        else JAVA_HOME_RESOLVED="$(dirname "$(dirname "$(find "$DF_PREFIX/jre" -name java -type f | head -1)")")"; fi
        ok "Java ready (bundled Temurin 17): $JAVA_HOME_RESOLVED"
    else
        warn "could not download a JRE. Spark engines will be unavailable until you install Java 17."
        warn "df and DuckDB still run. To add Spark later, install a JDK 17 and re-run install.sh."
    fi
fi

# ===========================================================================
# Step 9: Choose a license, write .env
# ===========================================================================

step "Writing configuration"

# DeltaForge needs a license key to run the engine, and the benchmark ships
# without one: every user brings their own. Resolve it in priority order:
#   1. the DELTA_FORGE_LICENSE_KEY env var (also covers non-interactive installs)
#   2. an interactive prompt (when run from a terminal)
# With neither, stop with guidance rather than install a benchmark that cannot
# execute a single query. The key is written verbatim into .env; the platform's
# own license validator is the authority on whether it is accepted.
if [ -n "${DELTA_FORGE_LICENSE_KEY:-}" ]; then
    LICENSE_KEY="$DELTA_FORGE_LICENSE_KEY"
    ok "license: using the key from DELTA_FORGE_LICENSE_KEY"
elif [ -t 0 ]; then
    printf '\n      %sDeltaForge needs a license key to run the engine.%s It is free:\n' "$BOLD" "$RST"
    printf '      sign in at %s%s%s, create a key, and paste it below.\n\n' "$CYN" "$CONSOLE_URL" "$RST"
    printf '      License key: '
    read -r LICENSE_KEY || LICENSE_KEY=""
    LICENSE_KEY="$(printf '%s' "$LICENSE_KEY" | tr -d '[:space:]')"
    [ -n "$LICENSE_KEY" ] || fail "No license key entered." \
        "DeltaForge cannot run the engine without a license key." \
        "Get a free key at ${CONSOLE_URL}, then re-run ./install.sh and paste it" \
        "(or run non-interactively: DELTA_FORGE_LICENSE_KEY=<key> ./install.sh)."
    ok "license: using the key you entered"
else
    fail "No license key provided." \
        "DeltaForge needs a license key to run the engine; the benchmark does not bundle one." \
        "This is a non-interactive install and DELTA_FORGE_LICENSE_KEY is unset." \
        "Get a free key at ${CONSOLE_URL}, then: DELTA_FORGE_LICENSE_KEY=<key> ./install.sh"
fi

ENV_FILE="$BENCH_HOME/.env"
{
    echo "# Generated by install.sh on $(uname -srm). Re-run install.sh to refresh."
    echo "DF_VERSION=${DF_VERSION}"
    echo "DF_PLATFORM_BIN=${DF_PLATFORM_BIN}"
    echo "DF_CLI_PATH=${DF_CLI_PATH}"
    echo "DF_CONTROL_URL=http://127.0.0.1:3000"
    echo "DF_USERNAME=admin@deltaforge.local"
    echo "DF_PASSWORD=Benchmark_Admin1"
    echo
    echo "# First-run bootstrap contract (read by the embedded platform)."
    echo "DELTA_FORGE_LICENSE_KEY=${LICENSE_KEY}"
    echo "DELTA_FORGE_ADMIN_EMAIL=admin@deltaforge.local"
    echo "DELTA_FORGE_ADMIN_PASSWORD=Benchmark_Admin1"
    echo "DELTA_FORGE_ENGINEER_PASSWORD=Benchmark_Engineer1"
    echo "DELTA_FORGE_CONFIG_DIR=${DF_PREFIX}/dfconfig"
    echo "DELTA_FORGE_BIND_ADDR=127.0.0.1:3000"
    [ -n "$JAVA_HOME_RESOLVED" ] && echo "JAVA_HOME=${JAVA_HOME_RESOLVED}"
} > "$ENV_FILE"
# Lock the file down: it holds the license key + admin password in plaintext.
# Non-fatal (a perms hiccup must not sink an otherwise-good install).
chmod 600 "$ENV_FILE" 2>/dev/null || warn "could not chmod 600 $ENV_FILE; it holds the license key + admin password, tighten it manually."
ok "wrote $ENV_FILE (mode 600: holds the license key + admin password)"

# ===========================================================================
# Done
# ===========================================================================

printf '\n%s  Setup complete. DeltaForge is ready to prove itself.%s\n\n' "$GRN$BOLD" "$RST"
printf '  %sWhat you have:%s\n' "$BOLD" "$RST"
printf '    platform : %s\n' "$DF_PLATFORM_BIN"
printf '    cli      : %s\n' "$DF_CLI_PATH"
printf '    config   : %s\n' "$ENV_FILE"
printf '\n  %sRun the benchmark:%s\n' "$BOLD" "$RST"
printf '    cd %s\n' "$BENCH_HOME"
printf '    ./bench                 %s# SF=1 smoke across all engines (a few minutes)%s\n' "$DIM" "$RST"
printf '    ./bench --scale 10      %s# the standard headline tier%s\n' "$DIM" "$RST"
printf '\n  %sEverything is configured in .env; edit it to change the license, ports, or version.%s\n' "$DIM" "$RST"

if [ "${SKIP_RUN:-0}" != "1" ] && [ "${ASSUME_YES:-0}" != "1" ] && [ -t 0 ]; then
    printf '\n%s  Run a quick SF=1 smoke now to confirm everything works? [Y/n] %s' "$CYN" "$RST"
    read -r ans || ans=""
    case "$ans" in
        n|N) info "skipped. Run ./bench whenever you are ready." ;;
        *)   exec "$BENCH_HOME/bench" ;;
    esac
fi
