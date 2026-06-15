#!/usr/bin/env bash
# One command to run the DeltaForge benchmark in its prebuilt container.
#
# Ensures Docker is INSTALLED (installs it if missing) and RUNNING (starts the
# daemon if stopped), then pulls the prebuilt image and runs the benchmark.
# Linux host; the image is linux/amd64.
#
#   DELTA_FORGE_LICENSE_KEY=<key> ./docker/run.sh
#   DELTA_FORGE_LICENSE_KEY=<key> ./docker/run.sh --scale 10 --engines df,duckdb --workloads tpch_read_delta
#
# A free license key takes a minute at https://console.deltaforge.org. Results
# land in ./results on the host.
set -euo pipefail

IMAGE="${BENCH_IMAGE:-ghcr.io/deltaforge-org/delta-forge-benchmarks:latest}"
RESULTS="${BENCH_RESULTS:-$PWD/results}"

log() { printf '\033[1;36m[run]\033[0m %s\n' "$*"; }
die() { printf '\033[1;31m[run] %s\033[0m\n' "$*" >&2; exit 1; }

# 1. Ensure Docker is installed.
if ! command -v docker >/dev/null 2>&1; then
    log "Docker not found; installing via https://get.docker.com (needs sudo)"
    if command -v curl >/dev/null 2>&1; then
        curl -fsSL https://get.docker.com | sh
    elif command -v wget >/dev/null 2>&1; then
        wget -qO- https://get.docker.com | sh
    else
        die "Need curl or wget to install Docker. Install it manually: https://docs.docker.com/engine/install/"
    fi
    command -v docker >/dev/null 2>&1 || die "Docker install did not complete; install it manually and re-run."
fi

# 2. Ensure the daemon is running.
if ! docker info >/dev/null 2>&1; then
    log "starting the Docker daemon"
    if command -v systemctl >/dev/null 2>&1; then
        sudo systemctl start docker 2>/dev/null || true
    elif command -v service >/dev/null 2>&1; then
        sudo service docker start 2>/dev/null || true
    fi
    for _ in $(seq 1 30); do docker info >/dev/null 2>&1 && break; sleep 2; done
    docker info >/dev/null 2>&1 || die "Docker daemon not reachable. Start Docker and re-run (rootless/non-systemd hosts may need 'sudo dockerd &')."
fi
log "Docker ready: $(docker version --format '{{.Server.Version}}' 2>/dev/null)"

# 3. License (the image bundles none).
[ -n "${DELTA_FORGE_LICENSE_KEY:-}" ] || die "Set DELTA_FORGE_LICENSE_KEY=<key> (free at https://console.deltaforge.org)."

# 4. Pull the prebuilt image and run. Add --gpus all (NVIDIA Container Toolkit)
#    via BENCH_DOCKER_ARGS for the GPU graph workload.
log "pulling $IMAGE"
docker pull "$IMAGE"
mkdir -p "$RESULTS"
log "running the benchmark (results -> $RESULTS)"
exec docker run --rm \
    ${BENCH_DOCKER_ARGS:-} \
    -e DELTA_FORGE_LICENSE_KEY \
    -v "$RESULTS:/results" \
    "$IMAGE" "$@"
