# Prebuilt DeltaForge benchmark image.
#
# Bakes in the OFFICIAL SIGNED Linux release (the `deltaforge` desktop platform
# AppImage + the `deltaforge-cli`) plus the Python harness, and runs the whole
# benchmark fully HEADLESS and HERMETIC. Nothing is built from source: the
# platform binaries are downloaded from the signed release at image-build time
# and SHA256-verified against the release SHA256SUMS.
#
# Why a container: the desktop platform is single-instance-per-user with a fixed
# per-user catalog, so running it natively on a host that already runs DeltaForge
# fights for :3000/:3031 and the shared catalog. A container gives every run its
# own filesystem, ports, and catalog, so none of that host coupling exists.
#
# Build:
#   docker build -f docker/Dockerfile --build-arg DF_VERSION=1.0.6 -t deltaforge-benchmark .
# Run (license is supplied at runtime, never baked in):
#   docker run --rm -e DELTA_FORGE_LICENSE_KEY=<key> -v "$PWD/results:/results" deltaforge-benchmark
#   docker run --rm -e DELTA_FORGE_LICENSE_KEY=<key> -v "$PWD/results:/results" deltaforge-benchmark --scale 10 --engines df,duckdb

FROM ubuntu:24.04

ARG DF_VERSION=1.0.6
ARG DF_REPO=deltaforge-org/delta-forge
ENV DEBIAN_FRONTEND=noninteractive
ENV DF_VERSION=${DF_VERSION}

# Runtime dependencies:
#  - python3 + venv + pip      : the harness and the DuckDB / Spark comparison engines
#  - xvfb                      : off-screen X display for the desktop platform's window
#  - libgtk-3 / libwebkit2gtk  : the AppImage's embedded WebKitGTK webview needs these
#  - libgl1 / libegl1 / mesa   : software GL so the webview renders without a real GPU
#  - libvulkan1 / mesa-vulkan  : Vulkan loader so the GPU graph workload can run with --gpus
#  - openjdk-17-jre-headless   : Spark needs a JVM
#  - procps / ca-certificates  : process tooling + TLS roots for the download
RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates curl \
      python3 python3-venv python3-pip \
      xvfb libgtk-3-0 libwebkit2gtk-4.1-0 \
      libgl1 libegl1 mesa-utils libvulkan1 mesa-vulkan-drivers \
      openjdk-17-jre-headless procps \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/deltaforge

# Download + SHA256-verify + extract the SIGNED platform AppImage and CLI. The
# AppImage is extracted (--appimage-extract) rather than run via FUSE, so the
# container needs no FUSE device and no privileged mode.
RUN set -eu; \
    base="https://github.com/${DF_REPO}/releases/download/v${DF_VERSION}"; \
    app="deltaforge-${DF_VERSION}-linux-x64.AppImage"; \
    cli="deltaforge-cli-${DF_VERSION}-linux-x64.tar.gz"; \
    curl -fsSL -o "$app" "$base/$app"; \
    curl -fsSL -o "$cli" "$base/$cli"; \
    curl -fsSL -o SHA256SUMS "$base/SHA256SUMS"; \
    sha256sum --ignore-missing -c SHA256SUMS; \
    chmod +x "$app"; \
    "./$app" --appimage-extract >/dev/null; \
    mv squashfs-root platform; \
    mkdir -p cli-extract && tar -xzf "$cli" -C cli-extract; \
    ln -s "$(find /opt/deltaforge/cli-extract -type f -name 'deltaforge-cli' | head -1)" /usr/local/bin/deltaforge-cli; \
    rm -f "$app" "$cli" SHA256SUMS

# The harness lives at /workspace: the read-workload fixtures (_fixtures.py and
# data_gen) use /workspace-anchored paths, which is the benchmark's original
# container convention, so this layout makes every workload resolve naturally.
COPY . /workspace
WORKDIR /workspace

# Normalize CRLF -> LF on the in-container shell scripts. A build context checked
# out on Windows (git autocrlf) would otherwise carry a `\r` that breaks the
# `#!/usr/bin/env bash` shebang ("bash\r: No such file or directory").
RUN sed -i 's/\r$//' docker/entrypoint.sh bench install.sh docker/run.sh 2>/dev/null || true \
    && chmod +x docker/entrypoint.sh bench docker/run.sh 2>/dev/null || true

# Python venv with the comparison engines + reporting extras (best-effort).
RUN python3 -m venv .venv \
    && .venv/bin/pip install --no-cache-dir --upgrade pip \
    && .venv/bin/pip install --no-cache-dir --prefer-binary -r requirements.txt \
    && (.venv/bin/pip install --no-cache-dir --prefer-binary -r requirements-report.txt || true)

# bench_runner writes here when it exists; mount a volume at /results to keep them.
RUN mkdir -p /results

# PostgreSQL's initdb / postgres refuse to run as root, so the embedded database
# (and the platform, which is a normal desktop app) must run as a non-root user.
# Give that user ownership of the writable trees (the harness writes .env,
# .engine, logs, data under /workspace; results under /results). The extracted
# platform under /opt/deltaforge stays root-owned but world-readable/executable.
RUN useradd -m -u 10001 -s /bin/bash bench \
    && chown -R bench:bench /workspace /results /opt/deltaforge \
    && chmod -R a+rX /opt/deltaforge/platform
USER bench
ENV HOME=/home/bench

ENV DF_PLATFORM_BIN=/opt/deltaforge/platform/AppRun \
    DF_CLI_PATH=/usr/local/bin/deltaforge-cli

ENTRYPOINT ["/workspace/docker/entrypoint.sh"]
