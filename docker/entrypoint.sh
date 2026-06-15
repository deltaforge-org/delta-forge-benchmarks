#!/usr/bin/env bash
# Entrypoint for the prebuilt DeltaForge benchmark container.
#
# Composes the .env the launcher reads (injecting the runtime license key),
# then runs the standard Linux launcher `./bench`, which boots the signed
# platform fully headless (Xvfb + DELTA_FORGE_HEADLESS), activates the device,
# runs the harness, and tears the platform down. Any arguments are forwarded
# verbatim to bench_runner.py.
set -euo pipefail
cd /workspace

# Guard a baked x86-64-v3 (AVX2) platform build against a non-AVX2 host. The
# in-container /proc/cpuinfo reflects the host CPU, so this catches the
# incompatibility up front and fails with guidance instead of letting the
# engine die with an opaque SIGILL the moment its embedded compute starts.
if [ "$(cat /opt/deltaforge/CPU_TIER 2>/dev/null || echo generic)" = "v3" ] \
   && ! grep -qw avx2 /proc/cpuinfo 2>/dev/null; then
    cat >&2 <<'MSG'
[bench] This image bakes the x86-64-v3 (AVX2) platform build, but this host's CPU
does not advertise AVX2, so the engine would crash with SIGILL.

  Pull or build the portable (generic) image instead:

    docker build -f docker/Dockerfile --build-arg DF_CPU_TIER=generic -t deltaforge-benchmark .

MSG
    exit 3
fi

if [ -z "${DELTA_FORGE_LICENSE_KEY:-}" ]; then
    cat >&2 <<'MSG'
[bench] DeltaForge needs a license key to run the engine, and this image bundles none.

  Pass one at run time (free, ~1 minute, no credit card, at https://console.deltaforge.org):

    docker run --rm -e DELTA_FORGE_LICENSE_KEY=<your-key> \
      -v "$PWD/results:/results" ghcr.io/deltaforge-org/delta-forge-benchmarks

MSG
    exit 2
fi

# The container filesystem is isolated, so fixed ports and a fixed catalog dir
# are safe: nothing else runs here. `./bench` redirects the platform's XDG dirs
# under /workspace/.engine, forces headless bootstrap against the embedded
# PostgreSQL (on a free port), and auto-activates this device. The dev-box
# host-coupling (port :3000 in use, a shared per-user catalog, the single-
# instance desktop lock) cannot happen inside the container.
cat > .env <<EOF
DF_VERSION=${DF_VERSION:-1.0.6}
DF_PLATFORM_BIN=${DF_PLATFORM_BIN:-/opt/deltaforge/platform/AppRun}
DF_CLI_PATH=${DF_CLI_PATH:-/usr/local/bin/deltaforge-cli}
DF_CONTROL_URL=http://127.0.0.1:3000
DF_USERNAME=admin@deltaforge.local
DF_PASSWORD=Benchmark_Admin1
DELTA_FORGE_LICENSE_KEY=${DELTA_FORGE_LICENSE_KEY}
DELTA_FORGE_ADMIN_EMAIL=admin@deltaforge.local
DELTA_FORGE_ADMIN_PASSWORD=Benchmark_Admin1
DELTA_FORGE_ENGINEER_PASSWORD=Benchmark_Engineer1
DELTA_FORGE_BIND_ADDR=127.0.0.1:3000
EOF

# Three ways to drive the run:
#   1. Interactive menu (pick ONE benchmark to gate the run, engines, scale):
#        docker run -it -e DELTA_FORGE_LICENSE_KEY=<key> ghcr.io/deltaforge-org/delta-forge-benchmarks
#   2. Scripted / gated (run just one benchmark, fully automated):
#        docker run --rm -e DELTA_FORGE_LICENSE_KEY=<key> ghcr.io/.../delta-forge-benchmarks --workloads tpch_read_delta --engines df,duckdb
#   3. Unattended default (no terminal, no args): the self-contained synthetic
#      write headline. bench_runner picks the menu only on a TTY; with no TTY it
#      runs exactly these flags, so plain `docker run` stays fully automated.
if [ "$#" -eq 0 ] && [ ! -t 0 ]; then
    set -- --scale 1 --engines df,spark-default,spark-tuned --workloads synthetic_write_delta
fi

# Always disable the cold-cache purge in the container: it cannot drop OS caches
# without --privileged and a dropcaches sidecar, and it kills the comparison
# engines' own JVMs mid-run (Spark fails with "PythonUtils does not exist in the
# JVM"). bench_runner labels runs purge_verified=False, which the report already
# excludes from cold aggregates.
exec ./bench "$@" --no-purge
