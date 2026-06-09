# delta-forge-benchmarks: single image bundling both engines + Postgres.
#
# Layers (top to bottom):
#   - Eclipse Temurin JDK 17 (Spark 4.0 requirement)
#   - Python 3.11 + pip
#   - PostgreSQL 15 (catalog DB for DeltaForge headless bootstrap)
#   - PySpark 4.0.0 + delta-spark 4.0.0 (pinned via build args)
#   - DuckDB Python (TPC-H data via the duckdb tpch extension)
#   - DeltaForge binaries: delta-forge-cli, delta-forge-server, delta-forge-compute
#     (consumed from ./build/df-bins/, see README "Building the bench image")
#
# This image is the unit reviewers reproduce. Pinning every version explicitly
# is a non-negotiable trust signal.
#
# Why apt Postgres rather than embedded-pg: DeltaForge's bootstrap supports
# both DbChoice::Embedded (desktop) and DbChoice::External (cloud / container).
# In a container the External path is the documented one (see
# delta-forge/docs/design/bootstrap.md "Headless / Cloud"). Apt-installed
# Postgres pins the version trivially, removes the first-run extraction step,
# and gives a single pg lifecycle the entrypoint script controls.

ARG JDK_VERSION=17
ARG PYSPARK_VERSION=4.0.0
ARG DELTA_SPARK_VERSION=4.0.0
ARG DUCKDB_VERSION=1.1.3
ARG POSTGRES_MAJOR=15

# ---------------------------------------------------------------------------
# BUILD_MODE controls how DeltaForge binaries are acquired.
#
#   release (default) -- download + GPG-verify from deltaforge-org GitHub
#                        releases at DF_VERSION. Used for published images.
#
#   local             -- copy pre-staged Linux ELF binaries from ./build/df-bins/
#                        (run scripts/stage-local-bins.sh first).
#                        For dev/testing before official packages land.
#
# When official packages exist, the Dockerfile gains an `apt` path and
# BUILD_MODE becomes obsolete — `apt install delta-forge` replaces everything.
# ---------------------------------------------------------------------------
ARG BUILD_MODE=release

# Engine version pulled from deltaforge-org releases. Required for BUILD_MODE=release.
ARG DF_VERSION
# Engine commit SHA, recorded in image labels and manifest.json.
ARG DF_GIT_SHA=unset
# Released-artifact URL prefix.
ARG DF_RELEASE_URL=https://github.com/deltaforge-org/delta-forge/releases/download
# GPG fingerprint expected on every released tarball's .sig.
ARG DF_GPG_FINGERPRINT=
# GPG public key bundled into the image.
ARG DF_GPG_PUBKEY_PATH=docker/deltaforge-release-key.asc

FROM eclipse-temurin:${JDK_VERSION}-jdk-jammy AS runtime

ARG PYSPARK_VERSION
ARG DELTA_SPARK_VERSION
ARG DUCKDB_VERSION
ARG POSTGRES_MAJOR
ARG BUILD_MODE
ARG DF_VERSION
ARG DF_GIT_SHA=unset
ARG DF_RELEASE_URL
ARG DF_GPG_FINGERPRINT
ARG DF_GPG_PUBKEY_PATH

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    DEBIAN_FRONTEND=noninteractive \
    DF_GIT_SHA=${DF_GIT_SHA} \
    POSTGRES_MAJOR=${POSTGRES_MAJOR} \
    PGDATA=/var/lib/postgresql/data \
    PGPORT=5432 \
    PGUSER=deltaforge \
    PGDATABASE=deltaforge

# Postgres 15 from the official PGDG repository so the version is pinned
# regardless of which Ubuntu base layer is in use. `gpg` is also installed
# here because the bench image GPG-verifies every DeltaForge release tarball
# it pulls from deltaforge-org against the bundled release public key.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       ca-certificates curl wget gnupg lsb-release \
    && install -d /usr/share/postgresql-common/pgdg \
    && curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc \
         -o /usr/share/postgresql-common/pgdg/apt.postgresql.org.asc \
    && echo "deb [signed-by=/usr/share/postgresql-common/pgdg/apt.postgresql.org.asc] \
       https://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" \
       > /etc/apt/sources.list.d/pgdg.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends \
       python3.11 python3.11-venv python3-pip \
       postgresql-${POSTGRES_MAJOR} \
       postgresql-client-${POSTGRES_MAJOR} \
       unzip jq procps psmisc \
       socat \
    && ln -sf /usr/bin/python3.11 /usr/local/bin/python \
    && ln -sf /usr/bin/python3.11 /usr/local/bin/python3 \
    && rm -rf /var/lib/apt/lists/*

# Postgres lives under /var/lib/postgresql, owned by the postgres user the
# package created. The entrypoint runs initdb + pg_ctl as that user. Keep the
# data path empty in the image so a fresh `docker run` always gets a clean
# catalog DB (the bench wants every run to start from a known schema state).
RUN mkdir -p ${PGDATA} \
    && chown postgres:postgres ${PGDATA} \
    && chmod 0700 ${PGDATA}

# Pinned Python deps. Versions echoed in `python -m pip list` for the manifest.
RUN python -m pip install --upgrade pip \
    && python -m pip install \
        "pyspark==${PYSPARK_VERSION}" \
        "delta-spark==${DELTA_SPARK_VERSION}" \
        "duckdb==${DUCKDB_VERSION}" \
        "deltalake==0.22.3" \
        "psutil==6.1.0" \
        "matplotlib==3.9.2" \
        "plotly==5.24.1" \
        "pandas==2.2.3" \
        "Jinja2==3.1.4"

# =============================================================================
# DeltaForge engine binaries
# =============================================================================
#
# Two acquisition paths, both deterministic:
#
#   1. delta-forge-cli + delta-forge-compute
#      Downloaded from the deltaforge-org public release at DF_VERSION:
#        https://github.com/deltaforge-org/delta-forge/releases/download/
#          v${DF_VERSION}/deltaforge-cli-${DF_VERSION}-linux-x64.tar.gz
#          v${DF_VERSION}/deltaforge-compute-${DF_VERSION}-linux-x64.tar.gz
#      Each tarball ships with a .sig that's verified against the bundled
#      DeltaForge release public key (DF_GPG_PUBKEY_PATH).
#      Note the renames inside the tarballs:
#        deltaforge-cli      -> /usr/local/bin/delta-forge-cli
#        deltaforge-compute  -> /usr/local/bin/delta-forge-compute
#      (The release script renames at staging time, see scripts/build-release.sh:311,314.)
#
#   2. delta-forge-server (control plane)
#      Not yet shipped by deltaforge-org's release.yml (the public release
#      components are cli, mcp, compute; there is no `server` component).
#      The bench's docker-publish.yml workflow source-builds it from the
#      engine repo at the same DF_VERSION tag and drops it into
#      ./build/df-bins/delta-forge-server. This is the ONLY thing that lands
#      in ./build/df-bins/ now; cli + compute are downloaded.
#
# When the engine adds `server` to its release components, this Dockerfile
# will switch to a third download line and the workflow's source-build step
# will go away. The image contract does not change.

# ---------------------------------------------------------------------------
# DeltaForge binary acquisition — BUILD_MODE selects the path.
#
#  local:   All three binaries are pre-staged in ./build/df-bins/ by
#           scripts/stage-local-bins.sh. No network access required.
#           DF_VERSION not needed. Used during development / pre-release.
#
#  release: CLI + compute downloaded from GitHub release at DF_VERSION.
#           Server is still built from source by the publish workflow and
#           staged at ./build/df-bins/delta-forge-server (the only file
#           that workflow puts there in release mode).
# ---------------------------------------------------------------------------

# Always copy build/df-bins/ into a staging directory.
# In local mode this has all three binaries.
# In release mode it has delta-forge-server only (workflow artifact).
COPY build/df-bins/ /tmp/df-stage/

# Bundle the GPG public key for release-mode signature verification.
COPY ${DF_GPG_PUBKEY_PATH} /tmp/deltaforge-release-key.asc

RUN set -eux; \
    if [ "${BUILD_MODE}" = "local" ]; then \
        echo "=== BUILD_MODE=local: installing pre-staged binaries ==="; \
        for bin in delta-forge-cli delta-forge-server delta-forge-compute; do \
            if [ ! -f "/tmp/df-stage/${bin}" ]; then \
                echo "ERROR: /tmp/df-stage/${bin} not found." >&2; \
                echo "  Run scripts/stage-local-bins.sh first." >&2; \
                exit 1; \
            fi; \
            install -m 0755 "/tmp/df-stage/${bin}" "/usr/local/bin/${bin}"; \
            echo "  installed ${bin}"; \
        done; \
    else \
        echo "=== BUILD_MODE=release: downloading from GitHub releases ==="; \
        if [ -z "${DF_VERSION}" ]; then \
            echo "ERROR: DF_VERSION build-arg required for BUILD_MODE=release" >&2; \
            exit 1; \
        fi; \
        cd /tmp; \
        GNUPGHOME=$(mktemp -d) && export GNUPGHOME; \
        gpg --batch --import /tmp/deltaforge-release-key.asc; \
        if [ -n "${DF_GPG_FINGERPRINT}" ]; then \
            gpg --batch --list-keys --with-colons "${DF_GPG_FINGERPRINT}" \
                > /dev/null 2>&1 \
                || { echo "ERROR: key fingerprint mismatch"; exit 1; }; \
        fi; \
        for component in cli compute; do \
            url_base="${DF_RELEASE_URL}/v${DF_VERSION}"; \
            tarball="deltaforge-${component}-${DF_VERSION}-linux-x64.tar.gz"; \
            curl -fsSL --retry 3 -o "${tarball}"     "${url_base}/${tarball}"; \
            curl -fsSL --retry 3 -o "${tarball}.sig" "${url_base}/${tarball}.sig"; \
            gpg --batch --verify "${tarball}.sig" "${tarball}"; \
            sha256sum "${tarball}" > "${tarball}.sha256"; \
            tar -xzf "${tarball}"; \
        done; \
        install -m 0755 /tmp/deltaforge-cli     /usr/local/bin/delta-forge-cli; \
        install -m 0755 /tmp/deltaforge-compute /usr/local/bin/delta-forge-compute; \
        mkdir -p /usr/local/share/deltaforge-bench; \
        mv /tmp/deltaforge-*-${DF_VERSION}-linux-x64.tar.gz.sha256 \
           /tmp/deltaforge-*-${DF_VERSION}-linux-x64.tar.gz.sig \
           /usr/local/share/deltaforge-bench/ 2>/dev/null || true; \
        rm -rf "${GNUPGHOME}" /tmp/deltaforge-* /tmp/*.tar.gz; \
        # Server comes from the staged build artifact (workflow-built).
        install -m 0755 /tmp/df-stage/delta-forge-server /usr/local/bin/delta-forge-server; \
    fi; \
    rm -rf /tmp/df-stage /tmp/deltaforge-release-key.asc; \
    echo "=== Binary check ==="; \
    delta-forge-cli --version  2>/dev/null || echo "WARN: cli --version failed"; \
    delta-forge-server --help  >/dev/null 2>&1 || echo "WARN: server --help failed";

# Bench harness lives at /workspace, mounted from the host at runtime.
WORKDIR /workspace

# Entrypoint: starts Postgres, waits for it, runs the headless bootstrap by
# starting delta-forge-server, waits for /health, starts delta-forge-compute,
# then `exec`s whatever CMD the user passed (or sleeps so an interactive
# `docker compose exec bench ...` flow works).
COPY docker/bench-entrypoint.sh /usr/local/bin/bench-entrypoint.sh
RUN chmod +x /usr/local/bin/bench-entrypoint.sh

# Sanity check: print all pinned versions on container start so the manifest
# generator can scrape them.
RUN printf '%s\n' \
        '#!/bin/sh' \
        'set -e' \
        'echo "df_git_sha=${DF_GIT_SHA}"' \
        'echo "delta_forge_cli=$(delta-forge-cli --version 2>/dev/null || echo unknown)"' \
        'echo "postgres=$(postgres --version 2>/dev/null || echo unknown)"' \
        'echo "java=$(java -version 2>&1 | head -1)"' \
        'echo "python=$(python --version)"' \
        'python -m pip show pyspark delta-spark duckdb deltalake psutil 2>/dev/null | grep -E "^(Name|Version):"' \
        > /usr/local/bin/print-versions \
    && chmod +x /usr/local/bin/print-versions

ENTRYPOINT ["/usr/local/bin/bench-entrypoint.sh"]
# Default: run the benchmark and write results to /results.
# Override with `docker run ... sleep infinity` for interactive exploration.
CMD ["python", "bench_runner.py", \
     "--engines", "df,spark-default", \
     "--workloads", "tpch_read,bulk_load,crud", \
     "--results-dir", "/results", \
     "--no-purge"]
