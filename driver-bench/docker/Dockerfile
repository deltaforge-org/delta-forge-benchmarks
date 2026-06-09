# driver-bench image. Extends the canonical bench image with the
# unixODBC runtime + dev headers, the two DeltaForge driver shared
# objects, and the driver_bench source built in place.
#
# Why a separate image: the canonical bench image (delta-forge-bench:local)
# bakes Spark, DuckDB, and Postgres in. Adding unixODBC + the ODBC driver
# .so to that image would bloat every TPC-H / TPC-DS / SSB / JOB image
# build with packages those benches do not use. Extending it preserves
# everything the parent already provisions (postgres, control plane,
# worker, GPG-verified DF binaries) and confines the BI-driver-specific
# tools to this derived image.
#
# Why this image still depends on the parent bringing up the stack:
# self-provisioning is the canonical bench shape (see ../../docker/
# bench-entrypoint.sh) and we reuse the same orchestration verbatim.
# This image inherits ENTRYPOINT, so on `docker compose up` it brings
# up postgres + control plane + worker the same way the parent does,
# then exec's our CMD which writes the ODBC DSN and runs driver_bench.

ARG PARENT_IMAGE=delta-forge-bench:local
FROM ${PARENT_IMAGE}

# unixODBC runtime + driver-manager headers, plus a build toolchain so the
# bench compiles its driver_bench binary from the bind-mounted source
# tree at first start. cmake is not in the parent image because the Spark
# benches do not need it.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       unixodbc unixodbc-dev odbcinst \
       cmake build-essential pkg-config \
    && rm -rf /var/lib/apt/lists/*

# The two driver shared objects, staged by
# driver-bench/scripts/stage-driver-bins.sh into build/df-drivers/
# at the benchmarks repo root. Installed under /usr/local/lib/ so
# both unixODBC's driver-name lookup and dlopen from driver_bench
# resolve them via the standard search path.
COPY build/df-drivers/libdeltaforgeodbc.so /usr/local/lib/libdeltaforgeodbc.so
COPY build/df-drivers/libdeltaforge_adbc.so /usr/local/lib/libdeltaforge_adbc.so

# Symlink the ADBC SONAME so a future strict dlopen against
# libdeltaforge_adbc.so.1 also resolves; ldconfig rebuilds the cache so
# unixODBC's dlopen of /usr/local/lib/libdeltaforgeodbc.so finds the
# driver's runtime dependencies without LD_LIBRARY_PATH games.
RUN ln -sf /usr/local/lib/libdeltaforge_adbc.so /usr/local/lib/libdeltaforge_adbc.so.1 \
    && ldconfig

# Register the DeltaForge ODBC driver with unixODBC. The DSN (with
# credentials) is written at container start by run-in-container.sh,
# because the admin password is provisioned at bootstrap time and is
# not known at image-build time.
RUN printf '%s\n' \
        '[DeltaForge]' \
        'Description    = DeltaForge ODBC Driver' \
        'Driver         = /usr/local/lib/libdeltaforgeodbc.so' \
        'DriverODBCVer  = 03.80' \
        'Threading      = 2' \
        'UsageCount     = 1' \
    > /etc/odbcinst.ini

# Default CMD: hand off to the driver-bench runner once the parent
# entrypoint has finished bringing up postgres + control plane + worker.
# Override with `docker compose run --rm driver-bench bash` for
# interactive debugging.
CMD ["/workspace/driver-bench/scripts/run-in-container.sh"]
