# driver-bench: DeltaForge ODBC vs ADBC head-to-head

Reproducible Linux benchmark that drives the same query through the
DeltaForge **ODBC** driver and the DeltaForge **ADBC** driver against
the same control plane, and reports per-phase wall-clock timings for
both.

The point of this bench is not to claim "ADBC is N times faster."
That ratio depends heavily on the result-set shape (wide vs narrow,
decimal-heavy vs integer-only, mostly NULLs vs dense) and on the BI
client's bind pattern. The point is to give anyone a single command
they can run against their own DeltaForge instance to see what the
two drivers actually do on their workload, with the per-phase
breakdown that explains the difference (the `t_bind` phase exists on
the ODBC side and has no analogue on the ADBC side; that gap is the
columnar-to-row buffer setup ADBC eliminates by handing the consumer
an `ArrowArrayStream` directly).

## What the bench measures

Each iteration drives one driver through the full BI-tool lifecycle.

ODBC path:

| Phase       | What runs                                                            |
|-------------|----------------------------------------------------------------------|
| `t_connect` | `SQLAllocHandle(ENV/DBC)`, `SQLSetEnvAttr`, `SQLConnect`             |
| `t_execute` | `SQLExecDirect` (we collapse prepare+execute the way BI tools do)    |
| `t_bind`    | `SQLNumResultCols`, then per-column `SQLDescribeCol` + `SQLBindCol`  |
| `t_drain`   | `SQLFetch` loop until `SQL_NO_DATA`                                  |
| `t_release` | `SQLCloseCursor`, `SQLFreeHandle(STMT/DBC/ENV)`, `SQLDisconnect`     |

ADBC path:

| Phase       | What runs                                                                 |
|-------------|---------------------------------------------------------------------------|
| `t_connect` | `dlopen`, `AdbcDriverInit`, `AdbcDatabase{New,SetOption*,Init}`, `AdbcConnection{New,Init}` |
| `t_prepare` | `AdbcStatementNew`, `AdbcStatementSetSqlQuery`                            |
| `t_execute` | `AdbcStatementExecuteQuery` (returns the `ArrowArrayStream`)              |
| `t_bind`    | not applicable; reported as 0 for symmetry                                |
| `t_drain`   | `ArrowArrayStream.get_next` loop until the stream signals end             |
| `t_release` | `AdbcStatement{,Connection,Database}Release`, `dlclose`                   |

For each driver the harness runs `--warmups N` discarded warmup
iterations (default 1) followed by `--iters N` measured iterations
(default 5). The reported number is the **per-phase median** across
the measured iterations, excluding any iteration that errored. Phase
medians are computed independently, so the `t_*` fields of the warm
median are not co-located samples from one row, they are honest
per-phase distributions.

## What the bench does not measure

- **Windows .NET row-API hot path.** This bench is Linux only. unixODBC
  on Linux drives the DeltaForge ODBC driver through `SQLBindCol` +
  `SQLFetch`, which is the bound-column path. The per-cell
  `SQLGetData` path that .NET's `OdbcDataReader` exercises on
  Windows is structurally different and not covered here. On a wide
  mixed-schema scan the Windows gap is materially larger; the Linux
  numbers this bench publishes are a lower bound on the difference.
- **Driver-manager overhead.** unixODBC's driver manager has a fixed
  per-call cost. The ADBC path bypasses it (we `dlopen` the bridge
  directly), but Power BI Desktop also bypasses it when it uses
  `adbc_driver_manager` instead of the unixODBC DM. The two are
  comparable for the BI use case; if you care about the driver-manager
  cost in isolation, attach `strace -c` to the bench process.

## How to run

Same shape as the other benches in this repo (`install.sh` + `run_smoke.sh` +
`run_bench.sh`), run natively on the host.

### Host mode (canonical)

This bench needs a DeltaForge **platform** to talk to. The easiest path is to
install the parent benchmark suite first (`../install.sh`), which downloads the
official platform + CLI; `setup-host-stack.sh` will then launch it if nothing is
already serving. If you already run DeltaForge (the desktop app, or the parent
`../bench`), it just connects to that.

```
# 1. one-shot host setup (unixODBC, cmake, build-essential, .NET 8)
./scripts/install.sh

# 2. download the released ODBC + ADBC driver .so files (the subjects under test)
./scripts/stage-driver-bins.sh

# 3. connect to a DeltaForge platform + configure the zone + DSN
#    (uses an instance already at http://127.0.0.1:3000, else launches the one
#     the parent ../install.sh staged under ../.engine)
export DELTA_FORGE_LICENSE_KEY=DF1...   # only needed if a fresh platform must be launched
./scripts/setup-host-stack.sh

# 4. smoke (~30s, 100k rows)
./scripts/run_smoke.sh

# 5. canonical run (~2-5 min, 1M rows, both C++ and .NET, all driver modes)
./scripts/run_bench.sh

# (optional) stop a platform this bench started + restore your ODBC config
./scripts/teardown-host-stack.sh --restore-odbc
```

What `setup-host-stack.sh` sets up:

| Piece               | Where                                                              |
|---------------------|--------------------------------------------------------------------|
| DeltaForge platform | reused at `http://127.0.0.1:3000` if running, else the `../.engine` AppImage is launched (control plane + compute + DB in one process) |
| License             | your `DELTA_FORGE_LICENSE_KEY`, self-activated at bootstrap (required to launch a fresh platform; no key is bundled) |
| Drivers             | released ODBC + ADBC `.so` in `build/df-drivers/` (from `stage-driver-bins.sh`) |
| Bench zone          | `bench` (silver) under `${DF_HOME:-/tmp/df-bench-stack}/data/bench`  |
| Fixture table       | built by `build-fixture.sh` via ODBC CTAS (default 1M rows × 22 mixed-type cols) |
| unixODBC DSN        | `~/.odbc.ini` + `~/.odbcinst.ini` (originals backed up to `*.bench-backup`) |

Both `run_smoke.sh` and `run_bench.sh` source `${DF_HOME:-/tmp/df-bench-stack}/stack.env`
that `setup-host-stack.sh` writes, so the DSN, URLs, credentials, and driver
paths are picked up automatically. Re-running `setup-host-stack.sh` is a no-op
once the platform is reachable.

### Tuning knobs

| Variable                  | Default     | Effect                                 |
|---------------------------|-------------|----------------------------------------|
| `DRIVER_BENCH_ROWS`       | `1000000`   | Fixture row count                      |
| `DRIVER_BENCH_ITERS`      | `3`         | Measured iterations per driver         |
| `DRIVER_BENCH_WARMUPS`    | `1`         | Discarded warmup iterations            |
| `DRIVER_BENCH_DRIVER`     | `both`      | `odbc`, `adbc`, or `both` (C++ only)   |
| `DRIVER_BENCH_TABLE`      | `t_wide`    | Fixture table name                     |
| `DELTA_FORGE_LICENSE_KEY` | *required*  | Activates DeltaForge during bootstrap  |

### Client-mode (run against your own DeltaForge instance)

If you already have a DeltaForge control plane running and just want to
exercise the bench against it, skip `setup-host-stack.sh` and configure
unixODBC + run the binary directly:

```
# one-time host setup
./scripts/install.sh

# build the harness
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j

# run against your stack
./build/driver_bench \
    --adbc-uri http://your-control-plane:3000 \
    --adbc-user you@example.com \
    --adbc-pwd  '<your password>' \
    --adbc-so   /path/to/libdeltaforge_adbc.so.1 \
    --odbc-dsn  deltaforge \
    --warmups 1 --iters 3 \
    --sql 'SELECT * FROM your.schema.wide_table'
```

## DSN configuration (client-mode only)

When you run the bench in client-mode against an external DeltaForge
instance, the ODBC path resolves your driver through a configured DSN.
`setup-host-stack.sh` writes the DSN automatically when it provisions a
local host stack.

Sample `~/.odbc.ini` for client-mode:

```ini
[deltaforge]
Driver        = DeltaForge
Server        = http://your-control-plane:3000
ComputeServer = http://your-compute-node:3031
Uid           = you@example.com
Pwd           = <your password>
TLS           = disabled
```

With a matching driver registration in `~/.odbcinst.ini`:

```ini
[DeltaForge]
Description    = DeltaForge ODBC Driver
Driver         = /path/to/libdeltaforgeodbc.so
DriverODBCVer  = 03.80
Threading      = 2
```

The ADBC path does not use the DSN. Pass connection info via
`--adbc-uri`, `--adbc-user`, `--adbc-pwd` (or `--adbc-token`).

## Run (client-mode)

The default synthetic query is a 24-column wide mixed-type result
generated server-side by `generate_series`. No fixture table required.
The default row count is 10M, large enough that the structural
difference between ODBC's per-cell bind-and-fetch path and ADBC's
Arrow-stream path is unambiguous in the report.

```
./build/driver_bench \
    --adbc-uri http://your-control-plane:3000 \
    --adbc-user you@example.com \
    --adbc-pwd  '<your password>' \
    --adbc-so   /path/to/libdeltaforge_adbc.so.1 \
    --odbc-dsn  deltaforge \
    --warmups   1 \
    --iters     5 \
    --rows      10000000
```

To measure a real table instead of the synthetic one:

```
./build/driver_bench \
    --adbc-uri  http://your-control-plane:3000 \
    --adbc-user you@example.com \
    --adbc-pwd  '<your password>' \
    --odbc-dsn  deltaforge \
    --sql 'SELECT * FROM your.schema.wide_table'
```

For machine-readable output suitable for ingestion into the published
results page, add `--json-out result.json`. The schema is documented
inside `src/bench_main.cpp` (`emit_json`) and is forward-compatible:
fields are only added, never renamed.

## Interpreting the output

The summary table prints, per driver, the warm median of each phase
and an end-to-end rows/second number. When both drivers ran in the
same invocation, the harness also prints a pairwise comparison:

```
 ADBC vs ODBC: 4.18x on t_total, 11.7x on t_drain (bind + fetch vs Arrow stream)
 ODBC bind phase alone: 142.31 ms (this is the columnar -> row buffer setup that ADBC has no analogue for)
```

The `t_drain` ratio is the most informative one: it isolates the
cost of moving rows out of the driver into caller-visible buffers,
which is where the two architectures actually diverge. `t_connect`
ratios are dominated by one-time DLL-load and TLS handshake cost
and tend to be noisy; do not over-read them.

## Threats to validity

- **Result-set shape sensitivity.** The synthetic query is one
  particular mixed-type shape. Narrow integer-only results show a much
  smaller gap; very wide decimal-heavy results show a much larger one.
  Run with `--sql 'SELECT * FROM your.real.table'` to get a number
  relevant to your workload.
- **Warmup count.** The driver caches `information_schema` after the
  first connect. Default `--warmups 1` is enough on a well-provisioned
  control plane; increase to 3 if the first-iter `t_connect` is more
  than 5x the second-iter value.
- **Concurrent load.** The bench drives one connection. It is not a
  concurrency stress test. Run it on a quiet compute node for clean
  numbers.
