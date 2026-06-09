# driver-bench .NET sub-bench

Real .NET measurement of the DeltaForge ODBC and ADBC drivers,
exercising the access patterns Power BI Desktop, Power BI Service via
Gateway, .NET Tableau, DBeaver, and any other .NET-based BI client
actually use. Runs on .NET 8+ on Linux against the same DeltaForge
control plane the C++ bench in `../` targets.

## Why a .NET sub-bench

The C++ harness in `../` measures three patterns: ODBC + SQLBindCol +
SQLFetch (the unixODBC fast path), ODBC + per-cell SQLGetData (an
emulation of the BI-tool consumption pattern), and the DeltaForge ADBC
bridge consumed via the Arrow C Stream Interface.

The .NET sub-bench complements that with the **actual** managed
consumption stacks BI tools use:

| Mode          | What runs                                              |
|---------------|--------------------------------------------------------|
| `odbc-reader` | `System.Data.Odbc.OdbcDataReader.GetValues` per row.   |
|               | Internally translates to per-cell `SQLGetData` at each |
|               | column's natural C type via the unixODBC driver        |
|               | manager. This is what Power BI Mashup, EF Core's       |
|               | OdbcConnection, and Linq-over-ODBC reduce to.          |
| `adbc-arrow`  | `Apache.Arrow.Adbc.AdbcDriverLoader.LoadDriver` ->     |
|               | statement.ExecuteQuery -> `IArrowArrayStream` loop.    |
|               | Zero per-cell conversion; the consumer reads          |
|               | Arrow RecordBatches by reference. This is the path     |
|               | Power BI Desktop 2.145.1105.0+ takes when an ADBC      |
|               | driver is registered.                                  |

Same SQL, same DeltaForge control plane, sequential runs in one
invocation, warm-median across `--iters N` measured iterations with
`--warmups M` discards. Phase breakdown: connect / execute / drain /
release.

## Build

Requires .NET 8 SDK (`apt install dotnet-sdk-8.0` on Debian/Ubuntu;
`dnf install dotnet-sdk-8.0` on RHEL/Fedora; or the `dotnet-install.sh`
script). The csproj pins the two NuGet packages we depend on:
`System.Data.Odbc` and `Apache.Arrow.Adbc`.

```
dotnet build -c Release
```

## Run

```
dotnet run -c Release -- \
    --warmups 1 --iters 3 \
    --sql "SELECT * FROM demo.retail.fact_sales LIMIT 100000" \
    --odbc-dsn deltaforge \
    --adbc-uri http://your-control-plane:3000 \
    --adbc-user you@example.com \
    --adbc-pwd  '<your password>' \
    --adbc-so   /path/to/libdeltaforge_adbc.so \
    --json-out  ../results/dotnet-run.json
```

The DSN must be configured the same way the C++ bench expects
(`/etc/odbc.ini` or `~/.odbc.ini`). For the docker-self-provisioned
flow, `../scripts/run-in-container.sh` writes both files automatically.

## Initial results (100 k rows x 75 cols on demo.retail.fact_sales)

Warm median, 1 warmup + 3 measured iters, Linux x86_64, .NET 8.0.126,
unixODBC 2.3.9, DeltaForge ADBC bridge 1.0.0:

| Mode          | t_connect | t_execute | t_drain | t_total | rows/sec |
|---------------|----------:|----------:|--------:|--------:|---------:|
| `odbc-reader` |  2.213 s  |  3.464 s  | 2.275 s | 8.034 s | 12.4 k/s |
| `adbc-arrow`  |  481 us   |  5.017 s  | 566 ms  | 5.582 s | 17.9 k/s |

**ADBC is 1.44x faster end-to-end and 4.02x faster on the drain
phase** in real .NET. The drain ratio is the diagnostic number: per-cell
`SQLGetData` through OdbcDataReader pays a managed marshalling cost on
every cell that ADBC's Arrow-stream consumption skips entirely. The
total speedup is smaller than the drain speedup because both drivers
spend several seconds on the server-side query + first-batch
materialise (Apache.Arrow.Adbc's `ExecuteQuery()` is synchronous and
blocks until the first batch is ready, so the wait shows up in
`t_execute` on .NET ADBC; the .NET OdbcDataReader splits that wait
between `Open()` and `ExecuteReader()` semantics, which is why
ODBC's `t_execute` looks shorter at 3.46 s here).

The Linux per-cell ODBC path (`OdbcDataReader`) is the same code that
runs on Windows .NET against the Microsoft driver manager. Numbers
should be directionally identical between Linux and Windows on the
same driver build; absolute values vary with TLS and driver-manager
overhead.
