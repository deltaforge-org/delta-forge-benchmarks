# DeltaForge BI driver bench: ODBC vs ADBC

Linux head-to-head between the DeltaForge ODBC driver (in two
consumption modes) and the DeltaForge ADBC driver, driven through the
[driver-bench/](../) harness against a self-provisioned DeltaForge
control plane. The bench measures driver-side wall time on a real BI
fact table scan, broken out per-phase, so the structural difference
between the two driver APIs is visible without hand-waving about
"Arrow is faster."

## Headline numbers

Both runs use `SELECT ... FROM demo.retail.fact_sales LIMIT N` against
a Delta table the bench provisions and loads on first boot. The table
is a 200 M-row, 75-column synthetic retail fact (BIGINT keys, VARCHAR
strings, DATE, TIMESTAMP WITH TIME ZONE, DECIMAL(18,4), DOUBLE,
INTEGER, BOOLEAN). Warm median across 3 measured iterations, 1
discarded warmup. Hardware: WSL2 Ubuntu 22.04 on the same host as the
control plane (`172.29.80.1:3000`, embedded compute on `:3031`).

### Wide scan, 100 k rows × 75 columns

| Driver / mode    | t_total (s) | t_execute (s) | t_drain (s) | rows/sec |
|------------------|------------:|--------------:|------------:|---------:|
| ODBC bound       |       6.450 |         3.437 |       0.725 |   15.5 k |
| ODBC SQLGetData  |       6.524 |         3.384 |       1.063 |   15.3 k |
| ADBC             |       6.608 |     0.0005    |       4.646 |   15.1 k |

At this row count the server-side scan + Arrow serialisation dominates
end-to-end wall time. All three driver paths run within ~3 % on
`t_total`. **The per-phase breakdown is where the structural story
shows up:** `t_drain` for ODBC `SQLGetData` is **1.47×** the bound-column
`t_drain`, and that gap is exactly the per-cell binding cost the .NET
`OdbcDataReader` and Power BI mashup engine pay on every cell.

### Narrow scan, 1 M rows × 10 columns

| Driver / mode    | t_total (s) | t_execute (s) | t_drain (s) | rows/sec |
|------------------|------------:|--------------:|------------:|---------:|
| ODBC bound       |       1.745 |         0.497 |       0.606 |  573.2 k |
| ODBC SQLGetData  |       2.123 |         0.527 |       1.048 |  471.0 k |
| ADBC             |       2.223 |     0.0005    |       1.811 |  449.8 k |

With a narrower projection the server-side cost drops and the driver
layer shows up more clearly. `SQLGetData` is **22 % slower** end-to-end
than bound-column ODBC, and **1.73×** in the drain phase. This is the
cost a BI tool that does not bind columns up-front actually pays for a
1 M-row scan.

## How to read the per-phase numbers

`t_drain` is the most diagnostic phase. It is the only phase where the
two APIs diverge structurally:

- **ODBC bound** drain = `SQLFetch` loop. Driver fills pre-bound
  caller buffers one row at a time. The columnar → row transpose
  inside the driver happens once per row.
- **ODBC SQLGetData** drain = `SQLFetch` loop + per-(row, column)
  `SQLGetData` call. Each cell is copied individually at the column's
  natural C type. This is what `.NET`, `pyodbc.fetchall`, and the
  Power BI mashup engine do today.
- **ADBC** drain = `ArrowArrayStream.get_next` loop. The driver hands
  the caller an Arrow batch by reference and the caller consumes
  columnar buffers directly. There is no per-cell copy.

`t_connect` includes the first-connect TLS handshake, the
`information_schema` cache prime on the ODBC side, and `dlopen` +
`AdbcDriverInit` on the ADBC side. It is noisy by construction
(warmup amortises it, but a single warmup is sometimes not enough on
a cold worker) and should not be the headline. `t_total` minus
`t_connect` is the metric to compare across rows of the table above.

## What this bench does and does not measure

It measures driver-side wall time for a one-shot BI-style scan from a
single client thread. It does not measure:

- **Windows .NET hot path.** unixODBC on Linux drives the DeltaForge
  ODBC driver through `SQLBindCol` + `SQLFetch` when the caller binds
  columns. The per-cell `SQLGetData` path that `.NET`'s
  `OdbcDataReader` exercises on Windows is structurally different;
  the SQLGetData mode in this bench approximates the same access
  pattern but not the same driver-manager.
- **Concurrent load.** The bench drives one connection. Concurrent
  driver behaviour is out of scope here; the existing TPC-H /
  TPC-DS / SSB / JOB benches in this repository cover that for the
  engine side.
- **Result-set shape dependence.** Two shapes are published above.
  Customer workloads vary; the harness accepts `--sql 'SELECT ...
  FROM your.table'` so you can drop in a workload that matches your
  actual BI scan.

## How to reproduce

The bench self-provisions a DeltaForge stack (Postgres + control
plane + worker) in the canonical bench docker shape and runs the
client inside the same container.

```
export DELTA_FORGE_LICENSE_KEY=dfk_...    # free at console.deltaforge.org
./driver-bench/scripts/run.sh
```

The results above were collected with:

```
./build/driver_bench \
    --warmups 1 --iters 3 \
    --sql "SELECT * FROM demo.retail.fact_sales LIMIT 100000" \
    --adbc-uri http://172.29.80.1:3000 \
    --odbc-dsn deltaforge --odbc-uid ... --odbc-pwd ...
```

Raw JSON for both runs is in [../results/](../results/) (one
`run-${utc}.json` per invocation, machine-readable, schema documented
in `src/bench_main.cpp::emit_json`).

## Methodology

- **Iteration count.** 3 measured iterations + 1 discarded warmup per
  driver mode. The reported number is the per-phase median across the
  measured iterations, excluding any iteration that errored. Phase
  medians are computed independently, so the warm-median row is not a
  single sample.
- **Same query, same control plane, same hardware.** The three driver
  modes run sequentially in one bench invocation; the SQL, the
  control-plane URL, the compute worker, and the configured DSN are
  identical across them. No per-mode tuning.
- **No driver tuning between modes.** The ODBC bound and SQLGetData
  modes use the same `~/.odbc.ini` DSN, the same `/etc/odbcinst.ini`
  registration, and the same column-binding C types (chosen via
  `SQLDescribeCol` to match each column's SQL type 1:1). The
  difference between the two modes is whether the bench calls
  `SQLBindCol` before the drain loop. No other knob varies.
- **Result correctness.** Each iteration counts rows actually drained
  (`row_count` field in the JSON). All iterations published above
  drained the full LIMIT.
