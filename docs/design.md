# Design invariants and scope

Why the bench looks the way it does, what it deliberately does not
cover, and what counts as a release blocker. Companion to
[setup.md](setup.md) and the per-benchmark pages in
[`published/`](../published/index.md).

## What this is, and what it is not

**This is** an honest, reproducible head-to-head benchmark. The
harness, the data, the SQL, the engine versions, and the hardware are
all pinned and documented. You can clone this repo, run one command,
and produce numbers on your own machine that are directly comparable
to the numbers we publish under [`published/`](../published/).

**This is not** a marketing benchmark. There is no cherry-picking.
Queries where DeltaForge ties or loses are reported in every
published table, by name, with the slowdown factor.

## Design invariants (non-negotiable)

These properties are baked into the harness. If we ever break one,
that's a release blocker.

1. **Scripted, deterministic data generation. No live streams.**
   All input data is produced by deterministic scripts (`data_gen/generate_*.py`)
   into static files on disk before any engine starts. SHA-256 of every
   data file is recorded in `manifest.json`. The benchmark never reads
   from a CDC feed, a Kafka topic, a network stream, or anything
   time-varying. Every engine reads **identical bytes** from the same
   on-disk Delta directories.

2. **One engine runs at a time.** The harness never co-runs engines.
   Whichever engine is active gets the container's full CPU and memory
   budget.

3. **Identical sandbox.** Every engine runs inside the same Docker
   image with the same `--cpus` and `--memory` limits. No engine has
   a privilege, mount, or network advantage another does not have.

4. **Explicit state purge between engines.** Engine processes are
   killed, `/tmp` is cleared, and the host OS page cache is dropped
   (via the privileged `dropcaches` sidecar) before any cold run.
   Runs where the page cache could not be verified-cold are labeled
   `cold-os-cache=unverified` and excluded from the headline number.

5. **Two Spark baselines published.** "Stock OSS defaults" (the
   config a user gets from `pip install pyspark` with no extra
   tuning) and "Tuned" (AQE + DPP + runtime bloom filter + raised
   heap, etc.). Both are published side by side. The exact
   configuration of each is committed verbatim in
   [`engines/spark_default_engine.py`](../engines/spark_default_engine.py)
   and [`engines/spark_tuned_engine.py`](../engines/spark_tuned_engine.py).

6. **Plain Delta protocol on every fixture.** Deletion vectors,
   column mapping, and row tracking are all explicitly disabled
   so DuckDB's read-only `delta` extension can read every fixture.
   Without this constraint, DuckDB drops out and the suite collapses
   to df vs Spark only.

7. **Honest losses.** Every query result is reported. Every
   per-benchmark page in [`published/`](../published/) shows the
   slowdown factor on the queries where DeltaForge loses, by name.

8. **Open license.** Apache 2.0. Anyone may run, modify, and
   republish results.

## Scope filter

Every workload in this bench reads from or writes to **Delta tables**.
Benchmarks that fundamentally cannot run on the Delta format are out
of scope:

- **Graph databases against a native graph store** (Neo4j, Memgraph,
  TigerGraph): they don't read Delta. Running them is comparing graph
  stores, not Delta engines.
- **KV workloads** (YCSB): not a Delta access pattern.
- **OLTP against row stores** (TPC-C, TPC-E): wrong access pattern for
  a lake engine; running them produces numbers that mislead more than
  they inform.
- **ClickBench raw fixture**: a single 14 GB compressed Parquet file
  is not a real-world layout. Adopting the query shapes against a
  partitioned Delta version is acceptable; adopting the raw fixture is
  not.

The point of this benchmark is to measure DeltaForge against other
engines reading and writing the same Delta files, not against engines
doing something different.

## Future chapters

The current TPC-H + TPC-DS + SSB + JOB + synthetic-write set is the
v0.1 published headline. Anything beyond that is unfunded and
unscheduled.

| When | Workload chapter | Notes |
| --- | --- | --- |
| If asked | **Delta-protocol-specific writes**: MERGE / SCD2, time-travel joins (`AS OF VERSION`), OPTIMIZE / Z-ORDER, VACUUM, CDC ingestion (`table_changes`). df vs Spark + Delta Lake. | No analogue on bare Parquet engines; this is where the DeltaForge writer differentiates from Spark + Delta Lake. |
| If asked | **BI / ODBC over Delta**: Power BI / Tableau / Excel query shapes. df ODBC driver vs Spark Thrift / Databricks SQL endpoint, same plain-Delta TPC-H fixture. | The ODBC driver is a first-class shipping artifact; this is where it gets measured against the same workloads BI tools actually issue, on the same Delta bytes. |
| If asked | **Larger scale tiers**: SF=10 on a 32 GB host, SF=100 on a reference cloud instance. | SF=1 is a sanity-check; SF=10 is where engine differences emerge cleanly. |
| If asked | **Concurrency / tail latency** on the same Delta read + write workloads: multiple simultaneous clients, p99 under sustained load, mixed read/write contention against a shared Delta table. | Single-client throughput is necessary but not sufficient. |

## Contributing

PRs that improve methodology, tighten the Spark "tuned" config, add
new queries to existing chapters, or reproduce the published numbers
on different hardware are welcome. The bar is methodology, not
advocacy.

If you spot a methodological issue, the channel is
[GitHub issues](https://github.com/deltaforge-org/delta-forge-benchmarks/issues)
on this repo. PRs that add a knob and a one-line rationale are
welcome; PRs that just bump a number without rationale will be closed.
