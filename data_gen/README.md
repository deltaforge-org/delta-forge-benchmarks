# data_gen

Deterministic, scripted TPC-H data generation. The benchmark's input is
**always** static Parquet files produced here; there is no streaming source.

## Vendoring TPC-H dbgen

The official TPC-H toolkit is not redistributable, so this repo cannot ship
the source. To populate `data_gen/dbgen-src/`:

1. Download the TPC-H tools from https://www.tpc.org/tpch/ (registration required).
2. Extract the contents of the `dbgen/` directory into `data_gen/dbgen-src/`.
3. Verify the directory contains `dbgen.c`, `makefile`, and `dists.dss`.
4. Rebuild the bench image: `docker compose -f docker/docker-compose.yml build bench`.

The Dockerfile's `dbgen-builder` stage runs `make` against the vendored source.
If the source is missing, the build still succeeds but `generate_tpch.py`
exits with a clear error at runtime.

## Why DuckDB in the middle

dbgen produces pipe-delimited `.tbl` files. Both Spark and DeltaForge can
read text files, but the resulting Parquet bytes would differ slightly
(column statistics, page sizes) depending on which engine wrote them. To
eliminate that as a source of variance, we pin **one** writer (DuckDB) with
a fixed `COMPRESSION SNAPPY` codec, and both engines read those identical
Parquet bytes. SHA-256 of each Parquet file is recorded in the run's
`manifest.json`, so a reviewer with their own dbgen run can verify they
have bit-identical inputs.

## Determinism

dbgen is deterministic given a fixed scale factor and seed. The harness
runs it with default seeds (which are part of the TPC-H spec). DuckDB's
Parquet writer is also deterministic at a given version. Pinning the
DuckDB version in `docker/Dockerfile` is therefore part of the
reproducibility contract.
