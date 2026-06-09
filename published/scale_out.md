# Scale-out concurrency — DeltaForge benchmark

Per-query latency curve as independent workers are added. This is the
honest version of the TPC-DS Throughput Test for an architecture
where each query runs on exactly one worker with no distributed
coordination.

> One page per benchmark; full methodology in [methodology.md](methodology.md).
> Other benchmarks: [TPC-H](tpch.md) · [TPC-DS](tpcds.md) · [SSB](ssb.md) · [JOB](job.md) · [Index](index.md)

## What this measures

For N ∈ {1, 2, 4, 8} concurrent workers, each worker independently
runs the TPC-DS SF100 Power query set (all 99 queries) under a fixed
4-CPU + 4 GB unit-worker budget. The headline is the per-query
**latency curve** across N values: how much does each query's
wall-clock change as more workers are added?

If the curve is flat (every query stays within 1.10x of its N=1
baseline at every measured N), the architectural claim "one query,
one worker, no coordination" is demonstrated: adding workers does
not impose a per-query cost on the workers already running.

## What this does NOT measure

- **Aggregate throughput (QphDS, queries-per-hour, QPS)** -- by design.
  Multi-user throughput is uninteresting for an engine where each
  query runs on its own worker (it is `N x single-stream` by
  construction), and publishing such a number invites comparisons on
  a metric that does not reflect the architecture.
- **Distributed query execution** -- a single query still runs on a
  single worker. The concurrency is *across* queries, not within
  one.

## Unit-worker budget

| Resource    | Budget per worker | Enforcement |
| ----------- | ----------------- | ----------- |
| CPU         | 4 logical threads | cgroup v2 `AllowedCPUs`, disjoint blocks per worker |
| RAM         | 4 GB              | cgroup v2 `MemoryMax`, hard cap |
| Swap        | 0                 | cgroup v2 `MemorySwapMax=0`; an over-budget worker OOMs instead of thrashing |
| Threads     | DataFusion auto-sizes via tokio `available_parallelism()` and sees the affinity count | implicit via CPU affinity |

Why these numbers: this is a typical "small cluster node" sizing
(4 vCPU / 4 GB matches the smallest practical worker shape on every
hyperscaler). It also lets a 32 GB host fit N=8 workers cleanly with
no swap; bigger boxes can raise both the unit and the N cap.

## Methodology

- **Workload**: TPC-DS SF100 Power query set (99 queries, one pass
  per stream). Each stream sets `DF_BENCH_STREAM_SEED=<i>` so the
  per-stream query order is decorrelated (TPC-DS Throughput Test
  protocol). All streams read the same on-disk plain Delta tables;
  OS page cache is shared (which benefits all workers fairly).
- **Cold/warm**: each stream runs each query exactly once
  (`--cold-runs 0 --warm-runs 1`). A one-pass N=1 warmup run is
  performed before the measured matrix so the N=1 baseline is itself
  warm-cache; warmup results are discarded.
- **Sampling**: at concurrency level N, each query has N samples
  (one per stream). The published numbers per (query, N) are p50 and
  p95.
- **Verdict rule**: `flat` per (query, N) = `p50@N / p50@N=1 <= 1.10`.
  The summary reports, per N, the fraction of queries that are flat
  plus the worst-case ratio across all queries.
- **Quiesce**: the orchestrator (`scale_out/orchestrate.sh`) refuses
  to run if non-bench processes are consuming > 1% CPU. Without this
  the curve measures whatever else was scheduled on the host, not
  the engine.

## Host

The single-box scale-out matrix is bounded by host RAM. On a 32 GB
box the matrix runs cleanly through N=8 (8 x 4 GB = 32 GB total, no
swap). N=16 requires ≥64 GB; raise `N_MAX` in `orchestrate.sh` only
on a host that can hold the budget with swap disabled.

| Capability  | Floor for N    |
| ----------- | -------------- |
| 32 GB host  | N ≤ 8          |
| 64 GB host  | N ≤ 16         |
| 128 GB host | N ≤ 32         |

CPU floor is `N * cores_per_worker <= nproc`. The default
`cores_per_worker=4` means the 36-thread i9-7980XE can pin up to N=9
without oversubscription; the RAM cap binds first.

## Reproducing this curve

```bash
# 1. Stage the SF100 fixtures (TPC-H + TPC-DS + SSB + JOB).
docker compose exec bench ./scripts/prepare_sf100.sh

# 2. Quiesce the host (stop Tauri dev server, browsers, Docker
#    workloads other than the bench container).

# 3. Run the scale-out matrix.
docker compose exec bench ./scale_out/orchestrate.sh
```

Output artifacts:

- [`scale_out/curve.csv`](../scale_out/curve.csv) -- per-(query, N)
  p50 / p95 / ratio_vs_n1
- [`scale_out/curve.json`](../scale_out/curve.json) -- structured
  form plus per-N verdict summary
- [`scale_out/curve.md`](../scale_out/curve.md) -- human-readable
  verdict tables

Raw per-stream records under
[`scale_out/results/scaleout_n{N}_{TS}_s{i}/`](../scale_out/results/).

## Verdict template (filled in once the run completes)

| N | queries | flat (within 1.10x of N=1) | worst ratio | median ratio |
| - | ------: | -------------------------: | ----------: | -----------: |
| 1 | 99      | n/a                        | 1.000       | 1.000        |
| 2 | _tbd_   | _tbd_                      | _tbd_       | _tbd_        |
| 4 | _tbd_   | _tbd_                      | _tbd_       | _tbd_        |
| 8 | _tbd_   | _tbd_                      | _tbd_       | _tbd_        |

Replace `_tbd_` with the content of `scale_out/curve.md` after the
orchestrator finishes. Per project rule, this page does not publish
aggregate throughput (QphDS, queries-per-hour, QPS).

---

Next: [TPC-DS](tpcds.md) · [Methodology](methodology.md) · [Index](index.md)
