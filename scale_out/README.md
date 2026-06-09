# Scale-out concurrency bench

Demonstrates that DeltaForge per-query wall-clock holds flat as
independent workers are added. This is the honest version of the
TPC-DS Throughput Test for an architecture where each query runs on
exactly one worker with no distributed coordination: aggregate
throughput is uninteresting (it is N times the single-stream number by
construction), but per-query latency staying flat under load is the
actual architectural claim.

## What you publish

A curve of per-query p50 / p95 wall-clock vs. N concurrent workers,
for N in {1, 2, 4, 8}. If the curve is flat (each N stays within ~1.1x
of the N=1 baseline) the claim is demonstrated. **There is no QphDS,
no QPS, no aggregate throughput number in the output.** That is a
hard rule; see `feedback_no_multiuser_qps_numbers` in project memory.

## Methodology

- **Workload**: TPC-DS SF100 Power query set (all 99 queries, once
  per stream). Run `scripts/prepare_sf100.sh` first to stage the
  Delta tables under `/workspace/data/tpcds_sf100_delta/`.
- **Unit worker**: 4 logical CPUs + 4 GB RAM, cgroup-isolated via
  `systemd-run --user --scope` with `AllowedCPUs`, `MemoryMax`, and
  `MemorySwapMax=0`. Zero swap means an out-of-budget worker OOMs
  rather than thrashing and fabricating a flat curve.
- **CPU pinning**: each worker is pinned to a disjoint 4-CPU block
  (worker 1 -> 0-3, worker 2 -> 4-7, ...). Tokio's
  `available_parallelism()` sees the affinity count so DataFusion
  auto-sizes its runtime to 4 threads per worker.
- **Query order**: each stream sets `DF_BENCH_STREAM_SEED=<i>` so
  `workloads/tpcds_read_delta.py` shuffles the 99-query order
  per-seed. This matches the TPC-DS Throughput Test where per-stream
  ordering tables decorrelate cache / buffer-pool effects across
  streams.
- **Cold/warm**: each stream runs each query exactly once
  (`--cold-runs 0 --warm-runs 1`). OS page cache is shared across
  workers; a one-pass N=1 warmup run is performed before the measured
  matrix so the N=1 baseline is itself warm-cache.
- **Quiesce**: orchestrate.sh refuses to run if `delta-forge-server`,
  the Tauri dev server, or a browser is consuming > 1% CPU. The
  measured numbers are noise without this discipline.

## Files

- `orchestrate.sh` -- launches N copies of `bench_runner.py` under
  cgroup isolation, one per stream, writing each stream's results
  to `results/scaleout_n${N}_s${i}/`. Repeats for N in {1, 2, 4, 8}
  by default; pass `N_VALUES="1 2"` to override.
- `aggregate.py` -- reads every `results/scaleout_n${N}_s${i}/raw/df.jsonl`,
  computes per-(query, N) p50 / p95, and writes
  `scale_out/curve.csv` plus `scale_out/curve.json`. Also writes
  `scale_out/curve.md` with the headline flatness number per query
  and the N=8 / N=1 ratio.
- `prereqs.sh` -- one-shot environment check
  (user systemd available, cgroup v2 controllers, dataset staged,
  expected free disk, machine quiesced). Sourced by orchestrate.sh.

## Caps and limits

This box (i9-7980XE, 36 threads, 32 GB RAM) tops out at N=8 unit
workers (8 workers x 4 GB = 32 GB total, no swap). N=16 needs bigger
hardware: 16 workers x 4 GB = 64 GB RAM is the floor, more
realistically a 64 GB / 64-thread machine. The orchestrator hard-caps
N=16; raise the cap only on a box that actually has the headroom
and re-run the prereqs check.
