# Graph analytics: GPU vs CPU — DeltaForge benchmark

This page is generated from a run of the `graph_gpu_vs_cpu` workload. It
has not been populated yet on this checkout.

It is an **honest crossover study**: the same Cypher `CALL algo.*` query
run once on the CPU (Rayon, all cores) and once on the GPU
(`ON GPU THRESHOLD 1`), across graphs of 1M / 5M / 10M / 30M nodes
(~4.5x edges per node), for the algorithms whose GPU kernel is verified
by the `graph-gpu-10m-finance` correctness demo (PageRank, Connected
Components, Louvain, Betweenness, Triangle Count). The published table
shows where the GPU actually overtakes the CPU and where it does not.

## Generate this page

```bash
# 30M needs ~32 GB RAM; on a smaller host set GRAPH_BENCH_SIZES_M="1,5,10".
export DF_HTTP_TIMEOUT_SECS=0
python bench_runner.py --engines df --workloads graph_gpu_vs_cpu --no-purge
python reports/build_graph_gpu_report.py \
    --results-dir results/<timestamp>-<host> --out published/graph_gpu.md
```

Workload definition: [`workloads/graph_gpu_vs_cpu.py`](../workloads/graph_gpu_vs_cpu.py).
Report builder: [`reports/build_graph_gpu_report.py`](../reports/build_graph_gpu_report.py).
Methodology: [methodology.md](methodology.md).

---

[Index](index.md) · [TPC-H](tpch.md) · [Methodology](methodology.md)
