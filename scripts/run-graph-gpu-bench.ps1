<#
.SYNOPSIS
    Run the graph GPU-vs-CPU crossover benchmark against the SIGNED DeltaForge
    release. No source build, no cargo, no standalone server/worker.

.DESCRIPTION
    Thin convenience wrapper over the standard signed-binary launcher
    (bench.ps1). The desktop platform shipped in the official release already
    embeds a GPU-enabled compute node (delta-forge-compute is built with the
    `gpu` feature), and the `graph_gpu_vs_cpu` workload forces the GPU path with
    `ON GPU THRESHOLD 1`, so the crossover bench runs through exactly the same
    signed platform as every other workload. There is a single code path: this
    script sets the two workload knobs and delegates to bench.ps1, which boots
    the official `deltaforge` platform headless and tears it down on exit.

    Prerequisite: run `.\install.ps1` once first. It downloads the signed
    platform + CLI from deltaforge-org/delta-forge and writes .env. This script
    does not build anything; if the platform is missing it tells you to install.

.EXAMPLE
    .\scripts\run-graph-gpu-bench.ps1

.EXAMPLE
    # smaller graph tiers, fewer warm runs
    .\scripts\run-graph-gpu-bench.ps1 -SizesM "1,5,10" -Warm 3

.NOTES
    The 30M-node tier builds ~135M edges and wants ~32 GB addressable RAM for
    the CSR + score arrays. On a 16 GB host use -SizesM "1,5,10".
#>
[CmdletBinding()]
param(
    # Comma-separated graph sizes in millions of nodes (GRAPH_BENCH_SIZES_M).
    [string]$SizesM = "30",

    # Warm runs per measured step (GRAPH_BENCH_WARM). 1 cold run is always added.
    [int]$Warm = 3
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$BenchDir = (Get-Item $PSScriptRoot).Parent.FullName          # delta-forge-benchmarks
$BenchPs1 = Join-Path $BenchDir "bench.ps1"
if (-not (Test-Path $BenchPs1)) { throw "bench.ps1 not found at $BenchPs1 (is this the bench repo?)" }
if (-not (Test-Path (Join-Path $BenchDir ".env"))) {
    throw "No .env found. Run .\install.ps1 first; it downloads the signed platform + CLI and writes .env. This script never builds from source."
}

# Workload knobs read by workloads/graph_gpu_vs_cpu.py.
$env:GRAPH_BENCH_SIZES_M = $SizesM
$env:GRAPH_BENCH_WARM    = "$Warm"

Write-Host ""
Write-Host "graph_gpu_vs_cpu via the SIGNED release platform (sizes=$SizesM M, warm=$Warm)" -ForegroundColor Cyan
Write-Host "no source build: bench.ps1 boots the official deltaforge platform headless." -ForegroundColor DarkGray

# Delegate to the single signed-binary launcher. -Extra is forwarded verbatim to
# bench_runner.py; df is the only engine that runs this GPU workload
# (applicable_engines=("df",)). bench.ps1 owns the headless boot, license/device
# activation, and teardown.
& $BenchPs1 -Extra @(
    '--scale', '1',
    '--engines', 'df',
    '--workloads', 'graph_gpu_vs_cpu',
    '--no-purge',
    '--tag', 'graph-gpu'
)
$rc = $LASTEXITCODE

# Best-effort crossover report from the freshest results dir (post-processing
# only; the platform is already down). Never sinks the run's exit code.
if ($rc -eq 0) {
    $resultsDir = Join-Path $BenchDir "results"
    $latest = Get-ChildItem $resultsDir -Directory -ErrorAction SilentlyContinue |
        Sort-Object LastWriteTime -Descending | Select-Object -First 1
    if ($latest) {
        $reportOut = Join-Path $BenchDir "published\graph_gpu.md"
        $venvPy = Join-Path $BenchDir ".venv\Scripts\python.exe"
        $py = if (Test-Path $venvPy) { $venvPy } else { "python" }
        try {
            & $py (Join-Path $BenchDir "reports\build_graph_gpu_report.py") --results-dir $latest.FullName --out $reportOut
            if ((Test-Path $reportOut) -and $LASTEXITCODE -eq 0) {
                Write-Host ""
                Write-Host "----- $reportOut -----" -ForegroundColor Yellow
                Get-Content $reportOut
                Write-Host "----------------------------------------" -ForegroundColor Yellow
            }
        } catch {
            Write-Host "report generator failed ($($_.Exception.Message)); raw df.jsonl is under $($latest.FullName)\raw" -ForegroundColor Yellow
        }
    }
}
exit $rc
