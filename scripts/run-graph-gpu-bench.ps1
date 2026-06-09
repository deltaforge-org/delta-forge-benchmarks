<#
.SYNOPSIS
    Self-contained, native-Windows runner for the graph GPU-vs-CPU crossover
    benchmark (delta-forge-benchmarks workload `graph_gpu_vs_cpu`).

.DESCRIPTION
    Stands up a fully isolated, headless DeltaForge stack on this host the same
    way the Docker bench does (docker/bench-entrypoint.sh), but natively so the
    GPU (Vulkan) is reachable:

      1. Builds delta-forge-server (--features embedded-pg) and
         delta-forge-compute (--features gpu) in release, if missing.
      2. Headless-bootstraps a control plane via `delta-forge-server bootstrap
         --from-file`, using an EMBEDDED PostgreSQL (downloaded + managed by the
         server, identical to the desktop GUI's code path) in its own data dir.
      3. Starts the control plane + a GPU-enabled compute worker in the
         background and waits for both to report healthy.
      4. Runs the canonical Python harness
         `bench_runner.py --engines df --workloads graph_gpu_vs_cpu`
         (single source of truth for the workload) at the requested graph size.
      5. Renders the crossover report and prints it.
      6. Tears the stack down (unless -KeepStack).

    The whole stack runs on dedicated ports and its own config dir, so it does
    NOT touch a delta-forge-gui already running on :3000 / embedded PG on :5432.

    A free license key is REQUIRED (the headless bootstrap activates online on
    first start). Supply it via -LicenseKey or $env:DELTA_FORGE_LICENSE_KEY. The
    key is written only to the stack dir under A:\tmp (never the repo) and is
    never committed.

.EXAMPLE
    $env:DELTA_FORGE_LICENSE_KEY = "DF1.xxxxx"
    .\scripts\run-graph-gpu-bench.ps1

.EXAMPLE
    # smaller graph, keep the stack up afterwards for poking around
    .\scripts\run-graph-gpu-bench.ps1 -SizesM "10" -KeepStack

.NOTES
    The 30M-node tier builds ~135M edges and wants ~32 GB addressable RAM for
    the CSR + score arrays. On a 16 GB host use -SizesM "1,5,10".
#>
[CmdletBinding()]
param(
    # Comma-separated graph sizes in millions of nodes (GRAPH_BENCH_SIZES_M).
    [string]$SizesM = "30",

    # Warm runs per measured step (GRAPH_BENCH_WARM). 1 cold run is always added.
    [int]$Warm = 3,

    # Dedicated, GUI-avoiding ports for the isolated bench stack.
    [int]$ControlPort = 3100,
    [int]$PgPort       = 5544,
    [int]$WorkerPort   = 3131,

    # Isolated stack root (config.toml, embedded PG data, logs). Outside the repo.
    [string]$StackDir = "A:\tmp\df-graph-gpu-bench",

    # Headless bootstrap credentials. Defaults are fine for a local bench.
    [string]$AdminEmail        = "admin@deltaforge.local",
    [string]$AdminPassword     = "Bench_Admin_2026!",
    [string]$EngineerEmail     = "engineer@deltaforge.local",
    [string]$EngineerPassword  = "Bench_Engineer_2026!",

    # Free license key. Falls back to $env:DELTA_FORGE_LICENSE_KEY.
    [string]$LicenseKey = $env:DELTA_FORGE_LICENSE_KEY,

    # Explicit Python interpreter. When empty, the script auto-detects/installs.
    [string]$PythonExe = "",

    # Force a fresh cargo build of the server + worker even if the exes exist.
    [switch]$Rebuild,

    # Wipe the stack dir (config + embedded PG data) and re-bootstrap from scratch.
    [switch]$Fresh,

    # Leave the control plane + worker running after the bench (for inspection).
    [switch]$KeepStack
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
$BenchDir = (Get-Item $PSScriptRoot).Parent.FullName          # delta-forge-benchmarks
$RepoRoot = (Get-Item $BenchDir).Parent.FullName              # repo root
$TargetRel = Join-Path $RepoRoot "target\release"
$ServerExe = Join-Path $TargetRel "delta-forge-server.exe"
$WorkerExe = Join-Path $TargetRel "delta-forge-compute.exe"
$CliExe    = Join-Path $TargetRel "delta-forge-cli.exe"

$LogDir    = Join-Path $StackDir "logs"
$InputsToml = Join-Path $StackDir "bench-inputs.toml"
$ResultsDir = Join-Path $BenchDir "results"

$ControlBase = "http://127.0.0.1:$ControlPort"
$ControlHealth = "$ControlBase/api/v1/health"
$WorkerHealth  = "http://127.0.0.1:$WorkerPort/health"

# Track background processes for teardown.
$script:BgProcs = @()

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
function Write-Step([string]$msg) {
    Write-Host ""
    Write-Host "==> $msg" -ForegroundColor Cyan
}
function Write-Info([string]$msg) { Write-Host "    $msg" -ForegroundColor DarkGray }
function Write-Ok([string]$msg)   { Write-Host "    $msg" -ForegroundColor Green }
function Die([string]$msg) {
    Write-Host ""
    Write-Host "ERROR: $msg" -ForegroundColor Red
    throw $msg
}

function Test-PortFree([int]$port) {
    try {
        $c = Get-NetTCPConnection -LocalPort $port -State Listen -ErrorAction Stop
        return ($null -eq $c)
    } catch {
        return $true   # nothing listening
    }
}

function Wait-Http {
    param([string]$Url, [int]$TimeoutSec = 180, [string]$Name = "endpoint")
    $deadline = (Get-Date).AddSeconds($TimeoutSec)
    while ((Get-Date) -lt $deadline) {
        try {
            $r = Invoke-WebRequest -Uri $Url -TimeoutSec 5 -UseBasicParsing -ErrorAction Stop
            if ($r.StatusCode -ge 200 -and $r.StatusCode -lt 500) {
                Write-Ok "$Name healthy ($Url)"
                return $true
            }
        } catch {
            # not up yet
        }
        Start-Sleep -Milliseconds 1000
    }
    return $false
}

function Start-Background {
    param([string]$FilePath, [string[]]$ArgList, [string]$LogBase)
    $out = "$LogBase.out.log"
    $err = "$LogBase.err.log"
    $p = Start-Process -FilePath $FilePath -ArgumentList $ArgList -PassThru `
        -RedirectStandardOutput $out -RedirectStandardError $err -WindowStyle Hidden
    $script:BgProcs += $p
    return $p
}

function Stop-Tree([System.Diagnostics.Process]$proc) {
    if ($null -eq $proc) { return }
    try {
        if (-not $proc.HasExited) {
            # Kill the whole tree; the server spawns the embedded postgres child.
            & taskkill.exe /PID $proc.Id /T /F 2>$null | Out-Null
        }
    } catch { }
}

function Resolve-Python {
    param([string]$Explicit)

    function Test-RealPython([string]$exe) {
        if ([string]::IsNullOrWhiteSpace($exe)) { return $false }
        try {
            # The Windows Store stub lives under WindowsApps and prints a
            # "not found" notice instead of a version. Reject it.
            $real = (& $exe -c "import sys; print(sys.executable)" 2>$null)
            if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($real)) { return $false }
            if ($real -like "*WindowsApps*") { return $false }
            return $true
        } catch { return $false }
    }

    if (Test-RealPython $Explicit) { return $Explicit }

    foreach ($cand in @("python", "python3", "py")) {
        $src = (Get-Command $cand -ErrorAction SilentlyContinue)
        if ($src) {
            $exe = $src.Source
            if ($cand -eq "py") { $exe = "py" }   # launcher
            if (Test-RealPython $exe) { return $exe }
        }
    }

    # Common per-user install locations.
    $globs = @(
        (Join-Path $env:LOCALAPPDATA "Programs\Python\Python3*\python.exe"),
        "C:\Python3*\python.exe"
    )
    foreach ($g in $globs) {
        $hit = Get-ChildItem $g -ErrorAction SilentlyContinue | Sort-Object FullName -Descending | Select-Object -First 1
        if ($hit -and (Test-RealPython $hit.FullName)) { return $hit.FullName }
    }

    # Last resort: install via winget (quiet, per-user).
    if (Get-Command winget -ErrorAction SilentlyContinue) {
        Write-Info "No real Python found; installing Python 3.12 via winget (one-time)..."
        try {
            & winget install -e --id Python.Python.3.12 --silent --scope user `
                --accept-package-agreements --accept-source-agreements | Out-Null
        } catch {
            Write-Info "winget install reported: $($_.Exception.Message)"
        }
        $hit = Get-ChildItem (Join-Path $env:LOCALAPPDATA "Programs\Python\Python3*\python.exe") -ErrorAction SilentlyContinue |
            Sort-Object FullName -Descending | Select-Object -First 1
        if ($hit -and (Test-RealPython $hit.FullName)) { return $hit.FullName }
    }

    return $null
}

# On any terminating error after we start spawning processes, stop the stack so
# the next run starts from clean ports. Logs under $LogDir are preserved.
trap {
    Write-Host ""
    Write-Host "FATAL: $($_.Exception.Message)" -ForegroundColor Red
    foreach ($p in $script:BgProcs) { Stop-Tree $p }
    try {
        Get-Process postgres -ErrorAction SilentlyContinue |
            Where-Object { $_.Path -and $_.Path.StartsWith($StackDir, [System.StringComparison]::OrdinalIgnoreCase) } |
            ForEach-Object { Stop-Process -Id $_.Id -Force -ErrorAction SilentlyContinue }
    } catch { }
    break
}

# ---------------------------------------------------------------------------
# 0. Preconditions
# ---------------------------------------------------------------------------
Write-Step "Preconditions"

if ([string]::IsNullOrWhiteSpace($LicenseKey)) {
    Die "No license key. Set `$env:DELTA_FORGE_LICENSE_KEY or pass -LicenseKey. Free key: https://console.deltaforge.org"
}
if (-not (Get-Command cargo -ErrorAction SilentlyContinue)) {
    Die "cargo not on PATH. Install the Rust toolchain first."
}
foreach ($p in @($ControlPort, $WorkerPort, $PgPort)) {
    if (-not (Test-PortFree $p)) {
        Die "Port $p is already in use. Pick free ports via -ControlPort/-WorkerPort/-PgPort (the running GUI uses :3000 and :5432)."
    }
}
Write-Ok "license present, cargo found, ports $ControlPort/$WorkerPort/$PgPort free"
Write-Info "repo:   $RepoRoot"
Write-Info "bench:  $BenchDir"
Write-Info "stack:  $StackDir"

if ($Fresh -and (Test-Path $StackDir)) {
    Write-Info "-Fresh: removing existing stack dir"
    Remove-Item $StackDir -Recurse -Force
}
New-Item -ItemType Directory -Force -Path $StackDir, $LogDir | Out-Null

# ---------------------------------------------------------------------------
# 1. Build server + worker
# ---------------------------------------------------------------------------
Write-Step "Build (release)"
Push-Location $RepoRoot
try {
    if ($Rebuild -or -not (Test-Path $ServerExe)) {
        Write-Info "cargo build delta-forge-server (--features embedded-pg)"
        & cargo build --release -p delta-forge-control --bin delta-forge-server --features embedded-pg
        if ($LASTEXITCODE -ne 0) { Die "server build failed" }
    } else { Write-Info "server exe present (skip; -Rebuild to force)" }

    if ($Rebuild -or -not (Test-Path $WorkerExe)) {
        Write-Info "cargo build delta-forge-compute (--features gpu)"
        & cargo build --release -p delta-forge-compute --bin delta-forge-compute --features gpu
        if ($LASTEXITCODE -ne 0) { Die "worker build failed" }
    } else { Write-Info "worker exe present (skip; -Rebuild to force)" }

    if (-not (Test-Path $CliExe)) {
        Write-Info "cargo build delta-forge-cli"
        & cargo build --release -p delta-forge-cli --bin delta-forge-cli
        if ($LASTEXITCODE -ne 0) { Die "cli build failed" }
    } else { Write-Info "cli exe present" }
} finally {
    Pop-Location
}
Write-Ok "server: $ServerExe"
Write-Ok "worker: $WorkerExe"
Write-Ok "cli:    $CliExe"

# The control-plane config dir + embedded PG port are wired through env so both
# the bootstrap pass and the serve pass agree.
$env:DELTA_FORGE_CONFIG_DIR = $StackDir
$env:DELTAFORGE_PG_PORT     = "$PgPort"
$env:DELTA_FORGE_LICENSE_KEY = $LicenseKey
$env:RUST_LOG = "info"

$configToml = Join-Path $StackDir "config.toml"

# ---------------------------------------------------------------------------
# 2. Headless bootstrap (embedded PG) via --from-file
# ---------------------------------------------------------------------------
Write-Step "Headless bootstrap (embedded PostgreSQL)"
if ((Test-Path $configToml) -and -not $Fresh) {
    Write-Info "config.toml already present; skipping bootstrap (use -Fresh to redo)"
} else {
    # db_data_dir's PARENT is what the embedded module uses to site pg-install/
    # pg-data (see delta-forge-bootstrap::embedded::ManagedPgParams::from_config_dir),
    # so a child path under the stack dir keeps everything self-contained.
    $pgDataMarker = ($StackDir -replace '\\','/') + "/postgres"
    $bindAddr = "127.0.0.1:$ControlPort"
    $tomlEsc = {
        param($s) $s -replace '\\','\\' -replace '"','\"'
    }
    $inputs = @"
# Generated by run-graph-gpu-bench.ps1. Contains secrets; lives under A:\tmp,
# never committed. Safe to delete after the run.
console_url       = "https://console.deltaforge.org"
license_key       = "$(& $tomlEsc $LicenseKey)"
db_data_dir       = "$pgDataMarker"
admin_email       = "$AdminEmail"
admin_password    = "$(& $tomlEsc $AdminPassword)"
engineer_email    = "$EngineerEmail"
engineer_password = "$(& $tomlEsc $EngineerPassword)"
bind_addr         = "$bindAddr"
activate_on_boot  = false
"@
    Set-Content -Path $InputsToml -Value $inputs -Encoding UTF8
    Write-Info "wrote $InputsToml (embedded PG on :$PgPort, control plane bind $bindAddr)"
    Write-Info "running bootstrap (first run downloads ~50 MB PostgreSQL binaries)..."

    & $ServerExe --config-dir $StackDir bootstrap --from-file $InputsToml 2>&1 |
        Tee-Object -FilePath (Join-Path $LogDir "bootstrap.log")
    if ($LASTEXITCODE -ne 0) { Die "bootstrap failed (see $LogDir\bootstrap.log)" }

    if (-not (Test-Path $configToml)) {
        # Fall back to locating it if the dir resolution differs.
        $found = Get-ChildItem $StackDir -Recurse -Filter config.toml -ErrorAction SilentlyContinue | Select-Object -First 1
        if ($found) { $configToml = $found.FullName } else { Die "bootstrap did not write config.toml under $StackDir" }
    }
    Write-Ok "bootstrap complete: $configToml"
}

# ---------------------------------------------------------------------------
# 2b. Patch the hardcoded embedded PG port (orchestrator writes port = 5432).
# ---------------------------------------------------------------------------
$cfg = Get-Content $configToml -Raw
if ($cfg -match '(?m)^\s*port\s*=\s*5432\s*$' -and $PgPort -ne 5432) {
    $cfg = [regex]::Replace($cfg, '(?m)^(\s*port\s*=\s*)5432(\s*)$', "`${1}$PgPort`$2")
    Set-Content -Path $configToml -Value $cfg -Encoding UTF8
    Write-Info "patched [database] port 5432 -> $PgPort in config.toml"
}

# ---------------------------------------------------------------------------
# 3. Start control plane (serve) + worker
# ---------------------------------------------------------------------------
Write-Step "Start control plane + GPU worker"

$server = Start-Background -FilePath $ServerExe -ArgList @("--config-dir", $StackDir) `
    -LogBase (Join-Path $LogDir "control")
Write-Info "control plane pid=$($server.Id), waiting for $ControlHealth ..."
if (-not (Wait-Http -Url $ControlHealth -TimeoutSec 240 -Name "control plane")) {
    Die "control plane did not become healthy. See $LogDir\control.err.log"
}

# Worker env (inherited by the child). NODE_SECRET is set by the first
# registration of this node_id; any value is accepted on a fresh control plane.
$env:CONTROL_PLANE_URL = $ControlBase
$env:NODE_ID    = "bench-gpu-worker"
$env:NODE_SECRET = "bench-node-secret"
$env:BIND_HOST  = "127.0.0.1"
$env:BIND_PORT  = "$WorkerPort"
$env:RUST_LOG   = "info,delta_forge_compute=info"

$worker = Start-Background -FilePath $WorkerExe -ArgList @() `
    -LogBase (Join-Path $LogDir "worker")
Write-Info "worker pid=$($worker.Id), waiting for $WorkerHealth ..."
if (-not (Wait-Http -Url $WorkerHealth -TimeoutSec 120 -Name "worker")) {
    Die "worker did not become healthy. See $LogDir\worker.err.log"
}

# Sanity: a trivial query end-to-end through the freshly bootstrapped stack.
Write-Info "probe: SELECT 1 through the CLI"
$env:DF_CONTROL_URL = $ControlBase
$env:DF_USERNAME = $AdminEmail
$env:DF_PASSWORD = $AdminPassword
& $CliExe --format json query "SELECT 1 AS ok" 2>&1 | Out-Null
if ($LASTEXITCODE -ne 0) { Die "SELECT 1 probe failed; the CLI cannot reach the stack" }
Write-Ok "stack is live and serving queries"

# ---------------------------------------------------------------------------
# 4. Resolve Python + run the canonical harness
# ---------------------------------------------------------------------------
Write-Step "Run benchmark harness (graph_gpu_vs_cpu)"
$py = Resolve-Python -Explicit $PythonExe
if ([string]::IsNullOrWhiteSpace($py)) {
    Die "No usable Python interpreter. Install Python 3 (https://python.org) or pass -PythonExe <path>."
}
Write-Info "python: $py"
Write-Info "ensuring psutil is available"
& $py -m pip install --quiet --disable-pip-version-check psutil
if ($LASTEXITCODE -ne 0) { Die "failed to install psutil (required by the harness metrics sampler)" }

# Harness wiring (engines/df_engine.py reads these).
$env:DF_CLI_PATH        = $CliExe
$env:DF_CONTROL_URL     = $ControlBase
$env:DF_USERNAME        = $AdminEmail
$env:DF_PASSWORD        = $AdminPassword
$env:GRAPH_BENCH_SIZES_M = $SizesM
$env:GRAPH_BENCH_WARM   = "$Warm"
$env:DF_HTTP_TIMEOUT_SECS = "0"

Write-Info "GRAPH_BENCH_SIZES_M=$SizesM  GRAPH_BENCH_WARM=$Warm"
Write-Info "building the graph + running CPU vs GPU for each algorithm (this is the long part)"

Push-Location $BenchDir
try {
    & $py "bench_runner.py" --scale 1 --engines df --workloads graph_gpu_vs_cpu `
        --no-purge --results-dir $ResultsDir --tag graph-gpu
    if ($LASTEXITCODE -ne 0) { Die "bench_runner.py exited non-zero" }
} finally {
    Pop-Location
}

# ---------------------------------------------------------------------------
# 5. Report
# ---------------------------------------------------------------------------
Write-Step "Crossover report"
$latest = Get-ChildItem $ResultsDir -Directory -ErrorAction SilentlyContinue |
    Sort-Object LastWriteTime -Descending | Select-Object -First 1
if (-not $latest) { Die "no results directory produced under $ResultsDir" }
Write-Info "results: $($latest.FullName)"

$reportOut = Join-Path $BenchDir "published\graph_gpu.md"
Push-Location $BenchDir
try {
    & $py "reports/build_graph_gpu_report.py" --results-dir $latest.FullName --out $reportOut
    if ($LASTEXITCODE -ne 0) { Write-Info "report generator exited non-zero; raw df.jsonl is still at $($latest.FullName)\raw\df.jsonl" }
} finally {
    Pop-Location
}

if (Test-Path $reportOut) {
    Write-Host ""
    Write-Host "----- $reportOut -----" -ForegroundColor Yellow
    Get-Content $reportOut
    Write-Host "----------------------------------------" -ForegroundColor Yellow
}

Write-Ok "done. raw records: $($latest.FullName)\raw\df.jsonl"

# ---------------------------------------------------------------------------
# 6. Teardown
# ---------------------------------------------------------------------------
if ($KeepStack) {
    Write-Step "Stack left running (-KeepStack)"
    Write-Info "control plane: $ControlBase  (pid $($server.Id))"
    Write-Info "worker:        $WorkerHealth (pid $($worker.Id))"
    Write-Info "stop later with: Get-Process delta-forge-server,delta-forge-compute | Stop-Process -Force"
} else {
    Write-Step "Teardown"
    Stop-Tree $worker
    Stop-Tree $server
    # The server spawns the embedded postgres as a child; the tree kill above
    # handles it, but sweep any stragglers rooted in the stack dir to be sure.
    Get-Process postgres -ErrorAction SilentlyContinue |
        Where-Object { $_.Path -and $_.Path.StartsWith($StackDir, [System.StringComparison]::OrdinalIgnoreCase) } |
        ForEach-Object { Stop-Process -Id $_.Id -Force -ErrorAction SilentlyContinue }
    Write-Ok "control plane + worker + embedded PG stopped"
}
