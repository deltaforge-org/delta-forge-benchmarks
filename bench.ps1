<#
.SYNOPSIS
    DeltaForge benchmark launcher (Windows).

.DESCRIPTION
    Boots the DeltaForge platform (embedded control plane + compute node), waits
    until it is healthy, runs the bench harness against it (plus DuckDB / Spark
    comparison engines), and tears the platform down on exit.

    Run install.ps1 first; it writes the .env this script reads.

.EXAMPLE
    .\bench.ps1
    .\bench.ps1 -Scale 10 -Engines df,duckdb
    .\bench.ps1 -Extra '--scale','10','--workloads','tpch_read_delta'

    -Scale / -Engines / -Workloads are convenience flags. Anything in -Extra is
    forwarded verbatim to bench_runner.py and overrides the convenience flags.
#>
[CmdletBinding()]
param(
    [int]$Scale,
    [string]$Engines,
    [string]$Workloads,
    [string[]]$Extra
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
$RepoRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $RepoRoot

function Log  ($m) { Write-Host "[bench] $m" -ForegroundColor Cyan }
function Ok   ($m) { Write-Host "[bench] OK $m" -ForegroundColor Green }
function Warn ($m) { Write-Host "[bench] WARN $m" -ForegroundColor Yellow }
function Fail {
    param([string]$Message, [string[]]$Hints)
    Write-Host "`n[bench] X $Message" -ForegroundColor Red
    foreach ($h in $Hints) { Write-Host "  -> $h" -ForegroundColor Yellow }
    exit 1
}

# Friendly guidance when the license is missing or rejected by the engine.
function License-Help {
    Write-Host "`n[bench] DeltaForge needs a valid license key to run the engine." -ForegroundColor Yellow
    Write-Host "  The benchmark does not bundle a key. Either none is set in .env, or the one"
    Write-Host "  configured was rejected (expired, wrong, or out of daily compute)."
    Write-Host "`n  Fix (1 minute, free, no credit card):"
    Write-Host "    1. Get a free key at https://console.deltaforge.org"
    Write-Host "    2. Put it in .env:   DELTA_FORGE_LICENSE_KEY=<your-key>"
    Write-Host "       (or re-run:  `$env:DELTA_FORGE_LICENSE_KEY='<your-key>'; .\install.ps1)"
    Write-Host "    3. .\bench.ps1"
}

# Heuristic: does this text look like a license/quota rejection (vs auth/network)?
function Looks-Like-License-Limit($text) {
    return ($text -match '(?i)licen|quota|exhaust|dfcu|compute unit|usage cap|limit exceeded|over.?limit|rate.?limit|payment required|\b4(02|29)\b')
}

# ----- environment -----------------------------------------------------------

if (-not (Test-Path '.env')) { Fail "no .env found. Run .\install.ps1 first." }
foreach ($line in Get-Content '.env') {
    if ($line -match '^\s*#' -or $line -notmatch '=') { continue }
    $k, $v = $line -split '=', 2
    Set-Item -Path "Env:$($k.Trim())" -Value $v.Trim()
}

$VenvPy = Join-Path $RepoRoot '.venv\Scripts\python.exe'
if (-not (Test-Path $VenvPy)) { Fail "no .venv found. Run .\install.ps1 first." }
if (-not $env:DF_PLATFORM_BIN -or -not (Test-Path $env:DF_PLATFORM_BIN)) { Fail "platform binary missing; re-run install.ps1" }
if (-not $env:DF_CLI_PATH -or -not (Test-Path $env:DF_CLI_PATH)) { Fail "cli binary missing; re-run install.ps1" }

# DeltaForge needs a license key to run the engine; the benchmark bundles none.
# Fail fast here with guidance instead of booting the whole platform only to have
# every query rejected. A key that is present but expired/invalid still gets the
# final verdict from the engine at the license probe further below.
if (-not $env:DELTA_FORGE_LICENSE_KEY) {
    License-Help
    Fail "No license key configured (DELTA_FORGE_LICENSE_KEY is empty in .env)." `
         @("DeltaForge cannot run the engine without one; see the steps above.")
}

New-Item -ItemType Directory -Force -Path 'logs' | Out-Null
$PlatformLog = Join-Path $RepoRoot 'logs\platform.log'
$PlatformErr = "$PlatformLog.err"

# Control-plane port from DELTA_FORGE_BIND_ADDR (host:port), default 3000.
$port = 3000
if ($env:DELTA_FORGE_BIND_ADDR -and $env:DELTA_FORGE_BIND_ADDR -match ':(\d+)$') { $port = [int]$Matches[1] }
try {
    $l = New-Object System.Net.Sockets.TcpListener ([System.Net.IPAddress]::Loopback, $port)
    $l.Start(); $l.Stop()
} catch {
    Fail "Port $port is already in use." "Stop any running DeltaForge, or change DELTA_FORGE_BIND_ADDR and DF_CONTROL_URL in .env."
}

# ----- platform lifecycle ----------------------------------------------------

$Platform = $null
function Stop-Platform {
    if ($script:Platform -and -not $script:Platform.HasExited) {
        try { $script:Platform.Kill() } catch { }
    }
}

# Scan the platform log for the failure modes a first-time user is most likely
# to hit, and explain them in plain language.
function Diagnose-StartupFailure {
    $text = ''
    foreach ($f in @($PlatformLog, $PlatformErr)) { if (Test-Path $f) { $text += (Get-Content $f -Raw) } }
    if ($text -match '(?im)license|activat|exhaust|quota|expired|seat|node limit|max_(nodes|cores|users)') {
        License-Help
    }
    if ($text -match '(?im)address already in use|EADDRINUSE|bind') {
        Warn "the platform could not bind its port. Another instance may be running; free port $port and retry."
    }
}

try {
    Log "starting DeltaForge platform: $($env:DF_PLATFORM_BIN)"
    $Platform = Start-Process -FilePath $env:DF_PLATFORM_BIN -PassThru `
        -RedirectStandardOutput $PlatformLog -RedirectStandardError "$PlatformLog.err" -WindowStyle Hidden

    Log "waiting for the control plane to become healthy (up to 180s)"
    $deadline = (Get-Date).AddSeconds(180)
    $healthy = $false
    while ((Get-Date) -lt $deadline) {
        if ($Platform.HasExited) {
            if (Test-Path $PlatformLog) { Get-Content $PlatformLog -Tail 40 | Write-Host }
            Diagnose-StartupFailure
            Fail "The platform exited during startup. Full log: $PlatformLog"
        }
        & $env:DF_CLI_PATH health *> $null
        if ($LASTEXITCODE -eq 0) { $healthy = $true; break }
        Start-Sleep -Seconds 2
    }
    if (-not $healthy) {
        if (Test-Path $PlatformLog) { Get-Content $PlatformLog -Tail 40 | Write-Host }
        Diagnose-StartupFailure
        Fail "The platform did not become healthy within 180s. Full log: $PlatformLog"
    }
    Ok "platform healthy"

    # Early license check: a healthy control plane does not mean the license
    # still has daily capacity. One tiny query now turns a tapped license into a
    # fast, friendly failure instead of 22+ failing workload queries. The CLI
    # prints errors to stdout and still exits 0, so inspect the OUTPUT.
    Log "checking benchmark license capacity"
    $probe = (& $env:DF_CLI_PATH --format json query "SELECT 1 AS ok" 2>&1 | Out-String)
    if (Looks-Like-License-Limit $probe) {
        License-Help
        Fail "Benchmark license check failed before running (looks like the shared daily cap)." `
             @("Platform said: " + (($probe -replace '\s+', ' ').Trim().Substring(0, [Math]::Min(200, ($probe -replace '\s+', ' ').Trim().Length))))
    } elseif ($probe -notmatch '"row_count"|"rows"') {
        Write-Host ($probe | Select-Object -First 8)
        Fail "A probe query did not return a result (not a license issue)." `
             @("Check DF_USERNAME/DF_PASSWORD in .env and the platform log: $PlatformLog")
    }
    Ok "license OK — platform is serving queries"

    # ----- assemble harness args ---------------------------------------------

    if ($Extra -and $Extra.Count -gt 0) {
        $runArgs = $Extra
    } else {
        if ($Scale -gt 0) { $scaleVal = "$Scale" } else { $scaleVal = '1' }
        if ($Engines)   { $enginesVal = $Engines }     else { $enginesVal = 'df,duckdb,spark-default,spark-tuned' }
        if ($Workloads) { $workloadsVal = $Workloads } else { $workloadsVal = 'tpch_read_delta,tpcds_read_delta,ssb_read_delta,job_read_delta,synthetic_write_delta' }
        $runArgs = @('--scale', $scaleVal, '--engines', $enginesVal, '--workloads', $workloadsVal)
    }

    Log "running: bench_runner.py $($runArgs -join ' ')"
    & $VenvPy 'bench_runner.py' @runArgs
    $rc = $LASTEXITCODE
    Write-Host ""
    if ($rc -eq 0) { Ok "benchmark complete" } else { Warn "bench_runner exited with code $rc (some steps may have failed; see the report)" }
    Log "platform log: $PlatformLog"
    exit $rc
}
finally {
    Stop-Platform
}
