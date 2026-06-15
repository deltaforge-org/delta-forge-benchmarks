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

# Distinguish "key installed but NOT ACTIVATED" (HTTP 403 NOT_ACTIVATED) from a
# missing / rejected / exhausted key. The remedy is different: activate the
# device, do not fetch a new key. Checked BEFORE Looks-Like-License-Limit, which
# would otherwise swallow this (the text contains "licen").
function Looks-Like-Not-Activated($text) {
    return ($text -match '(?i)NOT_ACTIVATED|not activated|activate your license|license not activated')
}

# Guidance for the not-activated case.
function Activation-Help {
    Write-Host "`n[bench] The license could not be activated on this machine." -ForegroundColor Yellow
    Write-Host "  The benchmark auto-activates this device on boot, but the engine still"
    Write-Host "  reports the license as not activated. Likely causes:"
    Write-Host "    - No internet: activation is an online exchange with console.deltaforge.org."
    Write-Host "    - All device slots in use: a single-node license can be active on only ONE"
    Write-Host "      machine at a time. Deactivate it elsewhere, or use a license with more nodes."
    Write-Host "    - The key is expired, revoked, or wrong.`n"
    Write-Host "  Check your connection and the platform log, then re-run .\bench.ps1."
    Write-Host "  Manage your license at https://console.deltaforge.org"
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

# ----- headless, unattended platform environment -----------------------------
# Mirror the Linux `bench` contract so a fresh Windows box runs with no human in
# the loop: force HEADLESS bootstrap against the platform's built-in embedded
# PostgreSQL (no browser wizard), auto-activate the device, advertise the
# embedded compute node on loopback, and give the embedded PostgreSQL a free
# port so it never collides with a DeltaForge already on :5432. These are all
# read by the released platform binary from the environment; no rebuild, no
# source. Against a platform too old to support DELTA_FORGE_HEADLESS the boot
# falls back to the wizard and this run fails fast with a startup/license error
# instead of hanging on a window.
$env:DELTA_FORGE_HEADLESS = '1'
# Auto-activate this device on first boot so the benchmark runs on a machine
# that has never opened the DeltaForge GUI. Device-bound + idempotent: it takes
# the machine's ONE shared activation slot, or reuses an existing activation
# (GUI or a prior run) and consumes no extra slot. A single-node license can be
# active on only one machine at a time.
if (-not $env:DELTA_FORGE_ACTIVATE_ON_BOOT) { $env:DELTA_FORGE_ACTIVATE_ON_BOOT = '1' }
if (-not $env:DELTA_FORGE_SKIP_DEMO_USER)   { $env:DELTA_FORGE_SKIP_DEMO_USER   = 'false' }
# Advertise the embedded compute node on loopback. It binds 0.0.0.0:3031 and
# would otherwise advertise the host LAN IP for the catalog, which the local CLI
# cannot rely on reaching (laptops / CI / offline all break that route). The
# benchmark is single-node and local, so pin it to 127.0.0.1:3031.
if (-not $env:ADVERTISE_IP) { $env:ADVERTISE_IP = '127.0.0.1' }
# Free port for the embedded PostgreSQL so we never touch a DeltaForge already
# on :5432. Honors an explicit DELTAFORGE_PG_PORT if the operator set one.
if (-not $env:DELTAFORGE_PG_PORT) {
    $pgPort = 5442
    for ($p = 5442; $p -lt 5600; $p++) {
        try {
            $pgListener = New-Object System.Net.Sockets.TcpListener ([System.Net.IPAddress]::Loopback, $p)
            $pgListener.Start(); $pgListener.Stop(); $pgPort = $p; break
        } catch { continue }
    }
    $env:DELTAFORGE_PG_PORT = "$pgPort"
}
Log "embedded PostgreSQL: port $($env:DELTAFORGE_PG_PORT) (isolated from any DeltaForge on :5432)"
# NB: deliberately do NOT redirect USERPROFILE / HOME. License activation is
#     DEVICE-bound: the activation token (%USERPROFILE%\.deltaforge\activation.token)
#     and the machine instance id resolve from the user profile, and the console
#     counts ONE device slot per machine, so the GUI and the benchmark share one
#     activation. Hiding it makes the engine reject every query with HTTP 403
#     NOT_ACTIVATED (same rationale as the Linux launcher). Full catalog
#     isolation on Windows would need an engine-honored config-dir override (the
#     desktop build resolves its app dir from the OS, not an env var);
#     DELTAFORGE_PG_PORT already prevents the one collision that matters.

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
    # The platform's embedded PostgreSQL is a separate process that can outlive
    # the GUI. Stop only the one listening on OUR isolated bench port; a
    # DeltaForge PG the user runs elsewhere (default :5432) has a different port
    # and is left untouched.
    if ($env:DELTAFORGE_PG_PORT) {
        try {
            $conns = Get-NetTCPConnection -LocalPort ([int]$env:DELTAFORGE_PG_PORT) -State Listen -ErrorAction SilentlyContinue
            foreach ($c in $conns) {
                try { Stop-Process -Id $c.OwningProcess -Force -ErrorAction SilentlyContinue } catch { }
            }
        } catch { }
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

    # A healthy control plane does NOT mean the embedded compute node is ready,
    # that the device is activated, nor that the license still has daily
    # capacity. Poll a tiny query: it has to (a) authenticate, (b) reach the
    # freshly-spawned compute node (up a few seconds AFTER the control plane),
    # (c) wait for the on-boot device activation (DELTA_FORGE_ACTIVATE_ON_BOOT,
    # an async online exchange) to land, and (d) not hit the license cap.
    # NOT_ACTIVATED is therefore RETRYABLE (activation in flight); a rejected /
    # capped key is NOT (fail fast). The CLI prints errors to stdout and still
    # exits 0, so inspect the OUTPUT.
    Log "checking benchmark license capacity (waiting for compute to warm up)"
    $probeOut = ''; $probeOk = $false; $sawNotActivated = $false
    $probeDeadline = (Get-Date).AddSeconds(90)
    while ((Get-Date) -lt $probeDeadline) {
        if ($Platform.HasExited) {
            if (Test-Path $PlatformLog) { Get-Content $PlatformLog -Tail 30 | Write-Host }
            Diagnose-StartupFailure
            Fail "The platform exited while waiting for compute to become ready. Full log: $PlatformLog"
        }
        $probeOut = (& $env:DF_CLI_PATH --format json query "SELECT 1 AS ok" 2>&1 | Out-String)
        if (Looks-Like-Not-Activated $probeOut) {
            # Device activation is requested on boot and runs asynchronously, so
            # the first probes can race ahead of it. Keep polling; only a
            # NOT_ACTIVATED that survives the whole deadline is a real failure.
            $sawNotActivated = $true
            Start-Sleep -Seconds 2; continue
        }
        if (Looks-Like-License-Limit $probeOut) {
            License-Help
            $oneLine = ($probeOut -replace '\s+', ' ').Trim()
            Fail "Benchmark license check failed before running (daily cap, or the key was rejected)." `
                 @("Platform said: " + $oneLine.Substring(0, [Math]::Min(200, $oneLine.Length)))
        }
        if ($probeOut -match '"row_count"|"rows"') { $probeOk = $true; break }
        Start-Sleep -Seconds 2
    }
    if (-not $probeOk) {
        if ($sawNotActivated) {
            Activation-Help
            $oneLine = ($probeOut -replace '\s+', ' ').Trim()
            Fail "The license did not activate within 90s." `
                 @("Platform said: " + $oneLine.Substring(0, [Math]::Min(200, $oneLine.Length)))
        }
        Write-Host ($probeOut | Select-Object -First 8)
        Fail "A probe query did not return a result within 90s (compute did not come up, or auth failed)." `
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
