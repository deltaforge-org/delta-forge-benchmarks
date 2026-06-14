<#
.SYNOPSIS
    DeltaForge benchmark: one-command installer (Windows).

.DESCRIPTION
    A public, platform-agnostic proof of DeltaForge performance and correctness.
    This single script takes a fresh Windows PC to a runnable benchmark using
    ONLY official, signed release artifacts from deltaforge-org. Nothing is
    built from source.

        irm https://deltaforge.org/bench/install.ps1 | iex

    It installs exactly two DeltaForge artifacts plus the harness around them:
      1. deltaforge        - the platform (embeds the compute node + control plane)
      2. deltaforge-cli     - the SQL client the harness drives
      3. the bench harness   - this repo (cloned if you piped the script in)
      4. a Python venv       - DuckDB + Spark comparison engines and reporting
      5. a pinned JRE         - only if Java is absent (Spark needs a JVM)

    No separate compute node, no Postgres, no Docker: the platform bootstraps an
    embedded database and an embedded compute node in-process.

    Every step explains itself and every failure tells you what to do next.

.PARAMETER Version      Engine version (default: latest release).
.PARAMETER LicenseKey   Required license key (else you are prompted for one).
.PARAMETER SkipSpark    Do not provision Java / Spark (df + DuckDB only).
.PARAMETER SkipRun      Install only; do not offer a smoke run.
.PARAMETER AssumeYes    Non-interactive; skip the post-install smoke prompt.
#>
[CmdletBinding()]
param(
    [string]$Version    = $env:DF_VERSION,
    [string]$LicenseKey = $env:DELTA_FORGE_LICENSE_KEY,
    [switch]$SkipSpark,
    [switch]$SkipRun,
    [switch]$AssumeYes
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

# ---------------------------------------------------------------------------
# Constants + presentation
# ---------------------------------------------------------------------------

$RepoOwner   = 'deltaforge-org'
$EngineRepo  = 'delta-forge'
$BenchRepo   = 'delta-forge-benchmarks'
$ReleaseBase = "https://github.com/$RepoOwner/$EngineRepo/releases"
$ApiLatest   = "https://api.github.com/repos/$RepoOwner/$EngineRepo/releases/latest"
$BenchGitUrl = "https://github.com/$RepoOwner/$BenchRepo.git"
$IssuesUrl   = "https://github.com/$RepoOwner/$BenchRepo/issues"
$MinDiskMb   = 4096
$MinRamMb    = 8192

# DeltaForge requires a license key to run the engine; the benchmark does NOT
# bundle one. Each user supplies their own (a free key takes a minute at the
# console). Collected in step 9 from -LicenseKey / DELTA_FORGE_LICENSE_KEY or an
# interactive prompt, then written into .env.
$ConsoleUrl = 'https://console.deltaforge.org'

$script:StepN = 0
$script:StepTotal = 9
function Step($m) { $script:StepN++; Write-Host "`n[$($script:StepN)/$($script:StepTotal)] $m" -ForegroundColor Cyan }
function Info($m) { Write-Host "      $m" }
function Ok($m)   { Write-Host "      " -NoNewline; Write-Host "OK " -ForegroundColor Green -NoNewline; Write-Host $m }
function Warn($m) { Write-Host "      ! $m" -ForegroundColor Yellow }
function Fail {
    param([string]$Message, [string[]]$Hints)
    Write-Host "`nX $Message" -ForegroundColor Red
    foreach ($h in $Hints) { Write-Host "  -> $h" -ForegroundColor Yellow }
    Write-Host "`n  Need help? $IssuesUrl" -ForegroundColor DarkGray
    exit 1
}

function Banner {
    Write-Host @"

  ____       _ _        _____
 |  _ \  ___| | |_ __ _|  ___|__  _ __ __ _  ___
 | | | |/ _ \ | __/ _`` | |_ / _ \| '__/ _`` |/ _ \
 | |_| |  __/ | || (_| |  _| (_) | | | (_| |  __/
 |____/ \___|_|\__\__,_|_|  \___/|_|  \__, |\___|
                                      |___/  benchmark
"@ -ForegroundColor White
    Write-Host "  A public proof of DeltaForge performance and correctness." -ForegroundColor DarkGray
    Write-Host "  Official signed releases only. Nothing built from source." -ForegroundColor DarkGray
}

Banner

# ---------------------------------------------------------------------------
# Step 1: Detect this machine
# ---------------------------------------------------------------------------

Step "Checking your machine"
if ($env:PROCESSOR_ARCHITECTURE -ne 'AMD64') {
    Fail "Unsupported CPU architecture for Windows: $($env:PROCESSOR_ARCHITECTURE)" `
         @("DeltaForge publishes a Windows x64 build only.")
}
Ok "Windows on x64"
$Plat = 'windows-x64'

# ---------------------------------------------------------------------------
# Step 2: Required tools
# ---------------------------------------------------------------------------

Step "Checking required tools"
if (-not (Get-Command python -ErrorAction SilentlyContinue)) {
    Fail "Python was not found on PATH." `
         @("Install Python 3.9+ from https://www.python.org/downloads/ (tick 'Add python.exe to PATH'),",
           "or:  winget install Python.Python.3.12")
}
$pyv = (python -c "import sys;print('%d.%d'%sys.version_info[:2])") 2>$null
$pyOk = (python -c "import sys;print(1 if sys.version_info[:2]>=(3,9) else 0)") 2>$null
if ($pyOk -ne '1') { Fail "Python 3.9 or newer is required (found $pyv)." @("Update Python from https://www.python.org/downloads/") }
Ok "python $pyv"

# ---------------------------------------------------------------------------
# Step 3: Network
# ---------------------------------------------------------------------------

Step "Checking network access to the official release host"
try {
    Invoke-WebRequest -Uri 'https://github.com' -Method Head -TimeoutSec 15 -UseBasicParsing | Out-Null
    Ok "github.com reachable"
} catch {
    Fail "Cannot reach github.com to download the official release artifacts." `
         @("Check your internet connection, VPN, or corporate proxy and try again.",
           "Behind a proxy? Set `$env:HTTPS_PROXY = 'http://host:port' before re-running.")
}

# ---------------------------------------------------------------------------
# Step 4: Resolve version + locate harness
# ---------------------------------------------------------------------------

Step "Resolving the DeltaForge release to install"
if ([string]::IsNullOrWhiteSpace($Version)) {
    try {
        $rel = Invoke-RestMethod -Uri $ApiLatest -Headers @{ 'User-Agent' = 'deltaforge-bench-install' } -TimeoutSec 20
        $Version = $rel.tag_name -replace '^v', ''
    } catch { }
    if ([string]::IsNullOrWhiteSpace($Version)) {
        Fail "Could not determine the latest release version." `
             @("GitHub may be rate-limiting unauthenticated calls.", "Pin one:  -Version 1.0.5")
    }
}
Ok "engine version $Version"
$DlBase        = "$ReleaseBase/download/v$Version"
$PlatformAsset = "deltaforge-$Version-$Plat.msi"
$CliAsset      = "deltaforge-cli-$Version-$Plat.zip"

if ((Test-Path 'bench_runner.py') -and (Test-Path 'engines')) {
    $BenchHome = (Get-Location).Path
    Info "using the harness in the current directory"
} elseif ($env:BENCH_HOME -and (Test-Path (Join-Path $env:BENCH_HOME 'bench_runner.py'))) {
    $BenchHome = (Resolve-Path $env:BENCH_HOME).Path
} else {
    if (-not (Get-Command git -ErrorAction SilentlyContinue)) {
        Fail "git is required to fetch the bench harness." @("Install:  winget install Git.Git")
    }
    if ($env:BENCH_HOME) { $BenchHome = $env:BENCH_HOME } else { $BenchHome = Join-Path (Get-Location).Path $BenchRepo }
    if (-not (Test-Path (Join-Path $BenchHome '.git'))) {
        Info "cloning the bench harness into $BenchHome"
        git clone --depth 1 $BenchGitUrl $BenchHome 2>$null
        if ($LASTEXITCODE -ne 0) { Fail "Failed to clone $BenchGitUrl" }
    }
}
Set-Location $BenchHome
Ok "bench home: $BenchHome"
if ($env:DF_PREFIX) { $DfPrefix = $env:DF_PREFIX } else { $DfPrefix = Join-Path $BenchHome '.engine' }
$BinDir = Join-Path $DfPrefix 'bin'; $CacheDir = Join-Path $DfPrefix 'cache'
New-Item -ItemType Directory -Force -Path $BinDir, $CacheDir | Out-Null

# ---------------------------------------------------------------------------
# Step 5: Disk + memory + port headroom
# ---------------------------------------------------------------------------

Step "Checking disk and memory headroom"
try {
    $drive = New-Object System.IO.DriveInfo((Split-Path -Qualifier $BenchHome))
    $freeMb = [int]($drive.AvailableFreeSpace / 1MB)
    if ($freeMb -lt $MinDiskMb) {
        Warn "only $freeMb MB free; ~$MinDiskMb MB recommended for binaries + SF=1 data (SF=10 needs ~40 GB)."
    } else { Ok "disk: $freeMb MB free" }
} catch { Info "could not measure free disk space; continuing." }
try {
    $ramMb = [int]((Get-CimInstance Win32_ComputerSystem).TotalPhysicalMemory / 1MB)
    if ($ramMb -lt $MinRamMb) { Warn "only $ramMb MB RAM; $MinRamMb MB recommended. Spark may struggle; df and DuckDB are fine." }
    else { Ok "memory: $ramMb MB" }
} catch { Info "could not measure RAM; continuing." }
$portFree = $true
try {
    $l = New-Object System.Net.Sockets.TcpListener ([System.Net.IPAddress]::Loopback, 3000)
    $l.Start(); $l.Stop()
} catch { $portFree = $false }
if ($portFree) { Ok "control-plane port 3000 is free" }
else { Warn "port 3000 is already in use. Stop any running DeltaForge, or change DELTA_FORGE_BIND_ADDR/DF_CONTROL_URL in .env after install." }

# ---------------------------------------------------------------------------
# Step 6: Download + verify
# ---------------------------------------------------------------------------

Step "Downloading and verifying official release artifacts"
$SumsFile = Join-Path $CacheDir 'SHA256SUMS'
try { Invoke-WebRequest -Uri "$DlBase/SHA256SUMS" -OutFile $SumsFile -UseBasicParsing }
catch { Fail "Could not download SHA256SUMS for v$Version." @("Does this version exist? See $ReleaseBase", "Pin a known-good one:  -Version 1.0.5") }
Ok "fetched checksum manifest"

$Sums = @{}
foreach ($line in Get-Content $SumsFile) {
    if ($line -match '^\s*([0-9a-fA-F]{64})\s+\*?(.+?)\s*$') { $Sums[$Matches[2]] = $Matches[1].ToLower() }
}
function Get-Verified($asset) {
    $dest = Join-Path $CacheDir $asset
    if (-not (Test-Path $dest)) {
        try { Invoke-WebRequest -Uri "$DlBase/$asset" -OutFile $dest -UseBasicParsing }
        catch { Fail "Download failed: $asset" @("URL: $DlBase/$asset") }
    }
    if (-not $Sums.ContainsKey($asset)) { Fail "No checksum for $asset in SHA256SUMS (release layout changed?)." }
    $got = (Get-FileHash -Algorithm SHA256 -Path $dest).Hash.ToLower()
    if ($got -ne $Sums[$asset]) {
        Fail "Checksum mismatch for $asset." @("expected $($Sums[$asset])", "got      $got", "Delete $dest and re-run.")
    }
    Ok "verified $asset"
    return $dest
}
Info "platform: $PlatformAsset"; $PlatformPath = Get-Verified $PlatformAsset
Info "cli:      $CliAsset";      $CliPath      = Get-Verified $CliAsset

# ---------------------------------------------------------------------------
# Step 7: Install platform (portable MSI extract) + CLI
# ---------------------------------------------------------------------------

Step "Installing the DeltaForge platform and CLI"
$PlatformDir = Join-Path $DfPrefix 'platform'
if (Test-Path $PlatformDir) { Remove-Item -Recurse -Force $PlatformDir }
New-Item -ItemType Directory -Force -Path $PlatformDir | Out-Null
$logMsi = Join-Path $CacheDir 'msi-extract.log'
# `msiexec /a` is an administrative install: lays files out under TARGETDIR with
# no registry writes and no elevation. Keeps the benchmark self-contained.
$p = Start-Process msiexec.exe -Wait -PassThru -ArgumentList @('/a', "`"$PlatformPath`"", '/qn', "TARGETDIR=`"$PlatformDir`"", '/l*v', "`"$logMsi`"")
if ($p.ExitCode -ne 0) { Fail "Extracting the platform MSI failed (exit $($p.ExitCode))." @("See $logMsi") }
$DfPlatformBin = (Get-ChildItem -Path $PlatformDir -Recurse -Filter 'deltaforge.exe' -ErrorAction SilentlyContinue | Select-Object -First 1).FullName
if (-not $DfPlatformBin) { Fail "deltaforge.exe not found after extracting $PlatformAsset." }
Ok "platform installed"

$CliExtract = Join-Path $CacheDir 'cli'
if (Test-Path $CliExtract) { Remove-Item -Recurse -Force $CliExtract }
Expand-Archive -Path $CliPath -DestinationPath $CliExtract -Force
$cliSrc = (Get-ChildItem -Path $CliExtract -Recurse -Filter 'deltaforge-cli.exe' | Select-Object -First 1).FullName
if (-not $cliSrc) { Fail "deltaforge-cli.exe not found inside $CliAsset." }
$DfCliPath = Join-Path $BinDir 'deltaforge-cli.exe'
Copy-Item -Force $cliSrc $DfCliPath
Ok "deltaforge-cli installed"

# ---------------------------------------------------------------------------
# Step 8: Python harness + optional Java
# ---------------------------------------------------------------------------

Step "Setting up the Python harness and comparison engines"
$VenvDir = Join-Path $BenchHome '.venv'
$VenvPy  = Join-Path $VenvDir 'Scripts\python.exe'
if (-not (Test-Path $VenvPy)) { python -m venv $VenvDir; if (-not (Test-Path $VenvPy)) { Fail "Could not create a Python venv." } }
& $VenvPy -m pip install --quiet --upgrade pip 2>$null
# --prefer-binary so pip never silently source-builds when a wheel exists for
# this Python (the floors in requirements.txt guarantee one).
Info "installing core engine dependencies (this can take a minute)"
& $VenvPy -m pip install --quiet --prefer-binary -r (Join-Path $BenchHome 'requirements.txt')
if ($LASTEXITCODE -ne 0) {
    Fail "Core Python dependency install failed." @("Inspect:  $VenvPy -m pip install --prefer-binary -r requirements.txt",
        "If a package has no wheel for Python $pyv, try a Python in the 3.9-3.13 range.")
}
Ok "core engine dependencies ready (DuckDB + Spark)"

# Reporting / plotting extras are optional; install best-effort, never blocking.
$reportReq = Join-Path $BenchHome 'requirements-report.txt'
if (Test-Path $reportReq) {
    & $VenvPy -m pip install --quiet --prefer-binary -r $reportReq 2>$null
    if ($LASTEXITCODE -eq 0) { Ok "reporting extras ready (pandas + charts)" }
    else { Warn "reporting extras have no wheel for Python $pyv; skipping them. Results still generate." }
}

$JavaHome = ''
if ($SkipSpark) { Info "SkipSpark: df and DuckDB will run, Spark engines will not." }
elseif (Get-Command java -ErrorAction SilentlyContinue) {
    $JavaHome = Split-Path (Split-Path (Get-Command java).Source); Ok "found system Java: $JavaHome"
} else {
    Info "no Java found; fetching a pinned Temurin 17 JRE so Spark works out of the box"
    try {
        $jreZip = Join-Path $CacheDir 'temurin17-jre.zip'
        Invoke-WebRequest -Uri 'https://api.adoptium.net/v3/binary/latest/17/ga/windows/x64/jre/hotspot/normal/eclipse' -OutFile $jreZip -UseBasicParsing
        $jreDir = Join-Path $DfPrefix 'jre'
        if (Test-Path $jreDir) { Remove-Item -Recurse -Force $jreDir }
        Expand-Archive -Path $jreZip -DestinationPath $jreDir -Force
        $javaExe = (Get-ChildItem -Path $jreDir -Recurse -Filter 'java.exe' | Select-Object -First 1).FullName
        if ($javaExe) { $JavaHome = Split-Path (Split-Path $javaExe); Ok "Java ready (bundled Temurin 17): $JavaHome" }
    } catch { Warn "could not download a JRE. Spark unavailable until you install Java 17; df and DuckDB still run." }
}

# ---------------------------------------------------------------------------
# Step 9: License + .env
# ---------------------------------------------------------------------------

Step "Writing configuration"
# DeltaForge needs a license key to run the engine, and the benchmark ships
# without one: every user brings their own. Resolve it from -LicenseKey /
# DELTA_FORGE_LICENSE_KEY, else an interactive prompt; with neither, stop with
# guidance rather than install a benchmark that cannot run a query. The key is
# written verbatim into .env; the platform's license validator is the authority.
if ($LicenseKey) {
    $LicenseKey = $LicenseKey.Trim()
    Ok "license: using the key from -LicenseKey / DELTA_FORGE_LICENSE_KEY"
} elseif (-not $AssumeYes -and [Environment]::UserInteractive) {
    Write-Host ""
    Write-Host "      DeltaForge needs a license key to run the engine." -ForegroundColor White -NoNewline
    Write-Host " It is free:"
    Write-Host "      sign in at $ConsoleUrl, create a key, and paste it below.`n"
    $LicenseKey = (Read-Host "      License key").Trim()
    if (-not $LicenseKey) {
        Fail "No license key entered." @(
            "DeltaForge cannot run the engine without a license key.",
            "Get a free key at $ConsoleUrl, then re-run .\install.ps1 and paste it",
            "(or: `$env:DELTA_FORGE_LICENSE_KEY='<key>'; .\install.ps1)."
        )
    }
    Ok "license: using the key you entered"
} else {
    Fail "No license key provided." @(
        "DeltaForge needs a license key to run the engine; the benchmark does not bundle one.",
        "This is a non-interactive install and no -LicenseKey / DELTA_FORGE_LICENSE_KEY was set.",
        "Get a free key at $ConsoleUrl, then: `$env:DELTA_FORGE_LICENSE_KEY='<key>'; .\install.ps1"
    )
}

$EnvFile = Join-Path $BenchHome '.env'
$dfConfig = Join-Path $DfPrefix 'dfconfig'
$lines = @(
    "# Generated by install.ps1 on windows-x64. Re-run install.ps1 to refresh."
    "DF_VERSION=$Version"
    "DF_PLATFORM_BIN=$DfPlatformBin"
    "DF_CLI_PATH=$DfCliPath"
    "DF_CONTROL_URL=http://127.0.0.1:3000"
    "DF_USERNAME=admin@deltaforge.local"
    "DF_PASSWORD=Benchmark_Admin1"
    ""
    "# First-run bootstrap contract (read by the embedded platform)."
    "DELTA_FORGE_LICENSE_KEY=$LicenseKey"
    "DELTA_FORGE_ADMIN_EMAIL=admin@deltaforge.local"
    "DELTA_FORGE_ADMIN_PASSWORD=Benchmark_Admin1"
    "DELTA_FORGE_ENGINEER_PASSWORD=Benchmark_Engineer1"
    "DELTA_FORGE_CONFIG_DIR=$dfConfig"
    "DELTA_FORGE_BIND_ADDR=127.0.0.1:3000"
)
if ($JavaHome) { $lines += "JAVA_HOME=$JavaHome" }
Set-Content -Path $EnvFile -Value $lines -Encoding UTF8
# Restrict .env to the current user: it holds the license key + admin password in
# plaintext (equivalent to chmod 600). Non-fatal so a perms hiccup can't sink an
# otherwise-good install.
try {
    $me = [System.Security.Principal.WindowsIdentity]::GetCurrent().Name
    & icacls $EnvFile /inheritance:r /grant:r "${me}:F" *> $null
} catch { Warn "could not tighten .env permissions; it holds the license key + admin password, restrict it manually." }
Ok "wrote $EnvFile (restricted to your user: holds the license key + admin password)"

# ---------------------------------------------------------------------------
# Done
# ---------------------------------------------------------------------------

Write-Host "`n  Setup complete. DeltaForge is ready to prove itself.`n" -ForegroundColor Green
Write-Host "  What you have:"
Write-Host "    platform : $DfPlatformBin"
Write-Host "    cli      : $DfCliPath"
Write-Host "    config   : $EnvFile"
Write-Host "`n  Run the benchmark:"
Write-Host "    cd `"$BenchHome`""
Write-Host "    .\bench.ps1                 # SF=1 smoke across all engines (a few minutes)" -ForegroundColor DarkGray
Write-Host "    .\bench.ps1 -Scale 10       # the standard headline tier" -ForegroundColor DarkGray

if (-not $SkipRun -and -not $AssumeYes -and [Environment]::UserInteractive) {
    $ans = Read-Host "`n  Run a quick SF=1 smoke now to confirm everything works? [Y/n]"
    if ($ans -notmatch '^[nN]') { & (Join-Path $BenchHome 'bench.ps1') }
}
