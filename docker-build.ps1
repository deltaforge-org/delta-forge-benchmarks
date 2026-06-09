#!/usr/bin/env pwsh
# Build and push delta-forge-benchmarks to Docker Hub.
#
# Mirrors the working publish pattern from B:\Github\SQLFlow\docker-build.ps1:
# manual local PowerShell + `docker buildx build --push`. Auth is whatever
# Docker Desktop has cached in your OS keychain (no tokens in this repo).
#
# Prerequisites (all one-time setup):
#   1. `docker login` against Docker Hub (Docker Desktop handles this).
#   2. `docker buildx create --name mybuilder --use` (if `mybuilder` does not
#      already exist; SQLFlow's script assumes it does).
#   3. delta-forge-server binary staged under .\build\df-bins\delta-forge-server.
#      Build it from the engine repo at the matching tag:
#        cd <engine-repo>; git checkout v<DfVersion>
#        cargo build --release -p delta-forge-control --bin delta-forge-server `
#                    --features "api,cloud-all"
#        cp target\release\delta-forge-server <bench-repo>\build\df-bins\
#   4. DeltaForge release public key staged at .\docker\deltaforge-release-key.asc.
#      The engine release page hosts a copy; the Dockerfile uses it to
#      GPG-verify the cli + compute tarballs it downloads at build time.
#
# Usage:
#   .\docker-build.ps1 -DfVersion 0.5.2 -ImageTag v0.1.0
#   .\docker-build.ps1 -DfVersion 0.5.2 -ImageTag v0.1.0 -DfGitSha abc123 -Repo public

param(
    # Engine release version on github.com/deltaforge-org/delta-forge.
    # Must match an existing release tag (without leading "v"). The
    # Dockerfile downloads deltaforge-cli + deltaforge-compute tarballs
    # from the release at this version.
    [Parameter(Mandatory=$true)]
    [string]$DfVersion,

    # Docker tag to publish (e.g. v0.1.0). Defaults to v$DfVersion if empty.
    [string]$ImageTag = "",

    # Engine commit SHA, recorded in image labels and manifest.json.
    # Resolve via `git -C <engine-repo> rev-parse v<DfVersion>` and pass it
    # in. Defaults to "unset" so the script does not fail when you have not
    # got the engine repo cloned.
    [string]$DfGitSha = "unset",

    # GPG fingerprint of the DeltaForge release signing key. The Dockerfile
    # asserts the imported public key matches this fingerprint before
    # verifying any tarball. Pass an empty string to skip the assertion.
    [string]$DfGpgFingerprint = "",

    # `public` or `private`. Default `public` (the whole point of the bench).
    [ValidateSet("public", "private", "ask")]
    [string]$Repo = "ask",

    # Skip the buildx --push step (build only, useful for local validation).
    [switch]$NoPush,

    # Buildx builder name. SQLFlow uses `mybuilder`; we mirror that so the
    # same builder serves both repos. Override with -Builder if you want.
    [string]$Builder = "mybuilder"
)

# --- Repo selection -------------------------------------------------------

$publicRepo  = "deltaforge/benchmarks"
$privateRepo = "deltaforge/benchmarks-prv"

function Select-Repository {
    Write-Host "`nSelect repository type to publish to:" -ForegroundColor Yellow
    Write-Host "1. Public  ($publicRepo)" -ForegroundColor Cyan
    Write-Host "2. Private ($privateRepo)" -ForegroundColor Cyan
    $choice = Read-Host "Enter your choice (1 or 2)"
    switch ($choice) {
        "1" { return $publicRepo }
        "2" { return $privateRepo }
        default {
            Write-Host "Invalid choice. Defaulting to public ($publicRepo)." -ForegroundColor Yellow
            return $publicRepo
        }
    }
}

$selectedRepo = switch ($Repo) {
    "public"  { $publicRepo }
    "private" { $privateRepo }
    "ask"     { Select-Repository }
}

if (-not $ImageTag) { $ImageTag = "v$DfVersion" }

$primaryTag = "$selectedRepo`:$ImageTag"
$latestTag  = "$selectedRepo`:latest"

# --- Pre-flight checks ---------------------------------------------------

$repoRoot = Split-Path -Parent $PSCommandPath
$serverBin   = Join-Path $repoRoot "build\df-bins\delta-forge-server"
$gpgKeyPath  = Join-Path $repoRoot "docker\deltaforge-release-key.asc"

# delta-forge-server: not yet in the deltaforge-org public release set;
# Dockerfile expects it as a COPY from the build context.
if (-not (Test-Path $serverBin)) {
    Write-Host "ERROR: $serverBin missing." -ForegroundColor Red
    Write-Host ""
    Write-Host "Source-build it from the engine repo at v$DfVersion:" -ForegroundColor Yellow
    Write-Host "  cd <engine-repo>"
    Write-Host "  git checkout v$DfVersion"
    Write-Host "  cargo build --release -p delta-forge-control --bin delta-forge-server ``"
    Write-Host "              --features `"api,cloud-all`""
    Write-Host "  cp target\release\delta-forge-server $serverBin"
    exit 1
}

if (-not (Test-Path $gpgKeyPath)) {
    Write-Host "WARN: $gpgKeyPath missing. The Dockerfile uses this key" -ForegroundColor Yellow
    Write-Host "      to GPG-verify the cli + compute tarballs it downloads." -ForegroundColor Yellow
    Write-Host "      Fetch it from:" -ForegroundColor Yellow
    Write-Host "        https://github.com/deltaforge-org/delta-forge/releases/download/v$DfVersion/deltaforge-release-key.asc" -ForegroundColor Yellow
    Write-Host "      and place it at the path above before re-running."  -ForegroundColor Yellow
    exit 1
}

# Confirm the buildx builder exists; matches the SQLFlow assumption.
& docker buildx inspect $Builder *> $null
if ($LASTEXITCODE -ne 0) {
    Write-Host "ERROR: buildx builder '$Builder' not found." -ForegroundColor Red
    Write-Host "Create it once with:" -ForegroundColor Yellow
    Write-Host "  docker buildx create --name $Builder --use"
    exit 1
}

# --- Build + push --------------------------------------------------------

Write-Host ""
Write-Host "Building $primaryTag" -ForegroundColor Cyan
Write-Host "  DfVersion        : $DfVersion" -ForegroundColor Gray
Write-Host "  DfGitSha         : $DfGitSha"  -ForegroundColor Gray
Write-Host "  DfGpgFingerprint : $(if ($DfGpgFingerprint) { '<set>' } else { '<empty: skipping fp assertion>' })" -ForegroundColor Gray
Write-Host "  Builder          : $Builder"   -ForegroundColor Gray
Write-Host "  Push             : $(-not $NoPush)" -ForegroundColor Gray
Write-Host ""

$pushFlag = if ($NoPush) { "--load" } else { "--push" }

docker buildx build `
    --builder $Builder `
    --platform linux/amd64 `
    --attest type=provenance,mode=max `
    --attest type=sbom `
    -t $primaryTag `
    -t $latestTag `
    -f .\docker\Dockerfile `
    --build-arg DF_VERSION=$DfVersion `
    --build-arg DF_GIT_SHA=$DfGitSha `
    --build-arg DF_GPG_FINGERPRINT=$DfGpgFingerprint `
    --build-arg DF_GPG_PUBKEY_PATH=docker/deltaforge-release-key.asc `
    --build-arg PYSPARK_VERSION=4.0.0 `
    --build-arg DELTA_SPARK_VERSION=4.0.0 `
    --build-arg JDK_VERSION=17 `
    --build-arg POSTGRES_MAJOR=15 `
    --label "org.opencontainers.image.source=https://github.com/deltaforge/delta-forge-benchmarks" `
    --label "org.opencontainers.image.version=$ImageTag" `
    --label "org.opencontainers.image.licenses=Apache-2.0" `
    --label "com.deltaforge.engine.repo=https://github.com/deltaforge-org/delta-forge" `
    --label "com.deltaforge.engine.version=$DfVersion" `
    --label "com.deltaforge.engine.revision=$DfGitSha" `
    $pushFlag `
    .

if ($LASTEXITCODE -ne 0) {
    Write-Host "Build failed with exit code $LASTEXITCODE" -ForegroundColor Red
    exit $LASTEXITCODE
}

# --- Summary -------------------------------------------------------------

Write-Host ""
if ($NoPush) {
    Write-Host "Built (not pushed): $primaryTag" -ForegroundColor Green
} else {
    Write-Host "Pushed: $primaryTag" -ForegroundColor Green
    Write-Host "Pushed: $latestTag"  -ForegroundColor Green
    Write-Host ""
    Write-Host "Pull on any machine:" -ForegroundColor Yellow
    Write-Host "  docker pull $primaryTag"
}
