# Docker image — pull, build, publish

The bench is published as a public Docker Hub image. Reviewers do not
need this repo's source to run it; the harness, the engines, and
Postgres are all inside the image. The repo is for reading methodology,
auditing, and contributing patches.

## Pull and run

```bash
# 1. Pull a tagged release (or `latest`).
docker pull deltaforge/benchmarks:v0.1.0

# 2. Drop the .env file in your CWD and edit caps + secrets.
curl -fsSL https://raw.githubusercontent.com/deltaforge/delta-forge-benchmarks/v0.1.0/docker/.env.example \
    -o .env
$EDITOR .env

# 3. Run a smoke test (SF=1, query q01 only, 3 runs each engine).
docker run --rm -it \
    --name bench \
    --cpus="${BENCH_CPUS:-8}" --memory="${BENCH_MEMORY:-16g}" \
    --env-file .env \
    -v bench_data:/workspace/data \
    -v bench_pgdata:/var/lib/postgresql/data \
    -v $(pwd)/results:/workspace/results \
    deltaforge/benchmarks:v0.1.0 \
    python bench_runner.py --scale 1 --queries q01 --runs 3
```

For the full canonical run plus the cold-cache `dropcaches` sidecar,
use Compose:

```bash
curl -fsSL https://raw.githubusercontent.com/deltaforge/delta-forge-benchmarks/v0.1.0/docker/docker-compose.yml \
    -o docker-compose.yml
docker compose --env-file .env up -d
docker compose exec bench python bench_runner.py --scale 1
docker compose exec bench python reports/build_published.py \
    --results-dir results/<timestamp> --bench tpch_read_delta --out published/tpch.md
```

Results land under `./results/<timestamp>-<host>/` on the host.

## Image tag policy

| Tag | Meaning |
| --- | --- |
| `vX.Y.Z` | Immutable release. Pin this for reproducible runs. The bench repo CHANGELOG lists what each tag changed. |
| `vX.Y` | Floating tag, points at the latest patch in that minor line. |
| `latest` | Floating tag, points at the most recent published release. Useful for "give me the newest", not for citations. |

Every tagged image is built deterministically by
[`.github/workflows/docker-publish.yml`](../.github/workflows/docker-publish.yml)
on a clean GitHub Actions runner. The manifest of the published image
includes:

- the bench repo commit SHA (`org.opencontainers.image.revision`)
- the DeltaForge engine commit SHA (`com.deltaforge.engine.revision`)
- the build date

You can confirm an image's provenance with
`docker inspect deltaforge/benchmarks:vX.Y.Z`.

## How engine binaries reach the image

The Dockerfile uses two acquisition paths, both deterministic and tied
to a single `DF_VERSION` build-arg:

1. **`delta-forge-cli` and `delta-forge-worker`** are downloaded from
   the public release at
   `https://github.com/deltaforge-org/delta-forge/releases/download/v${DF_VERSION}/`,
   specifically the `deltaforge-cli-${DF_VERSION}-linux-x64.tar.gz` and
   `deltaforge-compute-${DF_VERSION}-linux-x64.tar.gz` artifacts. Each
   tarball is GPG-verified at build time against the DeltaForge release
   public key bundled into the image.
2. **`delta-forge-server` (control plane)** is **not** in the public
   release set today. The bench's publish workflow source-builds it
   from the engine repo at the same `v${DF_VERSION}` tag and stages it
   under `./build/df-bins/` for the Dockerfile to copy in.

When the engine adds `server` to its release components (a one-line
change to `delta-forge/scripts/build-release.sh:39`'s
`DEFAULT_COMPONENTS`), the bench's publish workflow drops the
source-build step and downloads all three binaries the same way. The
image contract does not change.

## Building and publishing the image (PowerShell)

The supported publish path is a local PowerShell script that uses
`docker buildx build --push` against a Docker Hub login already cached
by Docker Desktop. No CI secrets, no out-of-band credentials.

**One-time setup**:

1. `docker login` (Docker Desktop handles credential storage).
2. `docker buildx create --name mybuilder --use` if `mybuilder` does
   not already exist on your machine.
3. Stage `delta-forge-server` under `.\build\df-bins\`:

   ```powershell
   cd <engine-repo>
   git checkout v0.5.2          # or whichever DF_VERSION you want
   cargo build --release -p delta-forge-control --bin delta-forge-server `
               --features "api,cloud-all"
   Copy-Item target\release\delta-forge-server <bench-repo>\build\df-bins\
   ```

4. Stage the DeltaForge release public key at
   `.\docker\deltaforge-release-key.asc`. Fetch it from
   `https://github.com/deltaforge-org/delta-forge/releases/download/v<DfVersion>/deltaforge-release-key.asc`.

**Publish**:

```powershell
.\docker-build.ps1 -DfVersion 0.5.2 -ImageTag v0.1.0 `
                   -DfGitSha (git -C <engine-repo> rev-parse v0.5.2) `
                   -DfGpgFingerprint <release-key-fingerprint> `
                   -Repo public
```

The script:

- Verifies the staged binary + GPG key are present and the `mybuilder`
  buildx builder exists.
- Builds with `--platform linux/amd64`, `--attest type=provenance,mode=max`,
  and `--attest type=sbom`.
- Pushes both `deltaforge/benchmarks:<ImageTag>` and `:latest`.
- Records engine repo, engine version, and engine commit as image
  labels so reviewers can verify which DeltaForge they are
  benchmarking.

For a build-only smoke test without pushing, add `-NoPush` (the script
substitutes `--load` so the resulting image lands in your local Docker
daemon).

### One-off build (no publish)

If you only want to run the bench locally without publishing:

```powershell
.\docker-build.ps1 -DfVersion 0.5.2 -ImageTag local -NoPush
docker run --rm -it `
    -e DELTA_FORGE_ADMIN_PASSWORD=local `
    -e DELTA_FORGE_ENGINEER_PASSWORD=local `
    deltaforge/benchmarks:local `
    python bench_runner.py --scale 1 --queries q01 --runs 3
```

A locally-built image carries the same labels and behaves identically
to the published one, modulo the engine commit SHA the labels record.

### CI-driven publishing (deferred)

[`.github/workflows/docker-publish.yml`](../.github/workflows/docker-publish.yml)
exists as a fully wired alternative path: it source-builds
`delta-forge-server` from the engine repo at the matching tag,
downloads the GPG release key, runs `docker buildx build --push`
against Docker Hub, and smoke-tests the published image. It needs
three secrets configured on this repo before it will run:
`DOCKERHUB_USERNAME`, `DOCKERHUB_TOKEN`, and
`DELTAFORGE_GPG_FINGERPRINT`.

## What's inside the image

| Component | Version | License |
| --- | --- | --- |
| Eclipse Temurin OpenJDK | 17 (LTS) | GPL-2.0 with Classpath Exception (redistributable) |
| Python | 3.11 (Ubuntu Jammy) | PSF |
| PostgreSQL | 15 (apt PGDG) | PostgreSQL License |
| PySpark | 4.0.0 | Apache 2.0 |
| delta-spark | 4.0.0 | Apache 2.0 |
| DuckDB (Python) | pinned in `Dockerfile` | MIT |
| deltalake (Python) | pinned in `Dockerfile` | Apache 2.0 |
| DeltaForge binaries | from build args; recorded as `DF_GIT_SHA` | DeltaForge Community License |

The image is **not** a runtime endorsed by the Apache Spark or Delta
Lake projects; it bundles their official PyPI distributions for
benchmark purposes and credits them under their original licenses. See
[`LICENSE`](../LICENSE) and the `org.opencontainers.image.licenses`
label on the published image.

## Trust posture (what the public image guarantees, and does not)

- **Guaranteed.** Bit-identical bytes for the bench harness, the
  Postgres install, the JDK, PySpark, delta-spark, DuckDB, and
  deltalake at the tag you pulled. SHA-256 of every published tag is
  in [`CHANGELOG.md`](../CHANGELOG.md).
- **Guaranteed.** Engine commit SHA recorded in image labels and in
  every run's `manifest.json`.
- **Not guaranteed.** Hardware. The bench numbers we publish were
  produced on the documented reference box; your numbers depend on
  your CPU, RAM, disk, kernel, and noisy neighbors.
- **Not guaranteed.** That the engine binaries inside this image are
  the most recent DeltaForge release. They are the engine commit
  listed in the tag's manifest. We publish a new bench image whenever
  the engine ships a release we want benchmarked.
