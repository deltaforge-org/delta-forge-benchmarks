#!/usr/bin/env bash
# Environment pre-flight for the scale-out bench. Sourced by orchestrate.sh;
# also runnable standalone for diagnosis.
#
# Each check prints a [prereq] line and exits 1 on first failure. The
# whole script is silent on success except for a final OK summary.
#
# What gets checked:
#   1. systemd-run --user --scope works (we need cgroup-v2 ad-hoc scopes)
#   2. cgroup v2 cpuset + memory controllers available to the user slice
#   3. Dataset staged at $DATA_DIR/tpcds_sf100_delta with all 24 tables
#   4. /workspace volume has at least MIN_FREE_GB free
#   5. Machine is quiesced: nothing other than this script + bench
#      processes is over 1% CPU

set -euo pipefail

DATA_DIR="${DATA_DIR:-/workspace/data}"
MIN_FREE_GB="${MIN_FREE_GB:-50}"
QUIESCE_CPU_PCT="${QUIESCE_CPU_PCT:-1.0}"
SCALE="${SCALE:-100}"

TPCDS_TABLES=(
    call_center catalog_page catalog_returns catalog_sales
    customer customer_address customer_demographics date_dim
    household_demographics income_band inventory item
    promotion reason ship_mode store store_returns
    store_sales time_dim warehouse web_page web_returns
    web_sales web_site
)

err() { echo "[prereq] FAIL $*" >&2; exit 1; }
ok()  { echo "[prereq] ok   $*"; }

# 1. systemd-run --user --scope availability ---------------------------------

if ! command -v systemd-run >/dev/null 2>&1; then
    err "systemd-run not on PATH; install systemd (we need cgroup-v2 scopes)"
fi

# Probe with a no-op scope. If the user systemd manager is not running
# or the user lacks delegation, this fails fast and tells us why.
if ! systemd-run --user --scope --quiet --unit=df-scaleout-probe-$$ -- /bin/true \
        >/dev/null 2>&1; then
    err "systemd-run --user --scope failed; ensure user systemd is running \
('loginctl enable-linger \$USER' once is usually enough on WSL2)"
fi
ok "systemd-run --user --scope works"

# 2. cgroup v2 controllers available -----------------------------------------

if [ ! -f /sys/fs/cgroup/cgroup.controllers ]; then
    err "no /sys/fs/cgroup/cgroup.controllers; this kernel is not cgroup v2"
fi
controllers=$(cat /sys/fs/cgroup/cgroup.controllers)
for ctrl in cpuset memory; do
    if ! grep -qw "$ctrl" <<<"$controllers"; then
        err "cgroup v2 controller '$ctrl' not available (need it for \
AllowedCPUs / MemoryMax). Available: $controllers"
    fi
done
ok "cgroup v2 controllers present: cpuset, memory"

# 3. dataset staged ----------------------------------------------------------

TPCDS_DELTA="$DATA_DIR/tpcds_sf${SCALE}_delta"
if [ ! -d "$TPCDS_DELTA" ]; then
    err "dataset missing at $TPCDS_DELTA; run scripts/prepare_sf100.sh first"
fi
missing=()
for t in "${TPCDS_TABLES[@]}"; do
    if [ ! -d "$TPCDS_DELTA/$t" ]; then
        missing+=("$t")
    fi
done
if [ ${#missing[@]} -gt 0 ]; then
    err "$TPCDS_DELTA is missing tables: ${missing[*]}"
fi
ok "dataset complete at $TPCDS_DELTA (24/24 tables)"

# 4. disk free ---------------------------------------------------------------

free_gb=$(df -BG --output=avail "$DATA_DIR" | tail -1 | tr -dc '0-9')
if [ "$free_gb" -lt "$MIN_FREE_GB" ]; then
    err "free space ${free_gb} GB on $(df --output=target "$DATA_DIR" | tail -1) \
is below the ${MIN_FREE_GB} GB floor (results dir + logs)"
fi
ok "free disk: ${free_gb} GB on $(df --output=target "$DATA_DIR" | tail -1)"

# 5. quiesce check -----------------------------------------------------------
#
# `ps -e -o %cpu,comm` returns instantaneous CPU% so it can miss spikes.
# That is fine for the gate: the operator should have closed browsers /
# stopped the dev server before running, and this check catches the
# common "I forgot to stop X" failure mode rather than chasing every
# noise source.

noisy=$(ps -e -o pcpu=,comm= | awk -v t="$QUIESCE_CPU_PCT" '
    $1+0 > t && $2 !~ /^(ps|awk|bash|prereqs.sh|orchestrate.sh|systemd-run|systemd)$/ {
        print $0
    }
' | head -10)
if [ -n "$noisy" ]; then
    echo "[prereq] WARN processes consuming > ${QUIESCE_CPU_PCT}% CPU:" >&2
    echo "$noisy" >&2
    if [ "${ALLOW_NOISE:-0}" != "1" ]; then
        err "machine is not quiesced; stop the above or pass ALLOW_NOISE=1 to override"
    fi
    echo "[prereq] WARN proceeding anyway (ALLOW_NOISE=1)" >&2
else
    ok "machine quiesced (nothing > ${QUIESCE_CPU_PCT}% CPU)"
fi

echo "[prereq] all checks passed"
