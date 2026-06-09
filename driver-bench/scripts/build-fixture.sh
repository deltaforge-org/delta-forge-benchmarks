#!/usr/bin/env bash
# driver-bench: build (or recreate) the wide BI fixture table the bench
# scans. Loaded via unixODBC -> the DeltaForge ODBC driver because the
# headless `delta-forge-cli` path on the standalone server cannot currently
# resolve user-created zones in DDL (CTAS returns "failed to resolve
# catalog"). The ODBC driver routes CTAS through a different planner
# entrypoint that does honor user zones, so this is the load-bearing
# fixture path on a self-provisioned stack.
#
# Table shape: 22 columns of mixed BI types (BIGINT, SMALLINT, TINYINT,
# BOOL, DOUBLE, DECIMAL(18,4), DECIMAL(28,8), DECIMAL(10,2), TIMESTAMP,
# DATE, VARCHAR of multiple widths, MD5 hexstrings, padded labels). Same
# shape the bench harness's synthetic --rows query would generate, but
# materialised once and scanned per-iteration instead of regenerated on
# every iter.
#
# Idempotent: DROP TABLE IF EXISTS + CREATE TABLE AS. Re-run to refresh
# at a different row count.
#
# Knobs:
#   DRIVER_BENCH_ROWS    default 1000000   number of rows in the fixture
#   DRIVER_BENCH_TABLE   default t_wide    table name (created at the
#                                          DSN's default zone/schema)

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

DF_HOME="${DF_HOME:-/tmp/df-bench-stack}"
[ -f "$DF_HOME/stack.env" ] && set -a && . "$DF_HOME/stack.env" && set +a

DSN="${DF_STACK_DSN:-deltaforge_bench}"
ROWS="${DRIVER_BENCH_ROWS:-1000000}"
TABLE="${DRIVER_BENCH_TABLE:-t_wide}"

log()  { printf "\033[1;36m[fixture] %s\033[0m\n" "$*"; }
fail() { printf "\033[1;31m[fixture] ERROR %s\033[0m\n" "$*" >&2; exit 1; }

if ! command -v isql >/dev/null 2>&1; then
    fail "isql not on PATH. Run ./scripts/install.sh first."
fi
if ! isql -b "$DSN" <<< "SELECT 1" >/dev/null 2>&1; then
    fail "DSN '$DSN' is not reachable via isql. Run ./scripts/setup-host-stack.sh first."
fi

# The (i+0) wrap is intentional: at the time of writing, the engine
# planner has an edge case where `cast(<bare generate_series column> AS
# decimal(p,s))` in a projection above ~200 rows fails to plan. Wrapping
# the column reference in any arithmetic expression sidesteps it. The
# +0 is constant-folded and has no measured cost.
SQL="
DROP TABLE IF EXISTS ${TABLE};
CREATE TABLE ${TABLE} AS
SELECT
  (i+0)                                       AS id_i64,
  (i+0) * 2                                   AS doubled_i64,
  ((i+0) % 32767)::smallint                   AS small_i,
  ((i+0) % 127)::tinyint                      AS tiny_i,
  ((i+0) % 2 = 0)                             AS flag_bool,
  ((i+0) * 1.5)::double                       AS f64_a,
  ((i+0) / 7.0)::double                       AS f64_b,
  cast((i+0)        AS decimal(18,4))         AS dec18,
  cast((i+0) * 0.01 AS decimal(28,8))         AS dec28,
  cast((i+0) * 1.0  AS decimal(10,2))         AS price,
  cast(timestamp '2026-01-01 00:00:00' + ((i+0) || ' seconds')::interval AS timestamp) AS ts_a,
  cast(date '2026-01-01' + ((i+0) % 365)      AS date) AS d_a,
  md5((i+0)::text)                            AS md5_hex,
  ('row-' || (i+0)::text)                     AS label_v32,
  repeat('x', 16)                             AS pad_v16,
  repeat('y', 64)                             AS pad_v64,
  repeat('z', 128)                            AS pad_v128,
  cast((i+0)        AS varchar(40))           AS i_as_text,
  upper(md5((i+0)::text))                     AS md5_upper,
  concat('id-', lpad((i+0)::text, 10, '0'))   AS padded_id,
  cast((i+0) % 127  AS tinyint)               AS bucket,
  cast((i+0) % 1000 AS integer)               AS dim_k
FROM generate_series(1, ${ROWS}) AS t(i);
"

log "building $TABLE with $ROWS rows via DSN=$DSN..."
# Pipe through tr to collapse the multi-line statement to one logical line
# (isql is line-oriented and would otherwise execute each line as a
# separate statement).
START=$(date +%s)
echo "$SQL" | tr '\n' ' ' | isql "$DSN" >/tmp/df-bench-fixture-build.log 2>&1 \
    || { tail -20 /tmp/df-bench-fixture-build.log >&2; fail "fixture build failed; see /tmp/df-bench-fixture-build.log"; }
END=$(date +%s)
log "fixture built in $((END-START))s"

# Verify by counting rows. isql's table-formatted output looks like
#   +---------------------+
#   | count(*)            |
#   +---------------------+
#   | 12345               |
#   +---------------------+
#   1 rows fetched
# We want the numeric value: the only `|`-delimited row that holds a
# pure integer. Strip ASCII art, drop empty lines, then match.
COUNT_OUT="$(echo "SELECT count(*) FROM ${TABLE};" | isql "$DSN" 2>&1)"
COUNT_LINE="$(echo "$COUNT_OUT" \
    | grep -oE '^\| *[0-9]+ *\|' \
    | head -1 \
    | tr -d ' |')"
log "${TABLE} row count: ${COUNT_LINE:-(parse failed)}"
if [ "$COUNT_LINE" != "$ROWS" ]; then
    echo "$COUNT_OUT" | tail -10 >&2
    fail "row count mismatch: expected $ROWS, got '${COUNT_LINE:-empty}'"
fi
log "ready. Next: ./scripts/run_bench.sh (will SELECT * FROM $TABLE)"
