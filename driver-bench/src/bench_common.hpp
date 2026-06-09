// driver-bench: Shared types between bench_main, odbc_path, adbc_path.
//
// One Config object is parsed from the command line in bench_main and then
// handed to each driver path. Each path returns one PhaseTimings record per
// iteration; bench_main aggregates them into warm-median + per-phase
// breakdowns and emits the JSON / Markdown report.

#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace driver_bench {

struct Config {
    // Which driver(s) to exercise.
    bool   run_odbc      = true;
    bool   run_adbc      = true;

    // Iterations + warmups. Warmups are run but excluded from the reported
    // median to amortise driver-side metadata-cache cold misses (the
    // DeltaForge ODBC driver caches information_schema in memory; first
    // connect always pays the cold-miss penalty).
    int    warmups       = 1;
    int    iters         = 5;

    // Synthetic-row count to push through the bench query. The bench
    // builds its result set with generate_series(1, N) so any DeltaForge
    // instance can run this without a pre-existing fixture table.
    //
    // Default is 10M rows so the structural difference between ODBC's
    // per-cell row-binding path and ADBC's Arrow-stream path is
    // unambiguous in the report. The per-cell cost scales linearly with
    // (rows * cols); at 24 columns and 10M rows the ODBC bind+drain
    // phase is comfortably in the multi-second range and the ADBC
    // get_next loop's zero-copy advantage is clearly visible.
    int64_t rows         = 10000000;

    // ODBC consumption pattern. Two values:
    //
    //   bound   - the bench calls SQLBindCol once per column and then
    //             loops on SQLFetch. This is the unixODBC + .NET on
    //             Linux fast path. It is the most efficient way to
    //             consume an ODBC result and is unfair to ADBC: the
    //             driver only pays the columnar -> row transpose once
    //             per row, not per cell.
    //
    //   getdata - the bench calls SQLGetData per (row, column) at the
    //             column's natural C type. This is what .NET's
    //             OdbcDataReader does on Windows, what Power BI's
    //             mashup engine does, and what DBeaver / Excel do for
    //             the metadata-discovery phase. It is the SLOW ODBC
    //             pattern, the one ADBC was created to obsolete, and
    //             the one where the Arrow Flight wire pays off most
    //             visibly. Use this mode to see what BI-tool consumers
    //             actually pay.
    //
    // Default `both` runs each driver in BOTH modes so the report has
    // odbc-bound, odbc-getdata, and adbc as three peer columns. This
    // is the most informative shape for a published bench.
    std::string odbc_mode = "both";   // bound | getdata | both

    // Custom query overrides the synthetic one. If non-empty, the harness
    // uses this verbatim and does NOT substitute N. Use this to point the
    // bench at a real table when you want to measure a representative
    // schema rather than the synthetic mix.
    std::string custom_sql;

    // ODBC side: a configured DSN (resolved by the unixODBC driver
    // manager via /etc/odbc.ini or ~/.odbc.ini).
    std::string odbc_dsn      = "deltaforge";
    std::string odbc_uid;     // overrides DSN's Uid if non-empty
    std::string odbc_pwd;     // overrides DSN's Pwd if non-empty

    // ADBC side: dlopen target + connection options. The bench dlopens
    // the driver directly (like Power BI Desktop's adbc_driver_manager
    // would) so there is no link-time ADBC dependency.
    std::string adbc_so       = "/home/chess/delta-forge/build/adbc-linux-x64/libdeltaforge_adbc.so.1";
    std::string adbc_uri;     // control plane, e.g. http://host:3000
    std::string adbc_compute; // optional compute override
    std::string adbc_user;
    std::string adbc_pwd;
    std::string adbc_token;   // session token; takes precedence over user+pwd

    // Output controls.
    bool   emit_json     = false;     // emit single JSON object on stdout
    std::string json_path;            // write JSON to file instead of stdout
};

struct PhaseTimings {
    // All times are wall-clock seconds. Phases not applicable to a given
    // driver stay at 0.0.
    double t_connect     = 0.0;       // ODBC: SQLConnect; ADBC: DatabaseNew+SetOption*+Init+ConnectionNew+Init
    double t_prepare     = 0.0;       // ODBC: SQLExecDirect prologue; ADBC: StatementNew+SetSqlQuery
    double t_execute     = 0.0;       // ODBC: SQLExecDirect; ADBC: ExecuteQuery returning ArrowArrayStream
    double t_bind        = 0.0;       // ODBC: SQLDescribeCol + SQLBindCol loop; ADBC: 0 (no bind step)
    double t_drain       = 0.0;       // ODBC: SQLFetch loop until SQL_NO_DATA; ADBC: ArrowArrayStream.get_next loop
    double t_release     = 0.0;       // ODBC: SQLFreeHandle + SQLDisconnect; ADBC: StatementRelease+ConnectionRelease+DatabaseRelease
    double t_total       = 0.0;       // sum of the above for sanity

    int64_t rows         = 0;
    int     columns      = 0;

    // Filled if the iteration failed. When non-empty, this iteration is
    // excluded from the median.
    std::string error;
};

struct DriverResult {
    std::string         driver;       // "odbc" or "adbc"
    std::vector<PhaseTimings> iters;  // one entry per (warmup + measured) iter
    PhaseTimings        warm_median;  // median of iters[warmups..], excluding errors
    int                 warmup_count = 0;
    int                 measured_count = 0;
    int                 error_count = 0;
    std::string         sql;
};

// Wall-clock seconds since an unspecified monotonic epoch.
double now_seconds();

// Compute the per-field median across the measured iterations of one
// driver run, skipping warmups and errored iterations. Median is computed
// per phase independently so the warm_median.t_* values are honest
// per-phase medians, not co-located samples from the same iteration.
void compute_warm_median(DriverResult& r, int warmup_count);

// Entry points implemented in odbc_path.cpp / adbc_path.cpp.
//
// odbc_mode: "bound" = SQLBindCol + SQLFetch path (Linux unixODBC fast path).
//            "getdata" = per-cell SQLGetData path (.NET / Power BI pattern).
PhaseTimings run_odbc_iter(const Config& cfg, const std::string& sql,
                           const std::string& odbc_mode);
PhaseTimings run_adbc_iter(const Config& cfg, const std::string& sql);

} // namespace driver_bench
