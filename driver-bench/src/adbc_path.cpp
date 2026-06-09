// driver-bench: ADBC round-trip path.
//
// Loads the DeltaForge ADBC driver shared object directly via dlopen and
// drives it through the full ADBC v1.0.0 lifecycle:
//
//   AdbcDriverInit                  (one-time, but counted under t_connect
//                                    so the comparison against ODBC's
//                                    library-load is honest)
//   AdbcDatabaseNew/SetOption*/Init
//   AdbcConnectionNew/Init
//   AdbcStatementNew/SetSqlQuery
//   AdbcStatementExecuteQuery       (returns ArrowArrayStream)
//   stream.get_next() in a loop until length==0
//   release (statement, connection, database)
//
// The drain phase pulls Arrow C Data Interface batches directly from the
// driver. No row-by-row materialisation, no SQLBindCol-equivalent step,
// no per-cell copy: that is the structural advantage ADBC has over ODBC
// for analytical / BI workloads.
//
// We do NOT link against libadbc_driver_manager. The Apache driver
// manager is convenient but adds a Linux runtime dependency the public
// bench is better off without. The ADBC bridge exports exactly one
// symbol (AdbcDriverInit) and the function-pointer table it fills in
// has stable ABI per ADBC_VERSION_1_0_0; dlopen + dlsym is enough.

#include "bench_common.hpp"
#include "adbc.h"

#include <dlfcn.h>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <stdexcept>
#include <string>
#include <vector>

namespace driver_bench {

namespace {

using AdbcDriverInitFn = AdbcStatusCode (*)(int, void*, struct AdbcError*);

const char* adbc_status_name(AdbcStatusCode rc) {
    switch (rc) {
        case ADBC_STATUS_OK:                return "OK";
        case ADBC_STATUS_UNKNOWN:           return "UNKNOWN";
        case ADBC_STATUS_NOT_IMPLEMENTED:   return "NOT_IMPLEMENTED";
        case ADBC_STATUS_NOT_FOUND:         return "NOT_FOUND";
        case ADBC_STATUS_ALREADY_EXISTS:    return "ALREADY_EXISTS";
        case ADBC_STATUS_INVALID_ARGUMENT:  return "INVALID_ARGUMENT";
        case ADBC_STATUS_INVALID_STATE:     return "INVALID_STATE";
        case ADBC_STATUS_INVALID_DATA:      return "INVALID_DATA";
        case ADBC_STATUS_INTEGRITY:         return "INTEGRITY";
        case ADBC_STATUS_INTERNAL:          return "INTERNAL";
        case ADBC_STATUS_IO:                return "IO";
        case ADBC_STATUS_CANCELLED:         return "CANCELLED";
        case ADBC_STATUS_TIMEOUT:           return "TIMEOUT";
        case ADBC_STATUS_UNAUTHENTICATED:   return "UNAUTHENTICATED";
        case ADBC_STATUS_UNAUTHORIZED:      return "UNAUTHORIZED";
        default:                            return "???";
    }
}

std::string adbc_error_to_string(AdbcStatusCode rc, struct AdbcError* err) {
    std::string out;
    out.reserve(256);
    out += "rc=";
    out += std::to_string(rc);
    out += " (";
    out += adbc_status_name(rc);
    out += ")";
    if (err) {
        if (err->sqlstate[0]) {
            out += " sqlstate=";
            out.append(err->sqlstate, 5);
        }
        if (err->message) {
            out += " msg=";
            out += err->message;
        }
        if (err->release) {
            err->release(err);
        }
    }
    return out;
}

// Count columns by walking the ArrowSchema once. The bench only needs
// the total to report apples-to-apples with ODBC's SQLNumResultCols.
int count_columns(const struct ArrowSchema* s) {
    if (!s) return 0;
    // Root schema for a record batch advertises format "+s" (struct)
    // and the n_children is the column count.
    return static_cast<int>(s->n_children);
}

} // namespace

PhaseTimings run_adbc_iter(const Config& cfg, const std::string& sql) {
    PhaseTimings out;

    void* lib = nullptr;
    AdbcDriverInitFn init_fn = nullptr;
    struct AdbcDriver driver = {};
    struct AdbcDatabase   db   = {};
    struct AdbcConnection conn = {};
    struct AdbcStatement  stmt = {};
    struct AdbcError      err  = {};
    bool db_init = false, conn_init = false, stmt_init = false;
    AdbcStatusCode rc;

    // --- Phase: connect (dlopen + DriverInit + DB + Connection) -------
    const double t0 = now_seconds();

    lib = dlopen(cfg.adbc_so.c_str(), RTLD_NOW | RTLD_LOCAL);
    if (!lib) {
        const char* dlerr = dlerror();
        out.error = std::string("dlopen(") + cfg.adbc_so + ") failed: " +
                    (dlerr ? dlerr : "(unknown)");
        return out;
    }
    init_fn = reinterpret_cast<AdbcDriverInitFn>(dlsym(lib, "AdbcDriverInit"));
    if (!init_fn) {
        out.error = "dlsym(AdbcDriverInit) failed: " +
                    std::string(dlerror() ? dlerror() : "(unknown)");
        dlclose(lib);
        return out;
    }
    rc = init_fn(ADBC_VERSION_1_0_0, &driver, &err);
    if (rc != ADBC_STATUS_OK) {
        out.error = "AdbcDriverInit failed: " + adbc_error_to_string(rc, &err);
        dlclose(lib);
        return out;
    }

    rc = driver.DatabaseNew(&db, &err);
    if (rc != ADBC_STATUS_OK) {
        out.error = "DatabaseNew failed: " + adbc_error_to_string(rc, &err);
        dlclose(lib);
        return out;
    }
    rc = driver.DatabaseSetOption(&db, "uri", cfg.adbc_uri.c_str(), &err);
    if (rc != ADBC_STATUS_OK) {
        out.error = "DatabaseSetOption(uri) failed: " + adbc_error_to_string(rc, &err);
        driver.DatabaseRelease(&db, &err);
        dlclose(lib);
        return out;
    }
    if (!cfg.adbc_compute.empty()) {
        rc = driver.DatabaseSetOption(&db, "adbc.deltaforge.compute_url",
                                      cfg.adbc_compute.c_str(), &err);
        if (rc != ADBC_STATUS_OK) {
            out.error = "DatabaseSetOption(compute_url) failed: " +
                        adbc_error_to_string(rc, &err);
            driver.DatabaseRelease(&db, &err);
            dlclose(lib);
            return out;
        }
    }
    if (!cfg.adbc_token.empty()) {
        (void) driver.DatabaseSetOption(&db, "adbc.deltaforge.session_token",
                                        cfg.adbc_token.c_str(), &err);
    } else {
        if (!cfg.adbc_user.empty()) {
            (void) driver.DatabaseSetOption(&db, "username",
                                            cfg.adbc_user.c_str(), &err);
        }
        if (!cfg.adbc_pwd.empty()) {
            (void) driver.DatabaseSetOption(&db, "password",
                                            cfg.adbc_pwd.c_str(), &err);
        }
    }
    rc = driver.DatabaseInit(&db, &err);
    if (rc != ADBC_STATUS_OK) {
        out.error = "DatabaseInit failed: " + adbc_error_to_string(rc, &err);
        driver.DatabaseRelease(&db, &err);
        dlclose(lib);
        return out;
    }
    db_init = true;

    rc = driver.ConnectionNew(&conn, &err);
    if (rc != ADBC_STATUS_OK) {
        out.error = "ConnectionNew failed: " + adbc_error_to_string(rc, &err);
        driver.DatabaseRelease(&db, &err);
        dlclose(lib);
        return out;
    }
    rc = driver.ConnectionInit(&conn, &db, &err);
    if (rc != ADBC_STATUS_OK) {
        out.error = "ConnectionInit failed: " + adbc_error_to_string(rc, &err);
        driver.ConnectionRelease(&conn, &err);
        driver.DatabaseRelease(&db, &err);
        dlclose(lib);
        return out;
    }
    conn_init = true;

    const double t1 = now_seconds();
    out.t_connect = t1 - t0;

    // --- Phase: prepare (StatementNew + SetSqlQuery) ------------------

    rc = driver.StatementNew(&conn, &stmt, &err);
    if (rc != ADBC_STATUS_OK) {
        out.error = "StatementNew failed: " + adbc_error_to_string(rc, &err);
        if (conn_init) driver.ConnectionRelease(&conn, &err);
        if (db_init)   driver.DatabaseRelease(&db, &err);
        dlclose(lib);
        return out;
    }
    stmt_init = true;

    rc = driver.StatementSetSqlQuery(&stmt, sql.c_str(), &err);
    if (rc != ADBC_STATUS_OK) {
        out.error = "StatementSetSqlQuery failed: " + adbc_error_to_string(rc, &err);
        if (stmt_init) driver.StatementRelease(&stmt, &err);
        if (conn_init) driver.ConnectionRelease(&conn, &err);
        if (db_init)   driver.DatabaseRelease(&db, &err);
        dlclose(lib);
        return out;
    }

    const double t2 = now_seconds();
    out.t_prepare = t2 - t1;

    // --- Phase: execute (returns the ArrowArrayStream) ----------------

    struct ArrowArrayStream stream = {};
    int64_t out_rows = -1;
    rc = driver.StatementExecuteQuery(&stmt, &stream, &out_rows, &err);
    if (rc != ADBC_STATUS_OK) {
        out.error = "StatementExecuteQuery failed: " + adbc_error_to_string(rc, &err);
        if (stmt_init) driver.StatementRelease(&stmt, &err);
        if (conn_init) driver.ConnectionRelease(&conn, &err);
        if (db_init)   driver.DatabaseRelease(&db, &err);
        dlclose(lib);
        return out;
    }

    // ArrowArrayStream.get_schema is where the DeltaForge ADBC bridge
    // performs the first server round-trip (stream.cpp pulls the first
    // batch to recover the schema). This is the apples-to-apples mirror
    // of the ODBC driver's SQLExecDirect blocking on the first chunk,
    // so we fold it into t_execute, not t_drain. Without this fold the
    // phase table is misleading: ADBC's drain looks 6x slower than
    // ODBC's purely because the first-batch wait lands in different
    // buckets between the two drivers.
    {
        struct ArrowSchema schema = {};
        int gs_rc = stream.get_schema(&stream, &schema);
        if (gs_rc != 0) {
            out.error = "ArrowArrayStream.get_schema returned " + std::to_string(gs_rc);
            if (stream.release) stream.release(&stream);
            if (stmt_init) driver.StatementRelease(&stmt, &err);
            if (conn_init) driver.ConnectionRelease(&conn, &err);
            if (db_init)   driver.DatabaseRelease(&db, &err);
            dlclose(lib);
            return out;
        }
        out.columns = count_columns(&schema);
        if (schema.release) schema.release(&schema);
    }

    const double t3 = now_seconds();
    out.t_execute = t3 - t2;     // includes get_schema's first-batch wait
    out.t_bind = 0.0;            // ADBC has no bind step; reported as 0 for symmetry

    // --- Phase: drain (pull Arrow batches until length==0) ------------
    //
    // Per-batch instrumentation: count batches and split drain time into
    // (a) time blocked inside stream.get_next, which is the bridge's
    // df_next_batch dequeue + arrow::ExportRecordBatch path, vs
    // (b) time spent in batch.release callbacks + bench-side bookkeeping.
    // The split tells us whether tuning effort should target the
    // bridge's per-batch export path or the consumer's release path.

    int64_t row_count    = 0;
    int64_t batch_count  = 0;
    double  t_get_next   = 0.0;
    double  t_release_b  = 0.0;
    int64_t min_batch_rows = 0;
    int64_t max_batch_rows = 0;
    while (true) {
        struct ArrowArray batch = {};
        const double tg0 = now_seconds();
        int gn = stream.get_next(&stream, &batch);
        const double tg1 = now_seconds();
        t_get_next += (tg1 - tg0);
        if (gn != 0) {
            out.error = "ArrowArrayStream.get_next returned " + std::to_string(gn);
            break;
        }
        if (batch.release == nullptr) {
            // End of stream per ADBC spec.
            break;
        }
        row_count += batch.length;
        ++batch_count;
        if (batch_count == 1 || batch.length < min_batch_rows) min_batch_rows = batch.length;
        if (batch.length > max_batch_rows) max_batch_rows = batch.length;
        const double tr0 = now_seconds();
        batch.release(&batch);
        const double tr1 = now_seconds();
        t_release_b += (tr1 - tr0);
    }
    if (stream.release) stream.release(&stream);

    const double t4 = now_seconds();
    out.t_drain = t4 - t3;
    out.rows    = row_count;

    // Side-channel the per-batch breakdown via stderr so the bench
    // table stays the same shape. This is gated on DF_BENCH_VERBOSE
    // so production runs do not get the chatter.
    if (std::getenv("DF_BENCH_VERBOSE")) {
        std::fprintf(stderr,
            "[adbc-instr] batches=%lld rows=%lld min_batch=%lld max_batch=%lld "
            "t_get_next=%.3fs t_release=%.3fs t_drain=%.3fs (other=%.3fs)\n",
            (long long)batch_count, (long long)row_count,
            (long long)min_batch_rows, (long long)max_batch_rows,
            t_get_next, t_release_b, out.t_drain,
            out.t_drain - t_get_next - t_release_b);

        // If the bridge was built with profiling support, dump its
        // per-batch breakdown of df_next_batch vs arrow::ExportRecordBatch
        // vs df_batch_free. Lets us see which phase of the bridge's
        // get_next is the actual cost driver.
        using ProfileFn = void(*)(const char*);
        auto profile_fn = reinterpret_cast<ProfileFn>(
            dlsym(lib, "df_adbc_profile_reset_and_dump"));
        if (profile_fn != nullptr) {
            profile_fn("post-drain");
        }
    }

    if (!out.error.empty()) {
        if (stmt_init) driver.StatementRelease(&stmt, &err);
        if (conn_init) driver.ConnectionRelease(&conn, &err);
        if (db_init)   driver.DatabaseRelease(&db, &err);
        dlclose(lib);
        return out;
    }

    // --- Phase: release ----------------------------------------------

    if (stmt_init) {
        rc = driver.StatementRelease(&stmt, &err);
        if (rc != ADBC_STATUS_OK) {
            out.error = "StatementRelease failed: " + adbc_error_to_string(rc, &err);
        }
    }
    if (conn_init) {
        rc = driver.ConnectionRelease(&conn, &err);
        if (rc != ADBC_STATUS_OK && out.error.empty()) {
            out.error = "ConnectionRelease failed: " + adbc_error_to_string(rc, &err);
        }
    }
    if (db_init) {
        rc = driver.DatabaseRelease(&db, &err);
        if (rc != ADBC_STATUS_OK && out.error.empty()) {
            out.error = "DatabaseRelease failed: " + adbc_error_to_string(rc, &err);
        }
    }
    dlclose(lib);

    const double t5 = now_seconds();
    out.t_release = t5 - t4;
    out.t_total   = t5 - t0;

    return out;
}

} // namespace driver_bench
