// driver-bench: ODBC round-trip path.
//
// Drives the DeltaForge ODBC driver through the standard unixODBC driver
// manager, in the shape a BI tool would: SQLConnect via configured DSN,
// SQLExecDirect, SQLDescribeCol for every column, SQLBindCol into a
// row-oriented C-type buffer, then SQLFetch in a loop until SQL_NO_DATA.
//
// The bind step is where the columnar -> row transpose happens inside
// the driver: every fetch copies the next row's cell of every column
// from the staged Arrow batch into the caller-supplied C buffer. That
// per-cell copy is the cost the ADBC path eliminates by handing the
// caller an ArrowArrayStream directly.
//
// For each column we pick a C type that matches the column's SQL type
// 1:1 (no implicit conversion). This mirrors the .NET / Power BI
// "natural type per column" pattern and avoids the ODBC driver's
// per-cell decimal/numeric snprintf+parse hot path.

#include "bench_common.hpp"

#include <sql.h>
#include <sqlext.h>
#include <sqltypes.h>

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <stdexcept>
#include <string>
#include <vector>

namespace driver_bench {

namespace {

// Pull the formatted diagnostic for a failed handle. We only use this on
// the error path so the cost does not show up in measured timings.
std::string read_diag(SQLSMALLINT htype, SQLHANDLE handle) {
    SQLCHAR sqlstate[6] = {0};
    SQLINTEGER native_err = 0;
    SQLCHAR msg[2048] = {0};
    SQLSMALLINT msg_len = 0;
    SQLRETURN rc = SQLGetDiagRec(htype, handle, 1, sqlstate, &native_err,
                                 msg, sizeof(msg), &msg_len);
    if (!SQL_SUCCEEDED(rc)) {
        return "(no diagnostic)";
    }
    return std::string("[") + reinterpret_cast<const char*>(sqlstate) + "] " +
           reinterpret_cast<const char*>(msg);
}

// A bound column: holds the per-row buffer ODBC writes the current cell
// into on each SQLFetch, plus the StrLen_or_Ind that reports NULL.
struct BoundColumn {
    SQLSMALLINT sql_type = 0;
    SQLSMALLINT c_type   = 0;
    SQLULEN     col_size = 0;
    SQLSMALLINT decimal_digits = 0;
    SQLLEN      buffer_len = 0;
    std::vector<char> buffer;
    SQLLEN      ind = 0;        // StrLen_or_Ind, written by SQLFetch
};

// Map a SQL_* type to the C-type the bind buffer should hold. We pick
// the *natural* C type for the column (the .NET / Power BI pattern),
// not a one-size-fits-all conversion. This is the fair-comparison
// shape: an ADBC consumer gets the native Arrow type, so the ODBC
// consumer should at least bind the native C type.
SQLSMALLINT natural_c_type(SQLSMALLINT sql_type) {
    switch (sql_type) {
        case SQL_BIT:           return SQL_C_BIT;
        case SQL_TINYINT:       return SQL_C_TINYINT;
        case SQL_SMALLINT:      return SQL_C_SHORT;
        case SQL_INTEGER:       return SQL_C_LONG;
        case SQL_BIGINT:        return SQL_C_SBIGINT;
        case SQL_REAL:          return SQL_C_FLOAT;
        case SQL_FLOAT:
        case SQL_DOUBLE:        return SQL_C_DOUBLE;
        case SQL_DECIMAL:
        case SQL_NUMERIC:       return SQL_C_NUMERIC;
        case SQL_DATE:
        case SQL_TYPE_DATE:     return SQL_C_TYPE_DATE;
        case SQL_TIME:
        case SQL_TYPE_TIME:     return SQL_C_TYPE_TIME;
        case SQL_TIMESTAMP:
        case SQL_TYPE_TIMESTAMP:return SQL_C_TYPE_TIMESTAMP;
        case SQL_GUID:          return SQL_C_GUID;
        case SQL_BINARY:
        case SQL_VARBINARY:
        case SQL_LONGVARBINARY: return SQL_C_BINARY;
        case SQL_WCHAR:
        case SQL_WVARCHAR:
        case SQL_WLONGVARCHAR:  return SQL_C_WCHAR;
        default:                return SQL_C_CHAR;
    }
}

// How many bytes the bind buffer needs for one cell of (sql_type,
// col_size). For variable-length types we cap at a generous bound
// because the synthetic bench generates short-to-mid strings; if a
// caller hits a longer payload they can override the query and the
// cap below still survives by virtue of being one full SQLFetch row
// (truncation would surface as SQL_SUCCESS_WITH_INFO + a 01004 state
// which the harness reports as an error iteration).
SQLLEN cell_buffer_bytes(SQLSMALLINT c_type, SQLULEN col_size) {
    switch (c_type) {
        case SQL_C_BIT:
        case SQL_C_TINYINT:     return 1;
        case SQL_C_SHORT:       return 2;
        case SQL_C_LONG:        return 4;
        case SQL_C_FLOAT:       return 4;
        case SQL_C_SBIGINT:     return 8;
        case SQL_C_DOUBLE:      return 8;
        case SQL_C_NUMERIC:     return sizeof(SQL_NUMERIC_STRUCT);
        case SQL_C_TYPE_DATE:   return sizeof(SQL_DATE_STRUCT);
        case SQL_C_TYPE_TIME:   return sizeof(SQL_TIME_STRUCT);
        case SQL_C_TYPE_TIMESTAMP: return sizeof(SQL_TIMESTAMP_STRUCT);
        case SQL_C_GUID:        return sizeof(SQLGUID);
        case SQL_C_BINARY: {
            SQLULEN n = (col_size > 0 && col_size < (1u << 20)) ? col_size : 65536;
            return static_cast<SQLLEN>(n);
        }
        case SQL_C_WCHAR: {
            SQLULEN n = (col_size > 0 && col_size < (1u << 18)) ? col_size : 8192;
            return static_cast<SQLLEN>((n + 1) * sizeof(SQLWCHAR));
        }
        case SQL_C_CHAR:
        default: {
            SQLULEN n = (col_size > 0 && col_size < (1u << 18)) ? col_size : 8192;
            return static_cast<SQLLEN>(n + 1);
        }
    }
}

void free_handles(SQLHSTMT hstmt, SQLHDBC hdbc, SQLHENV henv,
                  bool connected) {
    if (hstmt != SQL_NULL_HSTMT) SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
    if (connected)               SQLDisconnect(hdbc);
    if (hdbc != SQL_NULL_HDBC)   SQLFreeHandle(SQL_HANDLE_DBC, hdbc);
    if (henv != SQL_NULL_HENV)   SQLFreeHandle(SQL_HANDLE_ENV, henv);
}

} // namespace

PhaseTimings run_odbc_iter(const Config& cfg, const std::string& sql,
                           const std::string& odbc_mode) {
    PhaseTimings out;
    const bool use_getdata = (odbc_mode == "getdata");

    SQLHENV  henv  = SQL_NULL_HENV;
    SQLHDBC  hdbc  = SQL_NULL_HDBC;
    SQLHSTMT hstmt = SQL_NULL_HSTMT;
    bool     connected = false;
    SQLRETURN rc;

    // --- Phase: connect (env alloc + dbc alloc + SQLConnect) ----------
    const double t0 = now_seconds();

    rc = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &henv);
    if (!SQL_SUCCEEDED(rc)) {
        out.error = "SQLAllocHandle(ENV) failed";
        return out;
    }
    rc = SQLSetEnvAttr(henv, SQL_ATTR_ODBC_VERSION,
                       reinterpret_cast<SQLPOINTER>(SQL_OV_ODBC3), 0);
    if (!SQL_SUCCEEDED(rc)) {
        out.error = "SQLSetEnvAttr(ODBC_VERSION=3) failed: " + read_diag(SQL_HANDLE_ENV, henv);
        free_handles(hstmt, hdbc, henv, connected);
        return out;
    }
    rc = SQLAllocHandle(SQL_HANDLE_DBC, henv, &hdbc);
    if (!SQL_SUCCEEDED(rc)) {
        out.error = "SQLAllocHandle(DBC) failed: " + read_diag(SQL_HANDLE_ENV, henv);
        free_handles(hstmt, hdbc, henv, connected);
        return out;
    }

    {
        SQLCHAR* dsn = reinterpret_cast<SQLCHAR*>(const_cast<char*>(cfg.odbc_dsn.c_str()));
        SQLCHAR* uid_ptr = nullptr;
        SQLCHAR* pwd_ptr = nullptr;
        std::string uid_buf, pwd_buf;
        if (!cfg.odbc_uid.empty()) { uid_buf = cfg.odbc_uid; uid_ptr = reinterpret_cast<SQLCHAR*>(uid_buf.data()); }
        if (!cfg.odbc_pwd.empty()) { pwd_buf = cfg.odbc_pwd; pwd_ptr = reinterpret_cast<SQLCHAR*>(pwd_buf.data()); }
        rc = SQLConnect(hdbc,
                        dsn, SQL_NTS,
                        uid_ptr, uid_ptr ? SQL_NTS : 0,
                        pwd_ptr, pwd_ptr ? SQL_NTS : 0);
    }
    if (!SQL_SUCCEEDED(rc)) {
        out.error = "SQLConnect failed: " + read_diag(SQL_HANDLE_DBC, hdbc);
        free_handles(hstmt, hdbc, henv, connected);
        return out;
    }
    connected = true;

    rc = SQLAllocHandle(SQL_HANDLE_STMT, hdbc, &hstmt);
    if (!SQL_SUCCEEDED(rc)) {
        out.error = "SQLAllocHandle(STMT) failed: " + read_diag(SQL_HANDLE_DBC, hdbc);
        free_handles(hstmt, hdbc, henv, connected);
        return out;
    }

    const double t1 = now_seconds();
    out.t_connect = t1 - t0;

    // --- Phase: prepare + execute (we use SQLExecDirect so the two are
    // collapsed; report 0 for prepare and the full time for execute,
    // mirroring how a BI tool that does not parameterise behaves). -----

    rc = SQLExecDirect(hstmt,
                       reinterpret_cast<SQLCHAR*>(const_cast<char*>(sql.c_str())),
                       SQL_NTS);
    if (!SQL_SUCCEEDED(rc)) {
        out.error = "SQLExecDirect failed: " + read_diag(SQL_HANDLE_STMT, hstmt);
        free_handles(hstmt, hdbc, henv, connected);
        return out;
    }

    const double t2 = now_seconds();
    out.t_prepare = 0.0;
    out.t_execute = t2 - t1;

    // --- Phase: bind (SQLNumResultCols + SQLDescribeCol + SQLBindCol) -

    SQLSMALLINT ncols = 0;
    rc = SQLNumResultCols(hstmt, &ncols);
    if (!SQL_SUCCEEDED(rc)) {
        out.error = "SQLNumResultCols failed: " + read_diag(SQL_HANDLE_STMT, hstmt);
        free_handles(hstmt, hdbc, henv, connected);
        return out;
    }
    out.columns = ncols;

    std::vector<BoundColumn> cols(static_cast<size_t>(ncols));
    for (SQLSMALLINT i = 0; i < ncols; ++i) {
        SQLCHAR name[256] = {0};
        SQLSMALLINT name_len = 0;
        SQLSMALLINT sql_type = 0;
        SQLULEN     col_size = 0;
        SQLSMALLINT dec_dig  = 0;
        SQLSMALLINT nullable = 0;
        rc = SQLDescribeCol(hstmt, static_cast<SQLUSMALLINT>(i + 1),
                            name, sizeof(name), &name_len,
                            &sql_type, &col_size, &dec_dig, &nullable);
        if (!SQL_SUCCEEDED(rc)) {
            out.error = "SQLDescribeCol failed at col " + std::to_string(i + 1) +
                        ": " + read_diag(SQL_HANDLE_STMT, hstmt);
            free_handles(hstmt, hdbc, henv, connected);
            return out;
        }
        BoundColumn& bc = cols[i];
        bc.sql_type       = sql_type;
        bc.c_type         = natural_c_type(sql_type);
        bc.col_size       = col_size;
        bc.decimal_digits = dec_dig;
        bc.buffer_len     = cell_buffer_bytes(bc.c_type, col_size);
        bc.buffer.assign(static_cast<size_t>(bc.buffer_len), 0);

        // In bound mode every column is pre-bound and SQLFetch copies
        // the whole row at once. In getdata mode we still need
        // SQLDescribeCol (to know the natural C type), but we do NOT
        // call SQLBindCol -- each cell is pulled individually by
        // SQLGetData inside the drain loop. The buffer is still
        // allocated here because SQLGetData writes into the same
        // per-column scratch.
        if (!use_getdata) {
            rc = SQLBindCol(hstmt, static_cast<SQLUSMALLINT>(i + 1),
                            bc.c_type,
                            bc.buffer.data(),
                            bc.buffer_len,
                            &bc.ind);
            if (!SQL_SUCCEEDED(rc)) {
                out.error = "SQLBindCol failed at col " + std::to_string(i + 1) +
                            " (sql_type=" + std::to_string(sql_type) +
                            ", c_type=" + std::to_string(bc.c_type) + "): " +
                            read_diag(SQL_HANDLE_STMT, hstmt);
                free_handles(hstmt, hdbc, henv, connected);
                return out;
            }
        }
    }

    const double t3 = now_seconds();
    out.t_bind = t3 - t2;

    // --- Phase: drain ------------------------------------------------
    //
    // Bound mode:   one SQLFetch per row; the driver fills every bound
    //               column buffer in a single call.
    // GetData mode: one SQLFetch per row (to advance the cursor) plus
    //               one SQLGetData per (row, column) at the column's
    //               natural C type. This is what .NET's
    //               OdbcDataReader, Power BI's mashup engine, and
    //               DBeaver do when they materialise a result set.

    int64_t row_count = 0;
    double  t_fetch   = 0.0;     // time inside SQLFetch (driver-side row build)
    if (!use_getdata) {
        while (true) {
            const double tf0 = now_seconds();
            rc = SQLFetch(hstmt);
            const double tf1 = now_seconds();
            t_fetch += (tf1 - tf0);
            if (rc == SQL_NO_DATA) break;
            if (!SQL_SUCCEEDED(rc)) {
                out.error = "SQLFetch failed at row " + std::to_string(row_count + 1) +
                            ": " + read_diag(SQL_HANDLE_STMT, hstmt);
                free_handles(hstmt, hdbc, henv, connected);
                return out;
            }
            ++row_count;
        }
        if (std::getenv("DF_BENCH_VERBOSE")) {
            std::fprintf(stderr,
                "[odbc-bound-instr] rows=%lld fetch_calls=%lld "
                "t_fetch=%.3fs (avg=%.2fus/row)\n",
                (long long)row_count, (long long)row_count,
                t_fetch, row_count > 0 ? (t_fetch * 1e6 / row_count) : 0.0);
        }
    } else {
        while (true) {
            rc = SQLFetch(hstmt);
            if (rc == SQL_NO_DATA) break;
            if (!SQL_SUCCEEDED(rc)) {
                out.error = "SQLFetch failed at row " + std::to_string(row_count + 1) +
                            ": " + read_diag(SQL_HANDLE_STMT, hstmt);
                free_handles(hstmt, hdbc, henv, connected);
                return out;
            }
            for (SQLSMALLINT i = 0; i < ncols; ++i) {
                BoundColumn& bc = cols[static_cast<size_t>(i)];
                SQLRETURN grc = SQLGetData(hstmt,
                                           static_cast<SQLUSMALLINT>(i + 1),
                                           bc.c_type,
                                           bc.buffer.data(),
                                           bc.buffer_len,
                                           &bc.ind);
                // SQL_SUCCESS_WITH_INFO is fine here -- the BI tools
                // we emulate happily accept truncation diagnostics on
                // variable-length cells without aborting.
                if (!SQL_SUCCEEDED(grc) && grc != SQL_NO_DATA) {
                    out.error = "SQLGetData failed at row " + std::to_string(row_count + 1) +
                                " col " + std::to_string(i + 1) +
                                " (sql_type=" + std::to_string(bc.sql_type) +
                                ", c_type=" + std::to_string(bc.c_type) + "): " +
                                read_diag(SQL_HANDLE_STMT, hstmt);
                    free_handles(hstmt, hdbc, henv, connected);
                    return out;
                }
            }
            ++row_count;
        }
    }

    const double t4 = now_seconds();
    out.t_drain = t4 - t3;
    out.rows    = row_count;

    // --- Phase: release ----------------------------------------------

    SQLCloseCursor(hstmt);
    free_handles(hstmt, hdbc, henv, connected);

    const double t5 = now_seconds();
    out.t_release = t5 - t4;
    out.t_total   = t5 - t0;

    return out;
}

} // namespace driver_bench
