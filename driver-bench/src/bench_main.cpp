// driver-bench: CLI entry point + result aggregation + report emission.
//
// One pass through main:
//
//   1. Parse args into a Config.
//   2. Build the bench SQL (either user-supplied or the synthetic
//      generate_series query parameterised by --rows).
//   3. For each enabled driver, run (warmups + iters) iterations,
//      collecting one PhaseTimings per iter.
//   4. Compute per-phase warm median (excluding warmups + errored iters).
//   5. Emit a human-readable summary on stdout. If --json or --json-out
//      is set, also emit a machine-readable record.

#include "bench_common.hpp"

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <functional>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

namespace driver_bench {

double now_seconds() {
    using clock = std::chrono::steady_clock;
    static const auto epoch = clock::now();
    auto now = clock::now();
    return std::chrono::duration<double>(now - epoch).count();
}

namespace {

double median(std::vector<double> v) {
    if (v.empty()) return 0.0;
    std::sort(v.begin(), v.end());
    const size_t n = v.size();
    if (n & 1u) return v[n / 2];
    return 0.5 * (v[n / 2 - 1] + v[n / 2]);
}

} // namespace

void compute_warm_median(DriverResult& r, int warmup_count) {
    std::vector<double> v_connect, v_prepare, v_execute, v_bind, v_drain, v_release, v_total;
    std::vector<int64_t> v_rows;
    std::vector<int>     v_cols;
    r.warmup_count   = warmup_count;
    r.measured_count = 0;
    r.error_count    = 0;
    for (size_t i = 0; i < r.iters.size(); ++i) {
        const auto& it = r.iters[i];
        if (static_cast<int>(i) < warmup_count) continue;
        if (!it.error.empty()) { ++r.error_count; continue; }
        ++r.measured_count;
        v_connect.push_back(it.t_connect);
        v_prepare.push_back(it.t_prepare);
        v_execute.push_back(it.t_execute);
        v_bind.push_back(it.t_bind);
        v_drain.push_back(it.t_drain);
        v_release.push_back(it.t_release);
        v_total.push_back(it.t_total);
        v_rows.push_back(it.rows);
        v_cols.push_back(it.columns);
    }
    PhaseTimings& m = r.warm_median;
    m.t_connect = median(v_connect);
    m.t_prepare = median(v_prepare);
    m.t_execute = median(v_execute);
    m.t_bind    = median(v_bind);
    m.t_drain   = median(v_drain);
    m.t_release = median(v_release);
    m.t_total   = median(v_total);
    m.rows      = v_rows.empty()    ? 0 : v_rows.front();
    m.columns   = v_cols.empty()    ? 0 : v_cols.front();
}

namespace {

// Build the synthetic wide-schema bench query. 24 columns, mixed types
// (BIGINT, DOUBLE, DECIMAL, BOOLEAN, TIMESTAMP, DATE, VARCHAR of several
// lengths), N rows generated server-side by generate_series. This is
// the public-bench default because it requires no fixture table and
// stresses every driver type-mapping path that matters for BI workloads.
//
// Every expression referencing the row index uses `(i+0)` instead of
// a bare `i`. This sidesteps a planner edge case in some DeltaForge
// builds where `cast(<bare generate_series column> AS decimal(p,s))`
// fails to plan past ~200 rows. The `+0` is constant-folded by the
// planner so it has no measured cost, but it changes the expression
// shape just enough that the cast-to-decimal lowering pattern matches
// uniformly. Subquery / CTE aliasing re-creates the failing shape, so
// we apply the workaround inline at every reference instead.
std::string build_synthetic_sql(int64_t rows) {
    std::ostringstream o;
    o << "SELECT \n"
         "  (i+0)                              AS id_i64,\n"
         "  (i+0) * 2                          AS doubled_i64,\n"
         "  ((i+0) % 32767)::smallint          AS small_i,\n"
         "  ((i+0) % 127)::tinyint             AS tiny_i,\n"
         "  ((i+0) % 2 = 0)                    AS flag_bool,\n"
         "  ((i+0) * 1.5)::double              AS f64_a,\n"
         "  ((i+0) / 7.0)::double              AS f64_b,\n"
         "  cast((i+0)        AS decimal(18,4)) AS dec18,\n"
         "  cast((i+0) * 0.01 AS decimal(28,8)) AS dec28,\n"
         "  cast((i+0) * 1.0  AS decimal(10,2)) AS price,\n"
         "  cast(timestamp '2026-01-01 00:00:00' + ((i+0) || ' seconds')::interval AS timestamp) AS ts_a,\n"
         "  cast(timestamp '2020-06-15 12:00:00' + (((i+0)*60) || ' seconds')::interval AS timestamp) AS ts_b,\n"
         "  cast(date '2026-01-01' + ((i+0) % 365) AS date) AS d_a,\n"
         "  md5((i+0)::text)                   AS md5_hex,\n"
         "  ('row-' || (i+0)::text)            AS label_v32,\n"
         "  repeat('x', 16)                    AS pad_v16,\n"
         "  repeat('y', 64)                    AS pad_v64,\n"
         "  repeat('z', 128)                   AS pad_v128,\n"
         "  cast((i+0)        AS varchar(40))  AS i_as_text,\n"
         "  cast((i+0) * 3.14 AS varchar(40))  AS pi_as_text,\n"
         "  upper(md5((i+0)::text))            AS md5_upper,\n"
         "  concat('id-', lpad((i+0)::text, 10, '0')) AS padded_id,\n"
         "  cast((i+0) % 256 AS tinyint)       AS bucket,\n"
         "  cast((i+0) % 1000 AS integer)      AS dim_k\n"
         "FROM generate_series(1, " << rows << ") AS t(i)";
    return o.str();
}

void print_summary_table(const std::vector<DriverResult>& results,
                         const Config& cfg,
                         const std::string& sql) {
    auto fmt = [](double s) {
        char buf[64];
        if (s >= 1.0)        std::snprintf(buf, sizeof(buf), "%9.3f s", s);
        else if (s >= 0.001) std::snprintf(buf, sizeof(buf), "%9.2f ms", s * 1000.0);
        else                 std::snprintf(buf, sizeof(buf), "%9.1f us", s * 1000000.0);
        return std::string(buf);
    };

    std::printf("\n");
    std::printf("================================================================\n");
    std::printf(" DeltaForge driver-bench results\n");
    std::printf("================================================================\n");
    std::printf(" warmups       : %d\n", cfg.warmups);
    std::printf(" measured iters: %d\n", cfg.iters);
    std::printf(" rows requested: %lld\n", static_cast<long long>(cfg.rows));
    if (cfg.custom_sql.empty()) {
        std::printf(" query         : synthetic 24-col wide generate_series(%lld)\n",
                    static_cast<long long>(cfg.rows));
    } else {
        std::printf(" query         : (custom, %zu chars)\n", sql.size());
    }
    std::printf("\n");
    std::printf(" %-10s | %5s | %5s | %12s | %12s | %12s | %12s | %12s | %12s | %12s | %10s\n",
                "driver", "rows", "cols",
                "t_connect", "t_execute", "t_bind", "t_drain", "t_release",
                "t_total", "rows/sec", "errors");
    std::printf(" -----------+-------+-------+--------------+--------------+--------------+--------------+--------------+--------------+--------------+-----------\n");
    for (const auto& r : results) {
        const auto& m = r.warm_median;
        double rps = (m.t_total > 0.0) ? (static_cast<double>(m.rows) / m.t_total) : 0.0;
        char rps_buf[32];
        if (rps >= 1e6)      std::snprintf(rps_buf, sizeof(rps_buf), "%8.2f M/s", rps / 1e6);
        else if (rps >= 1e3) std::snprintf(rps_buf, sizeof(rps_buf), "%8.1f k/s", rps / 1e3);
        else                 std::snprintf(rps_buf, sizeof(rps_buf), "%8.0f /s",  rps);
        std::printf(" %-10s | %5lld | %5d | %12s | %12s | %12s | %12s | %12s | %12s | %12s | %5d/%-4d\n",
                    r.driver.c_str(),
                    static_cast<long long>(m.rows),
                    m.columns,
                    fmt(m.t_connect).c_str(),
                    fmt(m.t_execute).c_str(),
                    fmt(m.t_bind).c_str(),
                    fmt(m.t_drain).c_str(),
                    fmt(m.t_release).c_str(),
                    fmt(m.t_total).c_str(),
                    rps_buf,
                    r.error_count, r.measured_count + r.error_count);
    }

    // Multi-driver comparison. ADBC anchors the comparison because the
    // bench's central claim is that the Arrow-stream consumption path
    // beats both ODBC consumption modes (especially the per-cell
    // SQLGetData one that BI tools use in practice).
    const DriverResult* odbc_bound   = nullptr;
    const DriverResult* odbc_getdata = nullptr;
    const DriverResult* adbc         = nullptr;
    for (const auto& r : results) {
        if (r.driver == "odbc-bound"   && r.measured_count > 0) odbc_bound   = &r;
        if (r.driver == "odbc-getdata" && r.measured_count > 0) odbc_getdata = &r;
        if (r.driver == "adbc"         && r.measured_count > 0) adbc         = &r;
    }
    auto report_pair = [&](const char* label, const DriverResult* a, const DriverResult* b) {
        if (!a || !b) return;
        if (a->warm_median.t_total <= 0.0 || b->warm_median.t_total <= 0.0) return;
        const double speedup = a->warm_median.t_total / b->warm_median.t_total;
        std::printf(" %s: %.2fx on t_total", label, speedup);
        if (a->warm_median.t_drain > 0.0 && b->warm_median.t_drain > 0.0) {
            std::printf(", %.2fx on t_drain", a->warm_median.t_drain / b->warm_median.t_drain);
        }
        std::printf("\n");
    };
    if ((odbc_bound || odbc_getdata) && adbc) {
        std::printf("\n");
        report_pair("ODBC (bound)   vs ADBC", odbc_bound,   adbc);
        report_pair("ODBC (getdata) vs ADBC", odbc_getdata, adbc);
        if (odbc_bound && odbc_getdata) {
            report_pair("ODBC getdata   vs ODBC bound", odbc_getdata, odbc_bound);
            std::printf(" (the getdata vs bound gap is the cost of per-cell SQLGetData, which is what .NET / Power BI pays)\n");
        }
    }

    // Show per-iteration detail at the bottom so a regression is
    // immediately visible without re-running.
    for (const auto& r : results) {
        std::printf("\n %s iterations:\n", r.driver.c_str());
        for (size_t i = 0; i < r.iters.size(); ++i) {
            const auto& it = r.iters[i];
            const char* tag = (static_cast<int>(i) < r.warmup_count) ? "warm" : "meas";
            if (!it.error.empty()) {
                std::printf("   [%s %2zu] ERROR: %s\n", tag, i, it.error.c_str());
            } else {
                std::printf("   [%s %2zu] total=%s rows=%lld cols=%d\n",
                            tag, i, fmt(it.t_total).c_str(),
                            static_cast<long long>(it.rows), it.columns);
            }
        }
    }
    std::printf("\n");
}

std::string emit_json(const std::vector<DriverResult>& results,
                      const Config& cfg, const std::string& sql) {
    auto esc = [](const std::string& s) {
        std::string o;
        o.reserve(s.size() + 8);
        for (char c : s) {
            switch (c) {
                case '"':  o += "\\\""; break;
                case '\\': o += "\\\\"; break;
                case '\n': o += "\\n";  break;
                case '\r': o += "\\r";  break;
                case '\t': o += "\\t";  break;
                default:
                    if (static_cast<unsigned char>(c) < 0x20) {
                        char b[8];
                        std::snprintf(b, sizeof(b), "\\u%04x", c);
                        o += b;
                    } else {
                        o += c;
                    }
            }
        }
        return o;
    };
    std::ostringstream o;
    o << "{";
    o << "\"version\":1,";
    o << "\"warmups\":" << cfg.warmups << ",";
    o << "\"iters\":" << cfg.iters << ",";
    o << "\"rows_requested\":" << cfg.rows << ",";
    o << "\"sql\":\"" << esc(sql) << "\",";
    o << "\"drivers\":[";
    for (size_t k = 0; k < results.size(); ++k) {
        const auto& r = results[k];
        const auto& m = r.warm_median;
        if (k) o << ",";
        o << "{";
        o << "\"driver\":\"" << r.driver << "\",";
        o << "\"measured\":" << r.measured_count << ",";
        o << "\"errors\":" << r.error_count << ",";
        o << "\"rows\":" << m.rows << ",";
        o << "\"columns\":" << m.columns << ",";
        o << "\"warm_median\":{";
        o << "\"t_connect\":" << m.t_connect << ",";
        o << "\"t_prepare\":" << m.t_prepare << ",";
        o << "\"t_execute\":" << m.t_execute << ",";
        o << "\"t_bind\":" << m.t_bind << ",";
        o << "\"t_drain\":" << m.t_drain << ",";
        o << "\"t_release\":" << m.t_release << ",";
        o << "\"t_total\":" << m.t_total;
        o << "},";
        o << "\"iters\":[";
        for (size_t i = 0; i < r.iters.size(); ++i) {
            const auto& it = r.iters[i];
            if (i) o << ",";
            o << "{";
            o << "\"is_warmup\":" << (static_cast<int>(i) < r.warmup_count ? "true" : "false") << ",";
            o << "\"t_connect\":" << it.t_connect << ",";
            o << "\"t_execute\":" << it.t_execute << ",";
            o << "\"t_bind\":" << it.t_bind << ",";
            o << "\"t_drain\":" << it.t_drain << ",";
            o << "\"t_release\":" << it.t_release << ",";
            o << "\"t_total\":" << it.t_total << ",";
            o << "\"rows\":" << it.rows << ",";
            o << "\"columns\":" << it.columns;
            if (!it.error.empty()) {
                o << ",\"error\":\"" << esc(it.error) << "\"";
            }
            o << "}";
        }
        o << "]";
        o << "}";
    }
    o << "]";
    o << "}";
    return o.str();
}

void print_usage(const char* prog) {
    std::fprintf(stderr,
        "Usage: %s [options]\n"
        "  --driver odbc|adbc|both    which path(s) to exercise (default: both)\n"
        "  --warmups N                discarded warmup iterations (default: 1)\n"
        "  --iters N                  measured iterations (default: 5)\n"
        "  --rows N                   synthetic rows for generate_series (default: 10000000)\n"
        "  --sql 'SELECT ...'         custom query (skips synthetic; --rows ignored)\n"
        "  --odbc-mode MODE           bound | getdata | both (default: both)\n"
        "                             bound   = SQLBindCol + SQLFetch (Linux unixODBC fast path)\n"
        "                             getdata = SQLGetData per cell (.NET / Power BI pattern)\n"
        "                             both    = run both and report side by side\n"
        "\n"
        " ODBC:\n"
        "  --odbc-dsn NAME            DSN name from /etc/odbc.ini or ~/.odbc.ini (default: deltaforge)\n"
        "  --odbc-uid USER            override DSN's Uid\n"
        "  --odbc-pwd PASS            override DSN's Pwd\n"
        "\n"
        " ADBC:\n"
        "  --adbc-so PATH             driver .so to dlopen (default: build/adbc-linux-x64/libdeltaforge_adbc.so.1)\n"
        "  --adbc-uri URL             control plane URL, e.g. http://host:3000\n"
        "  --adbc-compute URL         optional compute override\n"
        "  --adbc-user USER\n"
        "  --adbc-pwd  PASS\n"
        "  --adbc-token TOK           session token (overrides user+pwd)\n"
        "\n"
        " Output:\n"
        "  --json                     emit JSON record on stdout after the table\n"
        "  --json-out PATH            write JSON to PATH (does not affect stdout)\n"
        "  --help                     this message\n",
        prog);
}

bool need_value(int i, int argc, const char* flag) {
    if (i + 1 >= argc) {
        std::fprintf(stderr, "error: %s requires a value\n", flag);
        return false;
    }
    return true;
}

int parse_args(int argc, char** argv, Config& cfg) {
    for (int i = 1; i < argc; ++i) {
        const char* a = argv[i];
        if (!std::strcmp(a, "--help") || !std::strcmp(a, "-h")) {
            print_usage(argv[0]);
            return 1;
        } else if (!std::strcmp(a, "--driver")) {
            if (!need_value(i, argc, a)) return 2;
            std::string v = argv[++i];
            if      (v == "odbc") { cfg.run_odbc = true;  cfg.run_adbc = false; }
            else if (v == "adbc") { cfg.run_odbc = false; cfg.run_adbc = true;  }
            else if (v == "both") { cfg.run_odbc = true;  cfg.run_adbc = true;  }
            else { std::fprintf(stderr, "error: --driver must be odbc|adbc|both\n"); return 2; }
        } else if (!std::strcmp(a, "--warmups")) {
            if (!need_value(i, argc, a)) return 2;
            cfg.warmups = std::atoi(argv[++i]);
        } else if (!std::strcmp(a, "--iters")) {
            if (!need_value(i, argc, a)) return 2;
            cfg.iters = std::atoi(argv[++i]);
        } else if (!std::strcmp(a, "--rows")) {
            if (!need_value(i, argc, a)) return 2;
            cfg.rows = std::atoll(argv[++i]);
        } else if (!std::strcmp(a, "--sql")) {
            if (!need_value(i, argc, a)) return 2;
            cfg.custom_sql = argv[++i];
        } else if (!std::strcmp(a, "--odbc-mode")) {
            if (!need_value(i, argc, a)) return 2;
            std::string v = argv[++i];
            if (v != "bound" && v != "getdata" && v != "both") {
                std::fprintf(stderr, "error: --odbc-mode must be bound|getdata|both, got '%s'\n", v.c_str());
                return 2;
            }
            cfg.odbc_mode = v;
        } else if (!std::strcmp(a, "--odbc-dsn")) {
            if (!need_value(i, argc, a)) return 2;
            cfg.odbc_dsn = argv[++i];
        } else if (!std::strcmp(a, "--odbc-uid")) {
            if (!need_value(i, argc, a)) return 2;
            cfg.odbc_uid = argv[++i];
        } else if (!std::strcmp(a, "--odbc-pwd")) {
            if (!need_value(i, argc, a)) return 2;
            cfg.odbc_pwd = argv[++i];
        } else if (!std::strcmp(a, "--adbc-so")) {
            if (!need_value(i, argc, a)) return 2;
            cfg.adbc_so = argv[++i];
        } else if (!std::strcmp(a, "--adbc-uri")) {
            if (!need_value(i, argc, a)) return 2;
            cfg.adbc_uri = argv[++i];
        } else if (!std::strcmp(a, "--adbc-compute")) {
            if (!need_value(i, argc, a)) return 2;
            cfg.adbc_compute = argv[++i];
        } else if (!std::strcmp(a, "--adbc-user")) {
            if (!need_value(i, argc, a)) return 2;
            cfg.adbc_user = argv[++i];
        } else if (!std::strcmp(a, "--adbc-pwd")) {
            if (!need_value(i, argc, a)) return 2;
            cfg.adbc_pwd = argv[++i];
        } else if (!std::strcmp(a, "--adbc-token")) {
            if (!need_value(i, argc, a)) return 2;
            cfg.adbc_token = argv[++i];
        } else if (!std::strcmp(a, "--json")) {
            cfg.emit_json = true;
        } else if (!std::strcmp(a, "--json-out")) {
            if (!need_value(i, argc, a)) return 2;
            cfg.json_path = argv[++i];
        } else {
            std::fprintf(stderr, "error: unknown flag %s\n", a);
            print_usage(argv[0]);
            return 2;
        }
    }

    if (cfg.warmups < 0) cfg.warmups = 0;
    if (cfg.iters   < 1) cfg.iters   = 1;
    if (cfg.rows    < 1) cfg.rows    = 1;
    if (cfg.run_adbc && cfg.adbc_uri.empty()) {
        std::fprintf(stderr, "error: --adbc-uri is required when running ADBC (e.g. http://host:3000)\n");
        return 2;
    }
    return 0;
}

// Generic runner shared by the ODBC (bound + getdata) and ADBC paths.
// The closure makes per-driver state (e.g. the ODBC mode string)
// transparent to this orchestrator.
void run_driver_closure(const std::string& name,
                        const std::function<PhaseTimings(const Config&, const std::string&)>& fn,
                        const Config& cfg, const std::string& sql,
                        std::vector<DriverResult>& out) {
    DriverResult r;
    r.driver = name;
    r.sql    = sql;
    const int total_iters = cfg.warmups + cfg.iters;
    r.iters.reserve(total_iters);
    std::printf(" [%s] running %d warmup(s) + %d measured iter(s)...\n",
                name.c_str(), cfg.warmups, cfg.iters);
    std::fflush(stdout);
    for (int i = 0; i < total_iters; ++i) {
        PhaseTimings it = fn(cfg, sql);
        r.iters.push_back(it);
        std::printf("   [%s %2d] %s rows=%lld total=%.3fs\n",
                    name.c_str(), i,
                    it.error.empty() ? "OK" : "ERR",
                    static_cast<long long>(it.rows), it.t_total);
        std::fflush(stdout);
    }
    compute_warm_median(r, cfg.warmups);
    out.push_back(std::move(r));
}

} // namespace
} // namespace driver_bench

int main(int argc, char** argv) {
    using namespace driver_bench;
    Config cfg;
    int parse_rc = parse_args(argc, argv, cfg);
    if (parse_rc != 0) {
        return parse_rc == 1 ? 0 : parse_rc;
    }

    const std::string sql = cfg.custom_sql.empty()
        ? build_synthetic_sql(cfg.rows)
        : cfg.custom_sql;

    std::vector<DriverResult> results;
    if (cfg.run_odbc) {
        if (cfg.odbc_mode == "bound" || cfg.odbc_mode == "both") {
            run_driver_closure(
                "odbc-bound",
                [](const Config& c, const std::string& s) {
                    return run_odbc_iter(c, s, "bound");
                },
                cfg, sql, results);
        }
        if (cfg.odbc_mode == "getdata" || cfg.odbc_mode == "both") {
            run_driver_closure(
                "odbc-getdata",
                [](const Config& c, const std::string& s) {
                    return run_odbc_iter(c, s, "getdata");
                },
                cfg, sql, results);
        }
    }
    if (cfg.run_adbc) {
        run_driver_closure(
            "adbc",
            [](const Config& c, const std::string& s) {
                return run_adbc_iter(c, s);
            },
            cfg, sql, results);
    }

    print_summary_table(results, cfg, sql);

    if (cfg.emit_json || !cfg.json_path.empty()) {
        std::string j = emit_json(results, cfg, sql);
        if (cfg.emit_json) {
            std::cout << j << "\n";
        }
        if (!cfg.json_path.empty()) {
            std::ofstream f(cfg.json_path);
            if (!f) {
                std::fprintf(stderr, "error: could not write %s\n", cfg.json_path.c_str());
                return 3;
            }
            f << j;
        }
    }
    return 0;
}
