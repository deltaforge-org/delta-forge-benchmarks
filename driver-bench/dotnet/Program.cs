// driver-bench .NET sub-bench.
//
// Real .NET consumer-side measurement of the DeltaForge ODBC and ADBC
// drivers, exercising the access patterns Power BI Desktop, Power BI
// Service via Gateway, .NET Tableau, DBeaver, and any other .NET-based
// BI client actually use.
//
// Three modes:
//
//   odbc-reader   System.Data.Odbc.OdbcDataReader, the canonical
//                 .NET ODBC consumer. Internally uses per-cell
//                 SQLGetData at the column's native C type. This is
//                 what Power BI's Mashup engine, the .NET Data
//                 Connectivity stack, and EF Core's OdbcConnection
//                 all reduce to under the hood. Linux .NET on
//                 unixODBC drives the same code path as Windows
//                 .NET on the Driver Manager: managed wrappers over
//                 SQLBindCol / SQLFetch / SQLGetData.
//
//   adbc-arrow    Apache.Arrow.Adbc.AdbcDriverLoader -> AdbcStatement
//                 -> ExecuteQuery -> QueryResult.Stream. Pulls Arrow
//                 record batches from the DeltaForge ADBC bridge with
//                 zero per-cell conversion. This is the modern path
//                 Power BI Desktop 2.145.1105.0+ uses when an ADBC
//                 driver is registered.
//
//   both          Run both modes back-to-back in the same invocation,
//                 same SQL, same control plane. Default.
//
// Methodology mirrors the C++ harness in ../build/driver_bench: warm-
// median across N measured iterations, M discarded warmups, per-phase
// wall time (connect / execute / drain / release). The two sub-benches
// publish to the same JSON shape so results/run-*.json from either
// can be ingested by the same downstream tooling.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;

namespace DriverBench;

internal sealed class Config
{
    public string Mode { get; set; } = "both";
    public int    Warmups { get; set; } = 1;
    public int    Iters   { get; set; } = 3;
    public string Sql { get; set; } =
        "SELECT * FROM demo.retail.fact_sales LIMIT 1000000";

    public string OdbcDsn { get; set; } = "deltaforge";
    public string OdbcUid { get; set; } = "";
    public string OdbcPwd { get; set; } = "";

    public string AdbcSo { get; set; } =
        "/usr/local/lib/libdeltaforge_adbc.so";
    public string AdbcUri { get; set; } = "";
    public string AdbcUser { get; set; } = "";
    public string AdbcPwd { get; set; } = "";
    public string AdbcToken { get; set; } = "";
    public string AdbcCompute { get; set; } = "";

    public string? JsonOut { get; set; }
}

internal sealed class PhaseTimings
{
    public double T_connect { get; set; }
    public double T_execute { get; set; }
    public double T_drain   { get; set; }
    public double T_release { get; set; }
    public double T_total   { get; set; }
    public long   Rows      { get; set; }
    public int    Columns   { get; set; }
    public string? Error    { get; set; }
}

internal sealed class DriverResult
{
    public string Driver { get; set; } = "";
    public List<PhaseTimings> Iters { get; set; } = new();
    public PhaseTimings WarmMedian { get; set; } = new();
    public int WarmupCount { get; set; }
    public int MeasuredCount { get; set; }
    public int ErrorCount { get; set; }
    public string Sql { get; set; } = "";
}

internal static class Program
{
    public static int Main(string[] args)
    {
        var cfg = ParseArgs(args);
        if (cfg is null) return 2;

        var results = new List<DriverResult>();

        if (cfg.Mode is "odbc-reader" or "both")
        {
            RunDriver("odbc-reader", cfg, OdbcPath.RunIter, results);
        }
        if (cfg.Mode is "adbc-arrow" or "both")
        {
            RunDriver("adbc-arrow", cfg, AdbcPath.RunIter, results);
        }

        PrintSummary(results, cfg);

        if (!string.IsNullOrEmpty(cfg.JsonOut))
        {
            File.WriteAllText(cfg.JsonOut!, EmitJson(results, cfg));
            Console.WriteLine($"[dotnet] JSON written to {cfg.JsonOut}");
        }
        return 0;
    }

    private static Config? ParseArgs(string[] args)
    {
        var cfg = new Config();
        for (int i = 0; i < args.Length; i++)
        {
            string a = args[i];
            string Next() {
                if (i + 1 >= args.Length) {
                    Console.Error.WriteLine($"error: {a} requires a value");
                    Environment.Exit(2);
                }
                return args[++i];
            }
            switch (a)
            {
                case "--mode":         cfg.Mode = Next(); break;
                case "--warmups":      cfg.Warmups = int.Parse(Next()); break;
                case "--iters":        cfg.Iters = int.Parse(Next()); break;
                case "--sql":          cfg.Sql = Next(); break;
                case "--odbc-dsn":     cfg.OdbcDsn = Next(); break;
                case "--odbc-uid":     cfg.OdbcUid = Next(); break;
                case "--odbc-pwd":     cfg.OdbcPwd = Next(); break;
                case "--adbc-so":      cfg.AdbcSo = Next(); break;
                case "--adbc-uri":     cfg.AdbcUri = Next(); break;
                case "--adbc-user":    cfg.AdbcUser = Next(); break;
                case "--adbc-pwd":     cfg.AdbcPwd = Next(); break;
                case "--adbc-token":   cfg.AdbcToken = Next(); break;
                case "--adbc-compute": cfg.AdbcCompute = Next(); break;
                case "--json-out":     cfg.JsonOut = Next(); break;
                case "--help":
                case "-h":
                    PrintUsage();
                    return null;
                default:
                    Console.Error.WriteLine($"error: unknown flag {a}");
                    PrintUsage();
                    return null;
            }
        }
        if (cfg.Mode is not ("odbc-reader" or "adbc-arrow" or "both"))
        {
            Console.Error.WriteLine($"error: --mode must be odbc-reader|adbc-arrow|both");
            return null;
        }
        if ((cfg.Mode == "adbc-arrow" || cfg.Mode == "both") && string.IsNullOrEmpty(cfg.AdbcUri))
        {
            Console.Error.WriteLine("error: --adbc-uri is required for ADBC mode");
            return null;
        }
        return cfg;
    }

    private static void PrintUsage()
    {
        Console.WriteLine(@"Usage: dotnet run -- [options]
  --mode MODE             odbc-reader | adbc-arrow | both (default: both)
  --warmups N             discarded warmup iterations (default: 1)
  --iters N               measured iterations (default: 3)
  --sql 'SELECT ...'      query to run

 ODBC (odbc-reader mode):
  --odbc-dsn NAME         DSN name (default: deltaforge)
  --odbc-uid USER         override DSN Uid
  --odbc-pwd PASS         override DSN Pwd

 ADBC (adbc-arrow mode):
  --adbc-so PATH          driver .so to dlopen
  --adbc-uri URL          control plane URL, required for ADBC
  --adbc-compute URL      optional compute override
  --adbc-user USER
  --adbc-pwd  PASS
  --adbc-token TOK        session token

 Output:
  --json-out PATH         write JSON result to PATH
  --help                  this message
");
    }

    private static void RunDriver(
        string name,
        Config cfg,
        Func<Config, PhaseTimings> iter,
        List<DriverResult> results)
    {
        var r = new DriverResult { Driver = name, Sql = cfg.Sql };
        int total = cfg.Warmups + cfg.Iters;
        Console.WriteLine($" [{name}] running {cfg.Warmups} warmup(s) + {cfg.Iters} measured iter(s)...");
        for (int i = 0; i < total; i++)
        {
            var it = iter(cfg);
            r.Iters.Add(it);
            Console.WriteLine(
                $"   [{name} {i,2}] {(it.Error is null ? "OK" : "ERR")} rows={it.Rows} total={it.T_total:0.000}s"
                + (it.Error is null ? "" : $"  msg={it.Error}"));
        }
        ComputeWarmMedian(r, cfg.Warmups);
        results.Add(r);
    }

    private static void ComputeWarmMedian(DriverResult r, int warmups)
    {
        var connect = new List<double>();
        var execute = new List<double>();
        var drain   = new List<double>();
        var release = new List<double>();
        var total   = new List<double>();
        var rows    = new List<long>();
        var cols    = new List<int>();
        r.WarmupCount   = warmups;
        r.MeasuredCount = 0;
        r.ErrorCount    = 0;
        for (int i = 0; i < r.Iters.Count; i++)
        {
            var it = r.Iters[i];
            if (i < warmups) continue;
            if (it.Error is not null) { r.ErrorCount++; continue; }
            r.MeasuredCount++;
            connect.Add(it.T_connect);
            execute.Add(it.T_execute);
            drain.Add(it.T_drain);
            release.Add(it.T_release);
            total.Add(it.T_total);
            rows.Add(it.Rows);
            cols.Add(it.Columns);
        }
        double Med(List<double> v) {
            if (v.Count == 0) return 0;
            v.Sort();
            int n = v.Count;
            return (n & 1) == 1 ? v[n / 2] : 0.5 * (v[n / 2 - 1] + v[n / 2]);
        }
        r.WarmMedian = new PhaseTimings
        {
            T_connect = Med(connect),
            T_execute = Med(execute),
            T_drain   = Med(drain),
            T_release = Med(release),
            T_total   = Med(total),
            Rows      = rows.Count == 0 ? 0 : rows[0],
            Columns   = cols.Count == 0 ? 0 : cols[0],
        };
    }

    private static string FormatSeconds(double s)
    {
        if (s >= 1.0)        return $"{s,9:0.000} s";
        if (s >= 0.001)      return $"{(s * 1000),9:0.00} ms";
        return $"{(s * 1_000_000),9:0.0} us";
    }

    private static void PrintSummary(List<DriverResult> results, Config cfg)
    {
        Console.WriteLine();
        Console.WriteLine("================================================================");
        Console.WriteLine(" DeltaForge driver-bench .NET results");
        Console.WriteLine("================================================================");
        Console.WriteLine($" warmups       : {cfg.Warmups}");
        Console.WriteLine($" measured iters: {cfg.Iters}");
        Console.WriteLine($" sql           : {(cfg.Sql.Length > 80 ? cfg.Sql.Substring(0, 77) + "..." : cfg.Sql)}");
        Console.WriteLine();
        Console.WriteLine($" {"driver",-13} | {"rows",7} | {"cols",4} | {"t_connect",12} | {"t_execute",12} | {"t_drain",12} | {"t_release",12} | {"t_total",12} | {"rows/sec",12} | errors");
        Console.WriteLine($" --------------+---------+------+--------------+--------------+--------------+--------------+--------------+--------------+--------");
        foreach (var r in results)
        {
            var m = r.WarmMedian;
            double rps = m.T_total > 0 ? m.Rows / m.T_total : 0;
            string rpsStr =
                rps >= 1e6 ? $"{rps / 1e6,8:0.00} M/s" :
                rps >= 1e3 ? $"{rps / 1e3,8:0.0} k/s" :
                             $"{rps,8:0} /s";
            Console.WriteLine($" {r.Driver,-13} | {m.Rows,7} | {m.Columns,4} | {FormatSeconds(m.T_connect),12} | {FormatSeconds(m.T_execute),12} | {FormatSeconds(m.T_drain),12} | {FormatSeconds(m.T_release),12} | {FormatSeconds(m.T_total),12} | {rpsStr,12} | {r.ErrorCount}/{r.MeasuredCount + r.ErrorCount}");
        }

        var odbc = results.FirstOrDefault(r => r.Driver == "odbc-reader" && r.MeasuredCount > 0);
        var adbc = results.FirstOrDefault(r => r.Driver == "adbc-arrow"  && r.MeasuredCount > 0);
        if (odbc is not null && adbc is not null && adbc.WarmMedian.T_total > 0)
        {
            double speedup = odbc.WarmMedian.T_total / adbc.WarmMedian.T_total;
            double drainRatio = adbc.WarmMedian.T_drain > 0
                ? odbc.WarmMedian.T_drain / adbc.WarmMedian.T_drain
                : 0;
            Console.WriteLine();
            Console.WriteLine($" .NET ODBC vs ADBC: {speedup:0.00}x on t_total, {drainRatio:0.00}x on t_drain (per-cell SQLGetData vs Arrow stream)");
        }
        Console.WriteLine();
        foreach (var r in results)
        {
            Console.WriteLine($" {r.Driver} iterations:");
            for (int i = 0; i < r.Iters.Count; i++)
            {
                var it = r.Iters[i];
                string tag = i < r.WarmupCount ? "warm" : "meas";
                if (it.Error is not null) {
                    Console.WriteLine($"   [{tag} {i,2}] ERROR: {it.Error}");
                } else {
                    Console.WriteLine($"   [{tag} {i,2}] total={FormatSeconds(it.T_total)} rows={it.Rows} cols={it.Columns}");
                }
            }
        }
    }

    private static string EmitJson(List<DriverResult> results, Config cfg)
    {
        var doc = new {
            version = 1,
            harness = "dotnet",
            warmups = cfg.Warmups,
            iters   = cfg.Iters,
            sql     = cfg.Sql,
            drivers = results.Select(r => new {
                driver         = r.Driver,
                measured       = r.MeasuredCount,
                errors         = r.ErrorCount,
                rows           = r.WarmMedian.Rows,
                columns        = r.WarmMedian.Columns,
                warm_median    = new {
                    t_connect = r.WarmMedian.T_connect,
                    t_execute = r.WarmMedian.T_execute,
                    t_drain   = r.WarmMedian.T_drain,
                    t_release = r.WarmMedian.T_release,
                    t_total   = r.WarmMedian.T_total,
                },
                iters = r.Iters.Select((it, idx) => new {
                    is_warmup = idx < r.WarmupCount,
                    t_connect = it.T_connect,
                    t_execute = it.T_execute,
                    t_drain   = it.T_drain,
                    t_release = it.T_release,
                    t_total   = it.T_total,
                    rows      = it.Rows,
                    columns   = it.Columns,
                    error     = it.Error,
                })
            })
        };
        return JsonSerializer.Serialize(doc, new JsonSerializerOptions { WriteIndented = true });
    }
}
