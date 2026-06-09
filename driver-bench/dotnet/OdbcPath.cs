// driver-bench .NET: ODBC consumer-side path.
//
// Drives System.Data.Odbc.OdbcDataReader against the configured DSN.
// This is the canonical .NET ODBC consumption shape: the same code
// path Power BI Desktop's Mashup engine, Linq-to-SQL over OdbcConnection,
// EF Core's OdbcConnection, and DBeaver's .NET branches use.
//
// Internally OdbcDataReader translates each GetValue(i) call into
// SQLGetData on the underlying unixODBC driver-manager handle. This is
// the per-cell access pattern that ADBC was designed to obsolete. The
// bench measures this pattern honestly: no SQLBindCol fast path, no
// row-buffer pre-allocation by the consumer, just the GetValue loop
// that real .NET BI clients drive.

using System;
using System.Data;
using System.Data.Odbc;
using System.Diagnostics;
using System.Text;

namespace DriverBench;

internal static class OdbcPath
{
    public static PhaseTimings RunIter(Config cfg)
    {
        var p = new PhaseTimings();
        var sw = Stopwatch.StartNew();

        OdbcConnection? conn = null;
        OdbcCommand?    cmd  = null;
        OdbcDataReader? rdr  = null;

        try
        {
            // ----- t_connect -----------------------------------------
            // The DSN-based connection string lets unixODBC resolve the
            // driver path + endpoint from /etc/odbc.ini or ~/.odbc.ini
            // exactly the way an installed .NET BI tool would. Uid/Pwd
            // here override the DSN's stored values when supplied so the
            // bench works against shared DSNs without baking credentials
            // into the DSN file.
            var csb = new StringBuilder();
            csb.Append("DSN=").Append(cfg.OdbcDsn).Append(';');
            if (!string.IsNullOrEmpty(cfg.OdbcUid))
                csb.Append("Uid=").Append(cfg.OdbcUid).Append(';');
            if (!string.IsNullOrEmpty(cfg.OdbcPwd))
                csb.Append("Pwd=").Append(cfg.OdbcPwd).Append(';');

            conn = new OdbcConnection(csb.ToString());
            conn.Open();
            double tConnect = sw.Elapsed.TotalSeconds;
            sw.Restart();

            // ----- t_execute -----------------------------------------
            cmd = new OdbcCommand(cfg.Sql, conn);
            // CommandBehavior.SequentialAccess tells the data reader
            // to read each row's columns left-to-right without
            // re-buffering. This is what the Power BI Mashup engine
            // sets when it wants the lowest-overhead .NET access
            // pattern; it forces every cell to a single SQLGetData
            // call at the natural type and disables any internal
            // caching the reader would otherwise do.
            rdr = cmd.ExecuteReader(CommandBehavior.SequentialAccess);
            double tExecute = sw.Elapsed.TotalSeconds;
            sw.Restart();

            // ----- t_drain -------------------------------------------
            int ncols = rdr.FieldCount;
            long rowCount = 0;
            // Bind a temp row buffer once so the per-row hot path
            // does not allocate. GetValue returns object so we read
            // every column to force the marshalling, then discard.
            object[] row = new object[ncols];
            while (rdr.Read())
            {
                int got = rdr.GetValues(row);
                if (got != ncols)
                {
                    // Defensive: should never happen for a uniform
                    // result set, but if it does the row count would
                    // be wrong otherwise.
                    throw new InvalidOperationException(
                        $"OdbcDataReader.GetValues returned {got} != {ncols}");
                }
                rowCount++;
            }
            double tDrain = sw.Elapsed.TotalSeconds;
            sw.Restart();

            // ----- t_release -----------------------------------------
            rdr.Close();
            cmd.Dispose();
            conn.Close();
            double tRelease = sw.Elapsed.TotalSeconds;

            p.T_connect = tConnect;
            p.T_execute = tExecute;
            p.T_drain   = tDrain;
            p.T_release = tRelease;
            p.T_total   = tConnect + tExecute + tDrain + tRelease;
            p.Rows      = rowCount;
            p.Columns   = ncols;
            return p;
        }
        catch (OdbcException oex)
        {
            p.Error = $"OdbcException: {oex.Message} (code={oex.ErrorCode})";
            return p;
        }
        catch (Exception ex)
        {
            p.Error = $"{ex.GetType().Name}: {ex.Message}";
            return p;
        }
        finally
        {
            rdr?.Dispose();
            cmd?.Dispose();
            conn?.Dispose();
        }
    }
}
