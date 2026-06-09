// driver-bench .NET: ADBC consumer-side path.
//
// Drives Apache.Arrow.Adbc against the DeltaForge ADBC driver. The
// AdbcDriverLoader.LoadDriver call dlopens the .so (no
// adbc_driver_manager runtime dependency) and uses the AdbcDriverInit
// entry point exactly the way Power BI Desktop's ADBC connector does
// under the hood.
//
// Drain is the IArrowArrayStream loop: ReadNextRecordBatchAsync pulls
// a RecordBatch by reference (zero-copy), the bench reads .Length to
// account for the row count, then releases. No per-cell conversion.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using Apache.Arrow.Adbc;
using Apache.Arrow.Adbc.C;
using Apache.Arrow.Ipc;

namespace DriverBench;

internal static class AdbcPath
{
    public static PhaseTimings RunIter(Config cfg)
    {
        var p = new PhaseTimings();
        var sw = Stopwatch.StartNew();

        AdbcDriver?     driver  = null;
        AdbcDatabase?   db      = null;
        AdbcConnection? conn    = null;
        AdbcStatement?  stmt    = null;
        QueryResult?    result  = null;
        IArrowArrayStream? stream = null;

        try
        {
            // ----- t_connect -----------------------------------------
            // LoadDriver does dlopen + dlsym("AdbcDriverInit") + builds
            // the C function-table wrapper. Open() invokes the wrapper's
            // DatabaseNew + SetOption(uri) + DatabaseInit. Connect()
            // calls ConnectionNew + ConnectionInit. We bundle all three
            // into t_connect because that's how an end-user
            // OdbcConnection.Open() equivalent feels: one synchronous
            // "give me a working connection" call.
            driver = AdbcDriverLoader.LoadDriver(cfg.AdbcSo, "AdbcDriverInit");

            var dbParams = new Dictionary<string, string> {
                { "uri", cfg.AdbcUri },
            };
            if (!string.IsNullOrEmpty(cfg.AdbcCompute))
                dbParams["adbc.deltaforge.compute_url"] = cfg.AdbcCompute;
            if (!string.IsNullOrEmpty(cfg.AdbcToken))
                dbParams["adbc.deltaforge.session_token"] = cfg.AdbcToken;
            else
            {
                if (!string.IsNullOrEmpty(cfg.AdbcUser))
                    dbParams["username"] = cfg.AdbcUser;
                if (!string.IsNullOrEmpty(cfg.AdbcPwd))
                    dbParams["password"] = cfg.AdbcPwd;
            }
            db = driver.Open(dbParams);
            conn = db.Connect(new Dictionary<string, string>());

            double tConnect = sw.Elapsed.TotalSeconds;
            sw.Restart();

            // ----- t_execute -----------------------------------------
            stmt = conn.CreateStatement();
            stmt.SqlQuery = cfg.Sql;
            result = stmt.ExecuteQuery();
            stream = result.Stream;
            if (stream is null)
                throw new InvalidOperationException("ExecuteQuery returned a null stream");
            double tExecute = sw.Elapsed.TotalSeconds;
            sw.Restart();

            // ----- t_drain -------------------------------------------
            // Synchronous batch pull. ReadNextRecordBatchAsync returns
            // null at end-of-stream. Each batch is held by reference
            // (zero-copy from the wire decoder); we account for the
            // row count via .Length and then dispose to release the
            // refs.
            long rowCount = 0;
            int ncols = 0;
            while (true)
            {
                var batchTask = stream.ReadNextRecordBatchAsync();
                var batch = batchTask.IsCompleted ? batchTask.Result : batchTask.AsTask().GetAwaiter().GetResult();
                if (batch is null) break;
                rowCount += batch.Length;
                if (ncols == 0) ncols = batch.ColumnCount;
                batch.Dispose();
            }
            double tDrain = sw.Elapsed.TotalSeconds;
            sw.Restart();

            // ----- t_release -----------------------------------------
            stream.Dispose();
            stmt.Dispose();
            conn.Dispose();
            db.Dispose();
            driver.Dispose();
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
        catch (AdbcException aex)
        {
            p.Error = $"AdbcException: {aex.Message} (status={aex.Status} sqlstate={aex.SqlState})";
            return p;
        }
        catch (Exception ex)
        {
            p.Error = $"{ex.GetType().Name}: {ex.Message}";
            return p;
        }
        finally
        {
            try { stream?.Dispose(); } catch { /* tearing down on error */ }
            try { stmt?.Dispose(); }   catch { }
            try { conn?.Dispose(); }   catch { }
            try { db?.Dispose(); }     catch { }
            try { driver?.Dispose(); } catch { }
        }
    }
}
