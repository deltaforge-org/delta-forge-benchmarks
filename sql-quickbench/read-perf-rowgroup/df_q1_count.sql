-- One CLI invocation, one session. OPEN DELTA TABLE registers the
-- duck-authored Delta-wrapped directory under a simple alias for the
-- duration of this session; the following SELECT runs against it.
OPEN DELTA TABLE 'B:/odbc_df/df-demo/perf_test/dim_customer_duck'
AS dim_customer_duck;

SELECT COUNT(*) AS rows FROM dim_customer_duck;
