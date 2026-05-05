-- index-perf: create the row-level index on customer_id.
--
-- The selector engages this index when:
--   - the predicate covers customer_id (point lookup, IN, BETWEEN), AND
--   - estimated result cardinality is small enough that pointer-driven
--     scanning beats a column scan with row-group pruning.
--
-- AUTO_UPDATE = true so future inserts / updates fire the maintenance
-- hook; this bench does not exercise that path but it's the production
-- default.

CREATE INDEX IF NOT EXISTS idx_customer_id
    ON pbi.bench.idx_perf_customer_10m (customer_id);
