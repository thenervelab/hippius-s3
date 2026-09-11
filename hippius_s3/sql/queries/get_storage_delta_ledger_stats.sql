-- Compactor health in one round trip: how far behind it is, and whether any counter has gone
-- negative.
--
-- `depth` is the queue length and `oldest_age_seconds` is the lag; both should sit near zero in
-- steady state, and a rising pair means the compactor is down or wedged while the rollup silently
-- freezes. `negative_buckets` should be exactly zero -- a negative counter is only reachable if a
-- decrement was recorded without its matching increment, i.e. a real defect.
--
-- count(*) over the ledger is a full scan, which is fine: the ledger is drained continuously and
-- steady-state size is one batch. If it is ever big enough for this to hurt, the number it returns
-- is itself the alert. Depth and lag come from ONE aggregate rather than two subqueries, so the
-- ledger is scanned once per call instead of twice.
--
-- `negative_buckets` is a full scan of bucket_storage_usage (no index on bytes_used, and a partial
-- one would be the fix if this ever needs 5-second resolution). That is why the caller runs this on
-- the reconcile cadence rather than every compaction cycle -- see run_usage_rollup_in_loop.
SELECT
    ledger.depth::bigint AS depth,
    ledger.oldest_age_seconds::bigint AS oldest_age_seconds,
    (SELECT count(*) FROM bucket_storage_usage WHERE bytes_used < 0)::bigint AS negative_buckets
FROM (
    SELECT count(*) AS depth,
           COALESCE(EXTRACT(EPOCH FROM (now() - min(created_at)))::bigint, 0) AS oldest_age_seconds
    FROM storage_delta_ledger
) AS ledger
