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
-- is itself the alert.
SELECT
    (SELECT count(*) FROM storage_delta_ledger)::bigint AS depth,
    COALESCE((
        SELECT EXTRACT(EPOCH FROM (now() - min(l.created_at)))::bigint FROM storage_delta_ledger l
    ), 0)::bigint AS oldest_age_seconds,
    (SELECT count(*) FROM bucket_storage_usage WHERE bytes_used < 0)::bigint AS negative_buckets
