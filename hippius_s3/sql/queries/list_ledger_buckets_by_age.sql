-- Buckets with unfolded ledger rows, oldest first, so the compactor drains in the order work arrived.
--
-- `min(ledger_id)` rather than `min(created_at)`: ledger_id is a bigint sequence and the only strictly
-- monotonic column here, and created_at is a wall clock that can tie or go backwards.
--
-- Cheap despite the GROUP BY because the ledger is a QUEUE, not a log: the compactor drains it to ~0
-- every cycle (prod steady state is single-digit rows) and its autovacuum is tuned flat at 10k dead
-- tuples for exactly this shape. If this ever becomes slow the ledger is not draining, which is its
-- own alert (storage_rollup_ledger_depth).
SELECT bucket_id
FROM storage_delta_ledger
GROUP BY bucket_id
ORDER BY min(ledger_id)
LIMIT $1
