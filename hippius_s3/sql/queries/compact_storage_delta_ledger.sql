-- Claim one batch of ledger rows and fold it into the per-bucket rollup. One statement, so the
-- claim and the apply cannot come apart.
--
-- THE CLAIM IS `DELETE ... RETURNING`, which is exactly-once by construction: the rows leave the
-- ledger in the same transaction that adds them to the counter, so a crash or a rollback puts them
-- back and a second compactor can only ever see rows nobody has taken. Two concurrent compactors
-- picking the same ledger_ids do not double-apply — the loser blocks on the row locks and then
-- finds nothing to delete. `FOR UPDATE SKIP LOCKED` would buy throughput we do not need
-- (replicas: 1, and a global advisory lock already serialises this against a recompute) at the
-- price of a second concurrency mechanism to reason about.
--
-- NO `GREATEST(0, ...)` ANYWHERE, and that is not an oversight. Clamping an upsert breaks
-- decrements: clamp the INSERT arm and `EXCLUDED.bytes_used` carries the clamped value into the
-- DO UPDATE arm, flooring every decrement at zero and silently pinning the counter high forever.
-- The counter is allowed to go negative; get_account_storage_bytes_rollup.sql clamps on read and
-- reports the anomaly, and the reconciler repairs it. See the table comment in the migration.
--
-- `JOIN buckets` discards deltas for buckets that no longer exist. A `DELETE FROM buckets` cascade
-- emits decrements for a bucket row that is already gone (the trigger only has the objects row's
-- bucket_id to work with), and bucket_storage_usage cascade-deletes with it, so there is nothing
-- left to apply them to — the FK would raise. Data-modifying CTEs always run to completion
-- regardless of what the outer query consumes, so those rows are still claimed and dropped rather
-- than retried forever.
--
-- Parameters: $1: max ledger rows to claim in this batch
WITH claimed AS (
    DELETE FROM storage_delta_ledger
    WHERE ledger_id IN (
        SELECT ledger_id
        FROM storage_delta_ledger
        ORDER BY ledger_id
        LIMIT $1
    )
    RETURNING bucket_id, delta_bytes
), folded AS (
    SELECT
        c.bucket_id,
        SUM(c.delta_bytes)::bigint AS delta_bytes,
        count(*)::bigint AS ledger_rows
    FROM claimed c
    GROUP BY c.bucket_id
), applied AS (
    INSERT INTO bucket_storage_usage AS bsu (bucket_id, bytes_used, updated_at)
    SELECT f.bucket_id, f.delta_bytes, now()
    FROM folded f
    JOIN buckets b ON b.bucket_id = f.bucket_id
    ON CONFLICT (bucket_id) DO UPDATE
       SET bytes_used = bsu.bytes_used + EXCLUDED.bytes_used,
           updated_at = now()
    RETURNING 1
)
SELECT
    COALESCE((SELECT SUM(f.ledger_rows) FROM folded f), 0)::bigint AS rows_claimed,
    (SELECT count(*) FROM folded)::bigint AS buckets_folded,
    (SELECT count(*) FROM applied)::bigint AS buckets_applied
