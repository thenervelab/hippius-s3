-- Fold one BUCKET's ledger rows into its counter. One statement, so the claim and the apply cannot
-- come apart.
--
-- PER-BUCKET, replacing a global claim across all buckets. The reason is lock scope, not throughput:
-- the caller takes pg_try_advisory_xact_lock(rollup_key, bucket_key) around this, and a claim that
-- spanned buckets could only ever be protected by one estate-wide lock. That global lock is what let
-- a single 300s recompute stall compaction for every other bucket -- prod 2026-09-15, ledger oldest
-- row aged to 412s against a normal 2s, because compact_once's pg_try_advisory_xact_lock kept losing
-- to a bucket it had nothing to do with.
--
-- THE CLAIM IS `DELETE ... RETURNING`, which is exactly-once by construction: the rows leave the
-- ledger in the same transaction that adds them to the counter, so a crash or a rollback puts them
-- back and a second compactor can only ever see rows nobody has taken. Two concurrent compactors
-- picking the same ledger_ids do not double-apply -- the loser blocks on the row locks and then
-- finds nothing to delete.
--
-- NO `GREATEST(0, ...)` ANYWHERE, and that is not an oversight. Clamping an upsert breaks
-- decrements: clamp the INSERT arm and `EXCLUDED.bytes_used` carries the clamped value into the
-- DO UPDATE arm, flooring every decrement at zero and silently pinning the counter high forever.
-- The counter is allowed to go negative; get_account_storage_bytes_rollup.sql clamps on read and
-- reports the anomaly, and the reconciler repairs it.
--
-- `churn_bytes` accumulates SUM(ABS(delta_bytes)) and never decreases. It is the tolerance bound for
-- sliced verification: a sweep that reads a bucket over many cycles cannot be compared to the counter
-- directly, but the truth cannot have moved further than the absolute delta that passed through here
-- while the sweep ran. ABS rather than the signed sum because +1 GB then -1 GB nets to zero while
-- genuinely moving the number twice, and a signed bound would admit exactly the drift this catches.
--
-- `JOIN buckets` discards deltas for a bucket that no longer exists, whose FK would otherwise raise.
WITH claimed AS (
    DELETE FROM storage_delta_ledger
    WHERE ledger_id IN (
        SELECT ledger_id
        FROM storage_delta_ledger
        WHERE bucket_id = $1
        ORDER BY ledger_id
        LIMIT $2
    )
    RETURNING delta_bytes
), folded AS (
    SELECT
        COALESCE(SUM(c.delta_bytes), 0)::bigint AS delta_bytes,
        COALESCE(SUM(ABS(c.delta_bytes)), 0)::bigint AS churn_bytes,
        count(*)::bigint AS ledger_rows
    FROM claimed c
), applied AS (
    INSERT INTO bucket_storage_usage AS bsu (bucket_id, bytes_used, churn_bytes, updated_at)
    SELECT $1, f.delta_bytes, f.churn_bytes, now()
    FROM folded f
    JOIN buckets b ON b.bucket_id = $1
    WHERE f.ledger_rows > 0
    ON CONFLICT (bucket_id) DO UPDATE
       SET bytes_used = bsu.bytes_used + EXCLUDED.bytes_used,
           churn_bytes = bsu.churn_bytes + EXCLUDED.churn_bytes,
           updated_at = now()
    RETURNING 1
)
SELECT
    (SELECT f.ledger_rows FROM folded f)::bigint AS rows_claimed,
    (SELECT count(*) FROM applied)::bigint AS buckets_applied
