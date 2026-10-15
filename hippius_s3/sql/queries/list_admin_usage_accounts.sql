-- Paged billable usage for GET /admin/usage. The chain reporter bills this number.
--
-- Same definition as get_account_storage_bytes_rollup.sql: SUM of GREATEST(0, bytes_used)
-- over buckets with deleted_at IS NULL. Clamp each bucket, not the account total — one
-- bucket at -3 and one at 10 is 10, not 7. A live bucket with no rollup row counts as a
-- bucket and zero bytes. An account with no live bucket is absent. Do not filter service
-- accounts or suspension here; ownership is buckets.main_account_id at read time.
--
-- `ready` is storage_usage_rollup_state.backfilled_at IS NOT NULL. Until the backfill has
-- run the counters are deltas-since-migration, not totals. The caller returns 503 and must
-- not bill the page. An empty page still returns one row (account_id NULL) so `ready` is
-- visible.
--
-- Parameters: $1 exclusive main_account_id cursor (NULL starts at the beginning),
--             $2 page size. A page of exactly this many rows is full: the handler
--             sets next_after to the last account even when no further row exists.
--
-- Plan: one read of every live bucket (thousands, not objects) plus the rollup
-- rows, grouped by account, then LIMIT. A cursor does not make the aggregate
-- smaller; the page is already the whole account set at limit 1000. Do not put
-- COLLATE "C" on the range comparison. idx_buckets_main_account uses the column
-- collation, and an explicit COLLATE will not use it. Deployed databases are
-- locale C, which is that byte order. ORDER BY names COLLATE "C".
SELECT
    COALESCE((SELECT s.backfilled_at IS NOT NULL FROM storage_usage_rollup_state s), false) AS ready,
    page.account_id,
    page.buckets,
    page.bytes
FROM (SELECT 1) AS anchor
LEFT JOIN LATERAL (
    SELECT
        b.main_account_id AS account_id,
        COUNT(*)::bigint AS buckets,
        COALESCE(SUM(GREATEST(0, bsu.bytes_used)), 0)::bigint AS bytes
    FROM buckets b
    LEFT JOIN bucket_storage_usage bsu ON bsu.bucket_id = b.bucket_id
    WHERE b.deleted_at IS NULL
      AND b.main_account_id > COALESCE($1::text, '')
    GROUP BY b.main_account_id
    ORDER BY b.main_account_id COLLATE "C"
    LIMIT $2::int
) page ON true
