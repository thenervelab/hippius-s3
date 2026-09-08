-- Ground-truth billable bytes for an account. THE definition of "storage used" in this codebase.
--
-- Expensive by construction: it walks every live object the account owns, which is 11.8M rows for
-- the largest prod account. It exists for exactly two callers, both of which can afford it:
--
--   1. The quota gate's DENIAL path. The cheap rollup may ALLOW an upload, but only this query may
--      DENY one. Denials are rare -- only accounts genuinely near their limit reach that branch --
--      so paying tens of milliseconds there buys the guarantee that a drifted counter can never 402
--      a paying customer. Run it under a short statement_timeout and ALLOW on timeout.
--   2. The reconciler / backfill, off the request path.
--
-- Keep in sync with recompute_bucket_storage_usage.sql and with usage_billable() in
-- migrations/20260908120000_bucket_storage_usage.sql -- these three encode one definition and any
-- disagreement shows up as permanent, unfixable counter drift.
--
-- Parameters: $1: main_account_id (SS58)
SELECT COALESCE(SUM(ov.size_bytes), 0)::bigint AS bytes_used
FROM buckets b
JOIN objects o
  ON o.bucket_id = b.bucket_id
 AND o.deleted_at IS NULL
JOIN object_versions ov
  ON ov.object_id = o.object_id
 AND ov.object_version = o.current_object_version
 AND ov.deleted_at IS NULL
 AND NOT ov.is_delete_marker
WHERE b.main_account_id = $1
  AND b.deleted_at IS NULL
