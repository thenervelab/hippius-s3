-- Bucket count + logical bytes (current versions only) for the admin status endpoint.
-- Bounded by the caller's statement timeout: a 10+ TB account can push the SUM into
-- seconds — the endpoint degrades to null counts on timeout rather than 500ing.
-- Keep in sync with get_account_storage_usage_authoritative.sql, console_list_buckets.sql and
-- usage_billable() in migrations/20260908120000_bucket_storage_usage.sql -- these encode ONE
-- definition of "storage used", and a billing plan is now enforced on it.
-- Parameters: $1: account_id (SS58)
SELECT
    (SELECT COUNT(*)
     FROM buckets
     WHERE main_account_id = $1 AND deleted_at IS NULL) AS buckets,
    COALESCE(
        (SELECT SUM(ov.size_bytes)
         FROM buckets b
         JOIN objects o ON o.bucket_id = b.bucket_id AND o.deleted_at IS NULL
         JOIN object_versions ov
           ON ov.object_id = o.object_id
          AND ov.object_version = o.current_object_version
          AND ov.deleted_at IS NULL
          AND NOT ov.is_delete_marker
         WHERE b.main_account_id = $1 AND b.deleted_at IS NULL),
        0
    )::bigint AS bytes
