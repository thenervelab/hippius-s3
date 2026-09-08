-- List buckets owned by a specific user with object count and total size (console/user endpoint)
--
-- The filters on `o.deleted_at`, `ov.deleted_at` and `ov.is_delete_marker` were MISSING here while
-- get_admin_account_stats.sql had them, so this endpoint and the admin endpoint reported two
-- different "storage used" numbers for the same account -- this one counting soft-deleted objects
-- and delete-markered keys. That was survivable while the number was only informational. It is not
-- survivable now that a billing plan refuses uploads on it: the number a customer sees in the
-- console has to be the number we enforce, or every near-limit rejection becomes a support ticket.
--
-- Keep in sync with get_admin_account_stats.sql. Those two encode one definition.
--
-- Parameters: $1: main_account_id
SELECT
    b.bucket_id,
    b.bucket_name,
    b.created_at,
    ba.acl_json,
    b.tags,
    COALESCE(COUNT(ov.object_id), 0)::bigint AS total_objects,
    COALESCE(SUM(ov.size_bytes), 0)::bigint AS total_size_bytes
FROM buckets b
LEFT JOIN bucket_acls ba ON ba.bucket_id = b.bucket_id
LEFT JOIN objects o
       ON o.bucket_id = b.bucket_id
      AND o.deleted_at IS NULL
LEFT JOIN object_versions ov
       ON ov.object_id = o.object_id
      AND ov.object_version = o.current_object_version
      AND ov.deleted_at IS NULL
      AND NOT ov.is_delete_marker
WHERE b.main_account_id = $1
  AND b.deleted_at IS NULL
GROUP BY b.bucket_id, b.bucket_name, b.created_at, ba.acl_json, b.tags
ORDER BY b.created_at DESC
