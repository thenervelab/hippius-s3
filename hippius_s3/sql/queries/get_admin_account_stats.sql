-- Bucket count + logical bytes (current versions only) for the admin status endpoint.
-- Bounded by the caller's statement timeout: a 10+ TB account can push the SUM into
-- seconds — the endpoint degrades to null counts on timeout rather than 500ing.
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
           ON ov.object_id = o.object_id AND ov.object_version = o.current_object_version
         WHERE b.main_account_id = $1 AND b.deleted_at IS NULL),
        0
    )::bigint AS bytes
