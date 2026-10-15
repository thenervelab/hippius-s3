-- The one version an unversioned overwrite replaced. get_unversioned_superseded_version
--
-- $1 object_id (uuid), $2 finalized object_version (bigint)
--
-- One backwards probe of object_versions_pkey (object_id, object_version) from $2,
-- stopping at the first eligible row. No row — enqueue nothing — unless $2 is still
-- the current version of a live object whose bucket has versioning_status IS NULL.
-- Enabled and Suspended keep history. A concurrent writer that has already advanced
-- current_object_version unpins when it finishes; this statement does nothing.
--
-- An open multipart upload has no version column. Initiate bumps the counter and the
-- upload's parts carry the reserved version (parts.upload_id; get_multipart_version_by_upload
-- resolves it the same way). That version is still accepting parts, so it is not the
-- object this write replaced. A partless reserve has the same shape as a PUT whose
-- address write failed and was unmarked (no size, no md5); skipping it keeps the walk
-- on the version readers could still fall back to. Object lock matches
-- get_chunk_backend_identifiers: a locked unpin returns zero rows and the unpinner
-- drops it after retries, so a locked version is never enqueued.
WITH live AS MATERIALIZED (
    SELECT o.object_id
    FROM objects o
    JOIN buckets b ON b.bucket_id = o.bucket_id
    WHERE o.object_id = $1
      AND o.deleted_at IS NULL
      AND o.current_object_version = $2
      AND b.versioning_status IS NULL
)
SELECT ov.object_version
FROM live
JOIN LATERAL (
    SELECT v.object_version
    FROM object_versions v
    WHERE v.object_id = live.object_id
      AND v.object_version < $2
      AND v.deleted_at IS NULL
      AND NOT v.is_delete_marker
      AND (v.size_bytes > 0 OR (v.md5_hash IS NOT NULL AND v.md5_hash != ''))
      AND NOT (
          v.object_lock_legal_hold
          OR (v.object_lock_retain_until IS NOT NULL AND v.object_lock_retain_until > now())
      )
      -- Incomplete uploads via idx_multipart_uploads_object_id, then their parts
      -- via idx_parts_upload. Constraining the part row by object as well makes
      -- the planner scan every part of this version on the request path.
      -- upload_id is the multipart primary key, so the upload already names it.
      AND NOT EXISTS (
          SELECT 1
          FROM multipart_uploads mu
          JOIN parts p ON p.upload_id = mu.upload_id
          WHERE mu.object_id = v.object_id
            AND COALESCE(mu.is_completed, false) = false
            AND p.object_version = v.object_version
      )
    ORDER BY v.object_version DESC
    LIMIT 1
) ov ON true
