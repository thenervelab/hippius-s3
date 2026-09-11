-- CompleteMultipartUpload If-None-Match: *, run right after lock_object_row_for_update.
--
-- True when the key already serves an object other than the version this upload completes: the
-- newest serveable version besides ours is not a delete marker. The lock means a concurrent PUT tail
-- or completion is either fully visible here or not started.
--
-- Unlike the PUT path there is no reserve-time baseline: the header only arrives with the completion,
-- and InitiateMultipartUpload has already cleared a soft delete (upsert_object_multipart sets
-- deleted_at = NULL). So a key soft-deleted in an unversioned bucket still counts as existing here,
-- which matches what reads serve for it while the upload is open.
--
-- $1 object_id (uuid), $2 our object_version (bigint)
SELECT COALESCE((
    SELECT NOT v.is_delete_marker
    FROM object_versions v
    WHERE v.object_id = $1
      AND v.object_version <> $2
      AND v.deleted_at IS NULL
      AND (v.is_delete_marker OR v.size_bytes > 0 OR (v.md5_hash IS NOT NULL AND v.md5_hash != ''))
    ORDER BY v.object_version DESC
    LIMIT 1
), FALSE)
