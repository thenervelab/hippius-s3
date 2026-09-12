-- CompleteMultipartUpload If-None-Match: *, run right after lock_object_row_for_update.
--
-- True when a version ABOVE the one this upload completes became serveable, and not as a delete
-- marker: someone created the key while the upload was open, so ours must not land. The lock means a
-- concurrent PUT tail or completion is either fully visible here or not started.
--
-- Versions at or below ours existed before this upload and are NOT judged here — they cannot be,
-- because InitiateMultipartUpload clears a soft delete (upsert_object_multipart sets deleted_at =
-- NULL), so by now a key that was deleted before the upload started is indistinguishable from one
-- that was live. That judgement is made at initiate instead and carried on
-- multipart_uploads.key_existed_at_initiate, which the caller ORs with this result — the same
-- baseline-then-re-check split the PutObject path uses (conditional_write_state +
-- conditional_write_conflict).
--
-- $1 object_id (uuid), $2 our object_version (bigint)
SELECT COALESCE((
    SELECT NOT v.is_delete_marker
    FROM object_versions v
    WHERE v.object_id = $1
      AND v.object_version > $2
      AND v.deleted_at IS NULL
      AND (v.is_delete_marker OR v.size_bytes > 0 OR (v.md5_hash IS NOT NULL AND v.md5_hash != ''))
    ORDER BY v.object_version DESC
    LIMIT 1
), FALSE)
