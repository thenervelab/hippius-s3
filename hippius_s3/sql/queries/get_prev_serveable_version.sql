-- Highest COMPLETED version below $3 for one (bucket, key) — the envelope-race fallback's target.
--
-- The reader used to fall back to `object_version - 1`, which assumes version numbers are
-- contiguous. They are not: an aborted MPU now retains its reserved row
-- (abort_cleanup_orphan_version.sql), and create_migration_version.sql mints versions out of band,
-- so N-1 can be a placeholder with no envelope and no parts. Falling back onto one turns a
-- recoverable mid-write read into a 500. Asking for the highest serveable version below N instead
-- skips any number of placeholders and is correct however sparse the numbering gets.
--
-- Same "complete, not a reserved placeholder" predicate as the unversioned resolver
-- (get_object_for_download_with_permissions.sql): a 0-byte object stores the md5 of the empty
-- string, so only the never-completed shape (no bytes AND no md5) is excluded.
-- Params: $1 bucket_name (text), $2 object_key (text), $3 object_version (bigint)
SELECT MAX(ov.object_version) AS object_version
FROM object_versions ov
JOIN objects o ON o.object_id = ov.object_id
JOIN buckets b ON b.bucket_id = o.bucket_id
WHERE b.bucket_name = $1
  AND b.deleted_at IS NULL
  AND o.object_key = $2
  AND o.deleted_at IS NULL
  AND ov.object_version < $3
  AND (ov.size_bytes > 0 OR (ov.md5_hash IS NOT NULL AND ov.md5_hash != ''));
