-- Highest COMPLETED version below $2 for one object — the envelope-race fallback's target.
--
-- The reader used to fall back to `object_version - 1`, which assumes contiguous numbering. It is
-- not: an aborted MPU retains its reserved row (abort_cleanup_orphan_version.sql) and
-- create_migration_version.sql mints versions out of band, so N-1 can be a placeholder with no
-- envelope and no parts — falling onto one turns a recoverable mid-write read into a 500.
--
-- Keyed on object_id, not (bucket, key): the caller already resolved the object, and the joins
-- would only re-derive a row it holds. Same "complete, not a reserved placeholder" predicate as
-- the unversioned resolver; a 0-byte object stores the md5 of the empty string, so only the
-- never-completed shape (no bytes AND no md5) is excluded.
-- Params: $1 object_id (uuid), $2 object_version (bigint)
-- deleted_at IS NULL: a versioned DELETE tombstones the row but keeps it (the unpinner resolves
-- backend identifiers at processing time). Without this filter the fallback can pick a version
-- that is already tombstoned and possibly reaped; the by-version resolver then correctly refuses
-- it, prev_info comes back empty, and the read 500s instead of falling further back to a version
-- that is genuinely serveable.
SELECT MAX(object_version) AS object_version
FROM object_versions
WHERE object_id = $1
  AND object_version < $2
  AND deleted_at IS NULL
  AND (size_bytes > 0 OR (md5_hash IS NOT NULL AND md5_hash != ''));
