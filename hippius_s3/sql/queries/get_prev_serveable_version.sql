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
SELECT MAX(object_version) AS object_version
FROM object_versions
WHERE object_id = $1
  AND object_version < $2
  AND (size_bytes > 0 OR (md5_hash IS NOT NULL AND md5_hash != ''));
