-- $1 bucket_id (UUID), $2 object_key (TEXT)
--
-- How many LIVE versions of this key are under an Object Lock. Used by the delete paths that act
-- on the whole object rather than one version (the unversioned soft delete, and DeleteObjects),
-- where AWS's per-version 403 has no single version to name.
--
-- Mirrors object_lock_enforcement.is_version_locked; the predicate is asserted consistent by
-- test_sql_gates_embed_the_canonical_predicate.
SELECT count(*)::int AS locked_count
FROM object_versions ov
WHERE ov.object_id = resolve_object_id($1::uuid, $2)
  AND ov.deleted_at IS NULL
  AND (ov.object_lock_legal_hold
       OR (ov.object_lock_retain_until IS NOT NULL AND ov.object_lock_retain_until > now()));
