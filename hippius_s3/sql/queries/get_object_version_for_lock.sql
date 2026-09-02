-- $1 bucket_id (UUID), $2 object_key (TEXT), $3 version_id (BIGINT, nullable = current version)
--
-- Resolves which version a per-object lock request addresses. A NULL $3 means "the current
-- version", matching AWS: ?retention and ?legal-hold without a versionId act on the current one.
-- Delete markers are excluded — they carry no data, so there is nothing to retain, and AWS
-- answers MethodNotAllowed rather than locking one.
SELECT o.object_id, ov.object_version
FROM objects o
LEFT JOIN object_versions ov
       ON ov.object_id = o.object_id
      AND ov.object_version = COALESCE($3::bigint, o.current_object_version)
      AND ov.deleted_at IS NULL
      AND NOT ov.is_delete_marker
WHERE o.object_id = resolve_object_id($1::uuid, $2)
  AND o.deleted_at IS NULL;
