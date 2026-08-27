-- Soft-delete ONE version. See the migration comment for why the row is not dropped here: the
-- unpinner still needs parts/part_chunks/chunk_backend to resolve backend identifiers, so the
-- janitor reaps the row only after every backend copy is confirmed gone.
-- Parameters: $1: object_id (uuid), $2: object_version (bigint)
UPDATE object_versions
   SET deleted_at = now()
 WHERE object_id = $1
   AND object_version = $2
   AND deleted_at IS NULL
RETURNING object_id, object_version, is_delete_marker
