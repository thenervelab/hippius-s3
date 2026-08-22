-- Insert a delete marker as the new current version of an object.
--
-- Mirrors upsert_object_basic's allocation: the UPDATE takes a row lock on `objects` and the
-- post-lock re-read of current_object_version (EvalPlanQual) makes concurrent writers serialise,
-- so two racing deletes cannot mint the same object_version. GREATEST(..., MAX(object_version))
-- keeps the same best-effort floor for out-of-band versions written by create_migration_version.
-- The caller retries on object_versions_pkey via retry_on_object_version_conflict.
--
-- The marker carries no data: zero size, no md5, no parts, no DEK envelope. storage_version is
-- inherited from the newest existing version so the row stays consistent with its siblings.
-- Returns nothing when the key does not exist or is already soft-deleted.
--
-- Parameters: $1: bucket_id (uuid), $2: object_key (text)
WITH upserted AS (
  UPDATE objects o
     SET current_object_version = GREATEST(
           o.current_object_version,
           (SELECT COALESCE(MAX(ov.object_version), 0)
              FROM object_versions ov
             WHERE ov.object_id = o.object_id)
         ) + 1
   WHERE o.bucket_id = $1
     AND o.object_key = $2
     AND o.deleted_at IS NULL
  RETURNING o.object_id, o.current_object_version
), ins AS (
  INSERT INTO object_versions (
    object_id,
    object_version,
    version_type,
    storage_version,
    size_bytes,
    content_type,
    status,
    is_delete_marker
  )
  SELECT u.object_id,
         u.current_object_version,
         'user',
         COALESCE(
           (SELECT ov.storage_version
              FROM object_versions ov
             WHERE ov.object_id = u.object_id
             ORDER BY ov.object_version DESC
             LIMIT 1),
           5
         ),
         0,
         'binary/octet-stream',
         'uploaded',
         TRUE
    FROM upserted u
  RETURNING object_id, object_version
)
SELECT object_id, object_version FROM ins
