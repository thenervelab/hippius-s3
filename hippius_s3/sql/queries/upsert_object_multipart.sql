WITH upserted AS (
  INSERT INTO objects (object_id, bucket_id, object_key, created_at, current_object_version)
  VALUES ($1, $2, $3, $8, 1)
  ON CONFLICT (bucket_id, object_key)
  DO UPDATE SET
    object_key = EXCLUDED.object_key,
    deleted_at = NULL,
    -- Allocate a fresh object_version for each MPU initiation.
    -- Atomic MAX()+1 under the row lock taken by the ON CONFLICT update.
    current_object_version = (
      SELECT COALESCE(MAX(ov.object_version), 0) + 1
      FROM object_versions ov
      WHERE ov.object_id = objects.object_id
    )
  RETURNING object_id, bucket_id, object_key, created_at, current_object_version
), ins_version AS (
  INSERT INTO object_versions (
    object_id,
    object_version,
    version_type,
    storage_version,
    size_bytes,
    content_type,
    metadata,
    md5_hash,
    ipfs_cid,
    cid_id,
    multipart,
    status,
    append_version,
    last_append_at,
    last_modified,
    created_at
  )
  SELECT
    u.object_id,
    u.current_object_version,
    'user'::version_type,
    $9,
    $7,
    $4,
    $5::jsonb,
    $6,
    NULL,
    NULL,
    TRUE,
    'publishing',
    0,
    $8,
    $8,
    $8
  FROM upserted u
  RETURNING object_id, object_version, content_type, metadata, md5_hash, size_bytes, status, multipart, storage_version
)
SELECT
  u.object_id,
  u.bucket_id,
  u.object_key,
  u.current_object_version,
  iv.content_type,
  iv.metadata,
  iv.md5_hash,
  iv.size_bytes,
  u.created_at,
  iv.status,
  iv.multipart,
  iv.storage_version
FROM upserted u
JOIN ins_version iv ON iv.object_id = u.object_id AND iv.object_version = u.current_object_version
