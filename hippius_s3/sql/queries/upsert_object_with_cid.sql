-- Allocate a new version and set cid_id on that version (promotes it live).
WITH upserted AS (
  INSERT INTO objects (object_id, bucket_id, object_key, created_at, current_object_version)
  VALUES ($1, $2, $3, $7, 1)
  ON CONFLICT (bucket_id, object_key)
  DO UPDATE SET
    object_key = EXCLUDED.object_key,
    deleted_at = NULL,
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
    'user',
    $10,
    $5,
    $6,
    $8::jsonb,
    $9,
    NULL,
    $4,
    FALSE,
    'publishing',
    0,
    $7,
    $7,
    $7
  FROM upserted u
  RETURNING object_id, object_version
)
SELECT
  u.object_id,
  u.bucket_id,
  u.object_key,
  $4 AS cid_id,
  $5 AS size_bytes,
  $6 AS content_type,
  $7 AS created_at,
  $8 AS metadata,
  $9 AS md5_hash
FROM upserted u
JOIN ins_version iv ON iv.object_id = u.object_id AND iv.object_version = u.current_object_version
