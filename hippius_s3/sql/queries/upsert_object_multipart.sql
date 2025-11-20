WITH upsert_object AS (
  INSERT INTO objects (object_id, bucket_id, object_key, created_at, current_object_version)
  VALUES ($1, $2, $3, $8, 1)
  ON CONFLICT (bucket_id, object_key)
  DO UPDATE SET
    current_object_version = COALESCE(objects.current_object_version, 1)
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
    manifest_cid,
    manifest_built_for_version,
    manifest_built_at,
    last_append_at,
    last_modified,
    created_at
  )
  SELECT
    uo.object_id,
    uo.current_object_version,
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
    NULL,
    NULL,
    NULL,
    $8,
    $8,
    $8
  FROM upsert_object uo
  ON CONFLICT (object_id, object_version)
  DO UPDATE SET
    storage_version = EXCLUDED.storage_version,
    size_bytes = EXCLUDED.size_bytes,
    content_type = EXCLUDED.content_type,
    metadata = EXCLUDED.metadata,
    md5_hash = EXCLUDED.md5_hash,
    multipart = EXCLUDED.multipart,
    status = EXCLUDED.status,
    last_append_at = EXCLUDED.last_append_at,
    last_modified = EXCLUDED.last_modified
  RETURNING object_id, object_version, content_type, metadata, md5_hash, size_bytes, status, multipart, storage_version
)
SELECT uo.object_id,
       uo.bucket_id,
       uo.object_key,
       iv.content_type,
       iv.metadata,
       iv.md5_hash,
       iv.size_bytes,
       uo.created_at,
       iv.status,
       iv.multipart,
       iv.storage_version
FROM upsert_object uo
JOIN ins_version iv ON iv.object_id = uo.object_id AND iv.object_version = uo.current_object_version
