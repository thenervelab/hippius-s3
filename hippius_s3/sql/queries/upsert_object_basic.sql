-- Create or update object and its latest version (simple, non-multipart)
WITH upsert_object AS (
  INSERT INTO objects (object_id, bucket_id, object_key, created_at, current_version_seq)
  VALUES ($1, $2, $3, $8, 1)
  ON CONFLICT (bucket_id, object_key)
  DO UPDATE SET object_id = EXCLUDED.object_id, current_version_seq = COALESCE(objects.current_version_seq, 1)
  RETURNING object_id, bucket_id, object_key, created_at, current_version_seq
), ins_version AS (
  INSERT INTO object_versions (
    object_id,
    version_seq,
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
    uo.current_version_seq,
    'user',
    $9,
    $7,
    $4,
    $5::jsonb,
    $6,
    NULL,
    NULL,
    FALSE,
    'publishing',
    0,
    NULL,
    NULL,
    NULL,
    $8,
    $8,
    $8
  FROM upsert_object uo
  ON CONFLICT (object_id, version_seq)
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
  RETURNING object_id, version_seq
)
SELECT o.object_id,
       o.bucket_id,
       o.object_key,
       o.current_version_seq,
       ov.content_type,
       ov.metadata,
       ov.md5_hash,
       ov.size_bytes,
       o.created_at,
       ov.status,
       ov.multipart,
       ov.storage_version
FROM objects o
JOIN ins_version iv ON iv.object_id = o.object_id AND iv.version_seq = o.current_version_seq
JOIN object_versions ov ON ov.object_id = iv.object_id AND ov.version_seq = iv.version_seq
