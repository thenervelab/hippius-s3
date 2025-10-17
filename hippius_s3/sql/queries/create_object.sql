-- Create a new object and its initial version
WITH ins_obj AS (
  INSERT INTO objects (object_id, bucket_id, object_key, created_at, current_version_seq)
  VALUES ($1, $2, $3, $7, 1)
  RETURNING object_id
), ins_ver AS (
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
    io.object_id,
    1,
    'user',
    $10,
    $5,
    $6,
    $8::jsonb,
    $9,
    $4,
    NULL,
    FALSE,
    'publishing',
    0,
    NULL,
    NULL,
    NULL,
    $7,
    $7,
    $7
  FROM ins_obj io
  RETURNING object_id
)
SELECT o.object_id, o.bucket_id, o.object_key, $4 AS ipfs_cid, $9 AS md5_hash
FROM objects o
JOIN ins_ver v ON v.object_id = o.object_id
