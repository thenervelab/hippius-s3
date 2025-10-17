-- Upsert object and update its latest version
WITH upsert_object AS (
  INSERT INTO objects (object_id, bucket_id, object_key, created_at, current_version_seq)
  VALUES ($1, $2, $3, $7, 1)
  ON CONFLICT (bucket_id, object_key)
  DO UPDATE SET object_id = EXCLUDED.object_id, current_version_seq = COALESCE(objects.current_version_seq, 1)
  RETURNING object_id, current_version_seq
)
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
FROM upsert_object uo
ON CONFLICT (object_id, version_seq)
DO UPDATE SET
    storage_version = EXCLUDED.storage_version,
    size_bytes = EXCLUDED.size_bytes,
    content_type = EXCLUDED.content_type,
    metadata = EXCLUDED.metadata,
    md5_hash = EXCLUDED.md5_hash,
    ipfs_cid = EXCLUDED.ipfs_cid,
    status = EXCLUDED.status,
    last_modified = EXCLUDED.last_modified
RETURNING object_id, $4 AS ipfs_cid, $5 AS size_bytes, $6 AS content_type, $7 AS created_at, $8 AS metadata, $9 AS md5_hash
