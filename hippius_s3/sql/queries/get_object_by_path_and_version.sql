-- Get one specific version of an object by bucket and key path.
-- Used by CopyObject when x-amz-copy-source carries ?versionId=.
-- Parameters: $1: bucket_id, $2: object_key, $3: object_version
SELECT o.object_id, o.bucket_id, o.object_key,
       COALESCE(c.cid, ov.ipfs_cid, '') as ipfs_cid,
       ov.size_bytes, ov.content_type, o.created_at, ov.metadata, ov.md5_hash,
       ov.append_version,
       ov.multipart,
       ov.storage_version,
       ov.object_version,
       ov.encryption_version,
       ov.enc_suite_id,
       ov.enc_chunk_size_bytes,
       ov.kek_id,
       ov.wrapped_dek,
       ov.is_delete_marker,
       b.bucket_name
FROM objects o
-- Pinned to an explicit version ($3) rather than resolving the newest serveable one.
-- A version soft-deleted by a versioned DELETE is invisible here, and a delete marker is
-- returned so the caller can reject it rather than copying a zero-byte placeholder.
JOIN object_versions ov
  ON ov.object_id = o.object_id
 AND ov.object_version = $3
 AND ov.deleted_at IS NULL
JOIN buckets b ON o.bucket_id = b.bucket_id
LEFT JOIN cids c ON ov.cid_id = c.id
WHERE o.bucket_id = $1 AND o.object_key = $2 AND o.deleted_at IS NULL
  AND b.deleted_at IS NULL
ORDER BY o.created_at DESC LIMIT 1
