-- Upload a part of a multipart upload
-- Parameters: $1: part_id, $2: upload_id, $3: part_number, $4: ipfs_cid, $5: size_bytes, $6: etag, $7: uploaded_at
INSERT INTO parts (
    part_id, upload_id, part_number, ipfs_cid, size_bytes, etag, uploaded_at, object_id
)
VALUES (
    $1,
    $2,
    $3,
    $4,
    $5,
    $6,
    $7,
    (SELECT object_id FROM multipart_uploads WHERE upload_id = $2)
)
ON CONFLICT (object_id, part_number) DO UPDATE SET
    ipfs_cid = EXCLUDED.ipfs_cid,
    size_bytes = EXCLUDED.size_bytes,
    etag = EXCLUDED.etag,
    uploaded_at = EXCLUDED.uploaded_at,
    object_id = EXCLUDED.object_id
RETURNING part_id, upload_id, part_number, etag
