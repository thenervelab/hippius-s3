-- SQL query to get file metadata by ID
-- Parameters: $1: file_id
SELECT f.file_id, COALESCE(c.cid, f.ipfs_cid) as ipfs_cid,
       f.file_name, f.content_type, f.file_size, f.created_at
FROM files f
LEFT JOIN cids c ON f.cid_id = c.id
WHERE f.file_id = $1
