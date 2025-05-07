-- SQL query to get file metadata by ID
-- Parameters: $1: file_id
SELECT file_id, ipfs_cid, file_name, content_type, file_size, created_at
FROM files
WHERE file_id = $1
