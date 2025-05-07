-- SQL query to insert a new file record
-- Parameters: $1: file_id, $2: ipfs_cid, $3: file_size, $4: file_name, $5: content_type, $6: created_at
INSERT INTO files (file_id, ipfs_cid, file_size, file_name, content_type, created_at)
VALUES ($1, $2, $3, $4, $5, $6)
RETURNING file_id, ipfs_cid
