-- Get a multipart upload by upload_id
-- Parameters: $1: upload_id
SELECT mu.*, b.bucket_name, o.current_version_seq
FROM multipart_uploads mu
JOIN buckets b ON mu.bucket_id = b.bucket_id
LEFT JOIN objects o ON o.object_id = mu.object_id
WHERE mu.upload_id = $1
