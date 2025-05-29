SELECT p.* FROM parts p
JOIN multipart_uploads m ON p.upload_id = m.upload_id
WHERE m.object_key = $1 AND m.bucket_id = $2
AND m.is_completed = true
ORDER BY p.uploaded_at DESC
LIMIT 10
