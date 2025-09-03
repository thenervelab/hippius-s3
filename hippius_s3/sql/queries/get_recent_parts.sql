SELECT p.* FROM parts p
JOIN objects o ON p.object_id = o.object_id
WHERE o.object_key = $1 AND o.bucket_id = $2
AND o.multipart = true
ORDER BY p.uploaded_at DESC
LIMIT 10
