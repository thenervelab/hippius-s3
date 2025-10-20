SELECT p.* FROM parts p
JOIN objects o ON p.object_id = o.object_id
JOIN object_versions ov ON ov.object_id = o.object_id AND ov.object_version = o.current_object_version
WHERE o.object_key = $1 AND o.bucket_id = $2
AND ov.multipart = true
ORDER BY p.uploaded_at DESC
LIMIT 10
