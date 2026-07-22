-- Resolve the object_version an MPU's parts live under, keyed by upload_id.
-- multipart_uploads has no version column; a FRESH object_version is allocated per MPU
-- initiation (upsert_object_multipart.sql), and the parts are written under it. Trusting
-- objects.current_object_version is WRONG under concurrent same-key MPUs (S3 allows them):
-- the pointer advances to the LATEST initiation, so an earlier upload's abort/complete would
-- target a different — possibly in-flight — version's parts/replication/cache.
-- Parameters: $1: upload_id
SELECT object_version
FROM parts
WHERE upload_id = $1
ORDER BY object_version DESC
LIMIT 1
