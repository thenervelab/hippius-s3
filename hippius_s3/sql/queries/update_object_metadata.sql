-- Write metadata to a specific object version to align with S3 per-version tags
-- Parameters: $1: metadata (JSON), $2: object_id, $3: object_version
UPDATE object_versions ov
SET metadata = $1::jsonb,
    last_modified = NOW()
WHERE ov.object_id = $2
  AND ov.object_version = $3
