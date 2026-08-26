-- $1 bucket_id, $2 object_key, $3 object_id
-- Refuse when dest is already a live primary of any object (overwrite streams).
INSERT INTO object_names (bucket_id, object_key, object_id)
SELECT $1, $2, $3
WHERE NOT EXISTS (
    SELECT 1 FROM objects o
    WHERE o.bucket_id = $1
      AND o.object_key = $2
      AND o.deleted_at IS NULL
)
ON CONFLICT (bucket_id, object_key) DO UPDATE
SET object_id = EXCLUDED.object_id
WHERE NOT EXISTS (
    SELECT 1 FROM objects o
    WHERE o.bucket_id = EXCLUDED.bucket_id
      AND o.object_key = EXCLUDED.object_key
      AND o.deleted_at IS NULL
      AND o.object_id IS DISTINCT FROM EXCLUDED.object_id
)
RETURNING object_id
