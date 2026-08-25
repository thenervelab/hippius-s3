-- $1 bucket_id, $2 object_key, $3 object_id
INSERT INTO object_names (bucket_id, object_key, object_id)
VALUES ($1, $2, $3)
ON CONFLICT (bucket_id, object_key) DO UPDATE
SET object_id = EXCLUDED.object_id
RETURNING object_id
