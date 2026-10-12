-- Parameters: $1: bucket_id
INSERT INTO bucket_public_objects (bucket_id)
VALUES ($1)
ON CONFLICT (bucket_id) DO NOTHING
