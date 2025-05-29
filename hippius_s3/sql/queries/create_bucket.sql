-- Create a new bucket
-- Parameters: $1: bucket_id, $2: bucket_name, $3: created_at, $4: is_public, $5: owner_user_id
INSERT INTO buckets (bucket_id, bucket_name, created_at, is_public, owner_user_id)
VALUES ($1, $2, $3, $4, $5)
RETURNING bucket_id, bucket_name, created_at, is_public, owner_user_id
