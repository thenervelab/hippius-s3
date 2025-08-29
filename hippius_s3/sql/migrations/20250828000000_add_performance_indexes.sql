-- migrate:up

-- Index for list_objects query - bucket_id + object_key ordering
CREATE INDEX IF NOT EXISTS idx_objects_bucket_key ON objects(bucket_id, object_key);

-- Index for list_objects with created_at ordering (alternative sort)
CREATE INDEX IF NOT EXISTS idx_objects_bucket_created ON objects(bucket_id, created_at);

-- Index for prefix filtering in list_objects
CREATE INDEX IF NOT EXISTS idx_objects_bucket_key_prefix ON objects(bucket_id, object_key text_pattern_ops);

-- Index for bucket lookups by name and owner
CREATE INDEX IF NOT EXISTS idx_buckets_name_owner ON buckets(bucket_name, main_account_id);

-- Index for multipart uploads
CREATE INDEX IF NOT EXISTS idx_multipart_uploads_bucket ON multipart_uploads(bucket_id);

-- Index for multipart parts
CREATE INDEX IF NOT EXISTS idx_parts_upload_part ON parts(upload_id, part_number);

-- migrate:down
DROP INDEX IF EXISTS idx_objects_bucket_key;
DROP INDEX IF EXISTS idx_objects_bucket_created;
DROP INDEX IF EXISTS idx_objects_bucket_key_prefix;
DROP INDEX IF EXISTS idx_buckets_name_owner;
DROP INDEX IF EXISTS idx_multipart_uploads_bucket;
DROP INDEX IF EXISTS idx_parts_upload_part;
