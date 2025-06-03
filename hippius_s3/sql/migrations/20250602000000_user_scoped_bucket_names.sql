-- migrate:up

-- Drop the existing global unique constraint on bucket_name
ALTER TABLE buckets DROP CONSTRAINT buckets_bucket_name_key;

-- Add composite unique constraint for user-scoped bucket names
ALTER TABLE buckets ADD CONSTRAINT buckets_name_owner_unique UNIQUE(bucket_name, owner_user_id);

-- Create index for efficient bucket lookups by name and owner
CREATE INDEX idx_buckets_name_owner ON buckets(bucket_name, owner_user_id);

-- migrate:down

-- Drop the composite unique constraint
ALTER TABLE buckets DROP CONSTRAINT buckets_name_owner_unique;

-- Drop the index
DROP INDEX idx_buckets_name_owner;

-- Re-add the global unique constraint (this will fail if there are duplicate names)
ALTER TABLE buckets ADD CONSTRAINT buckets_bucket_name_key UNIQUE(bucket_name);
