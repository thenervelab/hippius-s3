-- migrate:up

-- Buckets table
CREATE TABLE buckets (
    bucket_id UUID PRIMARY KEY,
    bucket_name TEXT UNIQUE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    is_public BOOLEAN DEFAULT FALSE
);

-- Files/objects table
CREATE TABLE objects (
    object_id UUID PRIMARY KEY,
    bucket_id UUID NOT NULL REFERENCES buckets(bucket_id) ON DELETE CASCADE,
    object_key TEXT NOT NULL,
    ipfs_cid TEXT NOT NULL,
    size_bytes BIGINT NOT NULL,
    content_type TEXT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    metadata JSONB,
    UNIQUE(bucket_id, object_key)
);

-- Indexes for efficient lookups
CREATE INDEX idx_objects_bucket_prefix ON objects(bucket_id, object_key);
CREATE INDEX idx_objects_ipfs_cid ON objects(ipfs_cid);

-- migrate:down
DROP TABLE IF EXISTS objects;
DROP TABLE IF EXISTS buckets;
