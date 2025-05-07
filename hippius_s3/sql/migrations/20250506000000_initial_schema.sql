-- migrate:up
-- Initial schema for Hippius S3 Gateway

-- Files table to store metadata about uploaded files
CREATE TABLE IF NOT EXISTS files (
    file_id UUID PRIMARY KEY,
    ipfs_cid TEXT NOT NULL,
    file_name TEXT NOT NULL,
    content_type TEXT NOT NULL,
    file_size BIGINT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    metadata JSONB
);

-- Index for faster lookups by IPFS CID
CREATE INDEX IF NOT EXISTS idx_files_ipfs_cid ON files (ipfs_cid);

-- Index for faster lookups by creation time
CREATE INDEX IF NOT EXISTS idx_files_created_at ON files (created_at);

-- migrate:down
DROP TABLE IF EXISTS files;
