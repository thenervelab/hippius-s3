-- migrate:up

-- Table for multipart uploads
CREATE TABLE multipart_uploads (
    upload_id UUID PRIMARY KEY,
    bucket_id UUID NOT NULL REFERENCES buckets(bucket_id) ON DELETE CASCADE,
    object_key TEXT NOT NULL,
    initiated_at TIMESTAMP WITH TIME ZONE NOT NULL,
    is_completed BOOLEAN DEFAULT FALSE,
    content_type TEXT,
    metadata JSONB,
    UNIQUE(bucket_id, object_key, upload_id)
);

-- Table for parts of a multipart upload
CREATE TABLE parts (
    part_id UUID PRIMARY KEY,
    upload_id UUID NOT NULL REFERENCES multipart_uploads(upload_id) ON DELETE CASCADE,
    part_number INTEGER NOT NULL,
    ipfs_cid TEXT NOT NULL,
    size_bytes BIGINT NOT NULL,
    etag TEXT NOT NULL,
    uploaded_at TIMESTAMP WITH TIME ZONE NOT NULL,
    UNIQUE(upload_id, part_number)
);

-- Indexes for efficient lookups
CREATE INDEX idx_multipart_uploads_bucket ON multipart_uploads(bucket_id);
CREATE INDEX idx_parts_upload ON parts(upload_id);

-- migrate:down
DROP TABLE IF EXISTS parts;
DROP TABLE IF EXISTS multipart_uploads;
