-- migrate:up

-- Bucket ACLs table
CREATE TABLE bucket_acls (
    id SERIAL PRIMARY KEY,
    bucket_name VARCHAR(255) NOT NULL,
    owner_id VARCHAR(255) NOT NULL,
    acl_json JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    UNIQUE(bucket_name)
);

CREATE INDEX idx_bucket_acls_bucket_name ON bucket_acls(bucket_name);
CREATE INDEX idx_bucket_acls_owner_id ON bucket_acls(owner_id);

-- Object ACLs table
CREATE TABLE object_acls (
    id SERIAL PRIMARY KEY,
    bucket_name VARCHAR(255) NOT NULL,
    object_key TEXT NOT NULL,
    owner_id VARCHAR(255) NOT NULL,
    acl_json JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    UNIQUE(bucket_name, object_key)
);

CREATE INDEX idx_object_acls_bucket_object ON object_acls(bucket_name, object_key);
CREATE INDEX idx_object_acls_owner_id ON object_acls(owner_id);

-- Add trigger to automatically update updated_at on bucket_acls
CREATE OR REPLACE FUNCTION update_bucket_acls_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER bucket_acls_updated_at
    BEFORE UPDATE ON bucket_acls
    FOR EACH ROW
    EXECUTE FUNCTION update_bucket_acls_updated_at();

-- Add trigger to automatically update updated_at on object_acls
CREATE OR REPLACE FUNCTION update_object_acls_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER object_acls_updated_at
    BEFORE UPDATE ON object_acls
    FOR EACH ROW
    EXECUTE FUNCTION update_object_acls_updated_at();

-- migrate:down

DROP TRIGGER IF EXISTS object_acls_updated_at ON object_acls;
DROP FUNCTION IF EXISTS update_object_acls_updated_at();
DROP TRIGGER IF EXISTS bucket_acls_updated_at ON bucket_acls;
DROP FUNCTION IF EXISTS update_bucket_acls_updated_at();
DROP TABLE IF EXISTS object_acls;
DROP TABLE IF EXISTS bucket_acls;
