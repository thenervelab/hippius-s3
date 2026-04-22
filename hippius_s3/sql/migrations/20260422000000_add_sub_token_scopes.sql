-- migrate:up

CREATE TABLE sub_token_scopes (
    access_key_id  VARCHAR(255) PRIMARY KEY,
    account_id     VARCHAR(255) NOT NULL,
    permission     VARCHAR(32)  NOT NULL,
    bucket_scope   VARCHAR(16)  NOT NULL,
    bucket_ids     UUID[]       NOT NULL DEFAULT '{}',
    created_at     TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at     TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    CONSTRAINT ck_sub_token_scopes_permission CHECK (
        permission IN ('admin_read_write','admin_read','object_read_write','object_read')
    ),
    CONSTRAINT ck_sub_token_scopes_bucket_scope CHECK (
        bucket_scope IN ('all','specific')
    ),
    CONSTRAINT ck_sub_token_scopes_specific_needs_buckets CHECK (
        bucket_scope = 'all' OR array_length(bucket_ids, 1) > 0
    )
);

CREATE INDEX idx_sub_token_scopes_account_id ON sub_token_scopes(account_id);

CREATE OR REPLACE FUNCTION update_sub_token_scopes_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER sub_token_scopes_updated_at
    BEFORE UPDATE ON sub_token_scopes
    FOR EACH ROW
    EXECUTE FUNCTION update_sub_token_scopes_updated_at();

-- migrate:down

DROP TRIGGER IF EXISTS sub_token_scopes_updated_at ON sub_token_scopes;
DROP FUNCTION IF EXISTS update_sub_token_scopes_updated_at();
DROP TABLE IF EXISTS sub_token_scopes;
