-- migrate:up

-- Account-level suspension state for admin billing enforcement (issue #421).
-- Row present = suspended; row absent = active. Keyed on the main-account SS58 so one
-- row covers every credential of the account (master token, sub-tokens, presigned,
-- bearer). Deliberately NOT an FK to users: the backend may suspend an account before
-- it has ever created a bucket.
CREATE TABLE account_suspensions (
    account_id  VARCHAR(255) PRIMARY KEY,
    mode        VARCHAR(16)  NOT NULL,
    created_at  TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    CONSTRAINT ck_account_suspensions_mode CHECK (mode IN ('full', 'read_only'))
);

CREATE OR REPLACE FUNCTION update_account_suspensions_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER account_suspensions_updated_at
    BEFORE UPDATE ON account_suspensions
    FOR EACH ROW
    EXECUTE FUNCTION update_account_suspensions_updated_at();

-- migrate:down

DROP TRIGGER IF EXISTS account_suspensions_updated_at ON account_suspensions;
DROP FUNCTION IF EXISTS update_account_suspensions_updated_at();
DROP TABLE IF EXISTS account_suspensions;
