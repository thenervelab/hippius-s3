-- migrate:up

-- Admit the write-once tier `object_read_write_no_delete` (see hippius_s3/models/sub_token.py).
--
-- sub_token_scopes is one row per scoped sub-token — tiny — so re-adding the CHECK validated is a
-- sub-millisecond scan. SET LOCAL so a queued reader cannot stall the ACCESS EXCLUSIVE ALTER behind
-- it, and so the setting does not leak into later migrations on dbmate's shared connection.
SET LOCAL lock_timeout = '3s';

ALTER TABLE sub_token_scopes
    DROP CONSTRAINT IF EXISTS ck_sub_token_scopes_permission;
ALTER TABLE sub_token_scopes
    ADD CONSTRAINT ck_sub_token_scopes_permission CHECK (
        permission IN ('admin_read_write','admin_read','object_read_write','object_read_write_no_delete','object_read')
    );

-- migrate:down

SET LOCAL lock_timeout = '3s';

-- Refuses (the CHECK fails) while any row still uses the new tier: revoke or re-scope those
-- sub-tokens first rather than silently widening them to object_read_write.
ALTER TABLE sub_token_scopes
    DROP CONSTRAINT IF EXISTS ck_sub_token_scopes_permission;
ALTER TABLE sub_token_scopes
    ADD CONSTRAINT ck_sub_token_scopes_permission CHECK (
        permission IN ('admin_read_write','admin_read','object_read_write','object_read')
    );
