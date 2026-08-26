-- migrate:up

-- `main_account_id` is the bucket's owner, and the ACL layer grants FULL_CONTROL on an owner
-- match. The account middleware stamps unauthenticated callers with the literal string
-- "anonymous", so a row holding that value (or '') is owned by every anonymous caller at once.
-- Three code paths already refuse to write it; this makes the state unrepresentable so a fourth
-- added later cannot reintroduce it.
--
-- NOT VALID deliberately: it enforces on every INSERT and UPDATE from here on without scanning
-- the existing table, so the migration cannot fail on legacy rows. Legacy ownerless rows are a
-- data question, handled separately -- do not VALIDATE this constraint until they are re-owned.
ALTER TABLE buckets
    ADD CONSTRAINT ck_buckets_owner_not_sentinel
    CHECK (
        main_account_id IS NOT NULL
        AND btrim(main_account_id) <> ''
        AND lower(btrim(main_account_id)) NOT IN ('anonymous', 'none', 'null', 'undefined')
    ) NOT VALID;

-- migrate:down

ALTER TABLE buckets DROP CONSTRAINT IF EXISTS ck_buckets_owner_not_sentinel;
