-- migrate:up

-- This migration is a placeholder that documents the manual cross-database copy.
-- Since dbmate runs migrations in a single database context, we cannot directly
-- copy data from hippius database to hippius_keys database within a migration.
--
-- MANUAL MIGRATION REQUIRED:
-- Run the following SQL command from the PostgreSQL host to copy encryption keys:
--
-- psql -U postgres -c "
--   INSERT INTO hippius_keys.encryption_keys (subaccount_id, encryption_key_b64, created_at)
--   SELECT subaccount_id, encryption_key_b64, created_at
--   FROM hippius.encryption_keys
--   WHERE NOT EXISTS (
--     SELECT 1 FROM hippius_keys.encryption_keys hk
--     WHERE hk.subaccount_id = hippius.encryption_keys.subaccount_id
--     AND hk.encryption_key_b64 = hippius.encryption_keys.encryption_key_b64
--   );
-- "
--
-- Or from docker:
-- docker exec hippius-s3-db-1 psql -U postgres -c "
--   INSERT INTO hippius_keys.encryption_keys (subaccount_id, encryption_key_b64, created_at)
--   SELECT subaccount_id, encryption_key_b64, created_at
--   FROM hippius.encryption_keys
--   WHERE NOT EXISTS (
--     SELECT 1 FROM hippius_keys.encryption_keys hk
--     WHERE hk.subaccount_id = hippius.encryption_keys.subaccount_id
--     AND hk.encryption_key_b64 = hippius.encryption_keys.encryption_key_b64
--   );
-- "

DO $$
BEGIN
    RAISE NOTICE '=============================================================================';
    RAISE NOTICE 'MANUAL MIGRATION REQUIRED - Cross-database copy needed';
    RAISE NOTICE 'See migration file comments for SQL commands to run';
    RAISE NOTICE 'This copies encryption keys from hippius database to hippius_keys database';
    RAISE NOTICE '=============================================================================';
END $$;

-- migrate:down

-- This migration created no schema changes, only documented manual steps.
-- No rollback action needed.

DO $$
BEGIN
    RAISE NOTICE 'No automatic rollback - manual migration was required for this step';
END $$;
