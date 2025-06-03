-- migrate:up

-- Step 1: Drop foreign key constraints that depend on buckets and users
ALTER TABLE objects DROP CONSTRAINT IF EXISTS objects_bucket_id_fkey;
ALTER TABLE multipart_uploads DROP CONSTRAINT IF EXISTS multipart_uploads_bucket_id_fkey;
ALTER TABLE buckets DROP CONSTRAINT IF EXISTS buckets_owner_user_id_fkey;
ALTER TABLE sub_accounts DROP CONSTRAINT IF EXISTS sub_accounts_parent_user_id_fkey;
ALTER TABLE sub_accounts DROP CONSTRAINT IF EXISTS sub_accounts_sub_user_id_fkey;
ALTER TABLE parts DROP CONSTRAINT IF EXISTS parts_upload_id_fkey;

-- Step 2: Clear existing data
DELETE FROM parts;
DELETE FROM multipart_uploads;
DELETE FROM objects;
DELETE FROM buckets;
DELETE FROM sub_accounts;
DELETE FROM users;

-- Step 3: Drop old tables completely
DROP TABLE IF EXISTS sub_accounts;
DROP TABLE IF EXISTS users;

-- Step 4: Create new users table with main_account_id as primary key
CREATE TABLE users (
    main_account_id TEXT PRIMARY KEY,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- Step 5: Update buckets table to use main_account_id instead of owner_user_id
ALTER TABLE buckets DROP COLUMN owner_user_id;
ALTER TABLE buckets ADD COLUMN main_account_id TEXT NOT NULL DEFAULT '';
ALTER TABLE buckets ALTER COLUMN main_account_id DROP DEFAULT;

-- Step 6: Update bucket constraints and indexes
DROP INDEX IF EXISTS buckets_name_owner_unique;
DROP INDEX IF EXISTS idx_buckets_name_owner;
DROP INDEX IF EXISTS idx_buckets_owner;

ALTER TABLE buckets ADD CONSTRAINT buckets_name_owner_unique UNIQUE(bucket_name, main_account_id);
CREATE INDEX idx_buckets_name_owner ON buckets(bucket_name, main_account_id);
CREATE INDEX idx_buckets_main_account ON buckets(main_account_id);

-- Step 7: Restore foreign key constraint from buckets to users (using main_account_id)
ALTER TABLE buckets ADD CONSTRAINT fk_buckets_main_account
    FOREIGN KEY (main_account_id) REFERENCES users(main_account_id) ON DELETE CASCADE;

-- Step 8: Restore foreign key constraints to buckets
ALTER TABLE objects ADD CONSTRAINT fk_objects_bucket
    FOREIGN KEY (bucket_id) REFERENCES buckets(bucket_id) ON DELETE CASCADE;

ALTER TABLE multipart_uploads ADD CONSTRAINT fk_multipart_uploads_bucket
    FOREIGN KEY (bucket_id) REFERENCES buckets(bucket_id) ON DELETE CASCADE;

-- Step 9: Restore parts table foreign key
ALTER TABLE parts ADD CONSTRAINT fk_parts_upload
    FOREIGN KEY (upload_id) REFERENCES multipart_uploads(upload_id) ON DELETE CASCADE;

-- migrate:down

-- This migration cannot be easily rolled back because it involves a fundamental change
-- in the user identification system from seed_phrase to main_account_id
-- A rollback would require:
-- 1. Recreating the old users table with seed_phrase
-- 2. Recreating the old buckets table with owner_user_id
-- 3. Manually mapping main_account_id back to seed_phrase (which we don't store)

-- For safety, we'll create the old structure but without data
CREATE TABLE users_old (
    user_id UUID PRIMARY KEY,
    seed_phrase TEXT UNIQUE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL
);

CREATE INDEX idx_users_old_seed_phrase ON users_old(seed_phrase);

CREATE TABLE sub_accounts_old (
    parent_user_id UUID NOT NULL REFERENCES users_old(user_id),
    sub_user_id UUID NOT NULL REFERENCES users_old(user_id),
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    PRIMARY KEY (parent_user_id, sub_user_id)
);

CREATE INDEX idx_sub_accounts_old_parent ON sub_accounts_old(parent_user_id);

-- Drop new tables
DROP TABLE IF EXISTS buckets;
DROP TABLE IF EXISTS users;

-- Rename old tables back
ALTER TABLE users_old RENAME TO users;
ALTER TABLE sub_accounts_old RENAME TO sub_accounts;

-- Note: Data loss will occur during rollback - this is an irreversible migration
-- in terms of preserving existing user relationships
