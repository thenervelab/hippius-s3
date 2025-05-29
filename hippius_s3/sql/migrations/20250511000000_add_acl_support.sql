-- migrate:up

-- Create users table to store user information
CREATE TABLE users (
    user_id UUID PRIMARY KEY,
    seed_phrase TEXT UNIQUE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL
);

-- Create index for efficient lookup by seed phrase
CREATE INDEX idx_users_seed_phrase ON users(seed_phrase);

-- Create sub_accounts table to track parent-child relationships
CREATE TABLE sub_accounts (
    parent_user_id UUID NOT NULL REFERENCES users(user_id),
    sub_user_id UUID NOT NULL REFERENCES users(user_id),
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    PRIMARY KEY (parent_user_id, sub_user_id)
);

-- Create index for efficient lookup of sub-accounts by parent
CREATE INDEX idx_sub_accounts_parent ON sub_accounts(parent_user_id);

-- Create a default admin user to own existing buckets
INSERT INTO users (user_id, seed_phrase, created_at)
VALUES ('00000000-0000-0000-0000-000000000000', 'admin-seed-phrase', NOW());

-- Add owner_user_id column to buckets table
ALTER TABLE buckets ADD COLUMN owner_user_id UUID REFERENCES users(user_id);

-- Update existing buckets to use the admin user
UPDATE buckets SET owner_user_id = '00000000-0000-0000-0000-000000000000';

-- Create index for efficient lookup by owner
CREATE INDEX idx_buckets_owner ON buckets(owner_user_id);

-- Now add the NOT NULL constraint
ALTER TABLE buckets ALTER COLUMN owner_user_id SET NOT NULL;

-- migrate:down

-- Remove owner_user_id column from buckets table
ALTER TABLE buckets DROP COLUMN owner_user_id;

-- Drop sub_accounts table
DROP TABLE IF EXISTS sub_accounts;

-- Drop users table
DROP TABLE IF EXISTS users;
