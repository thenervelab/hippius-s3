-- migrate:up

-- Create cid table to store IPFS CIDs separately
CREATE TABLE cids (
    id UUID PRIMARY KEY,
    cid TEXT NOT NULL UNIQUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Create index for efficient CID lookups
CREATE INDEX idx_cids_cid ON cids(cid);

-- Remove NOT NULL constraint from ipfs_cid column in objects table
ALTER TABLE objects ALTER COLUMN ipfs_cid DROP NOT NULL;

-- Add cid_id column to objects table to link to cids table
ALTER TABLE objects ADD COLUMN cid_id UUID REFERENCES cids(id);

-- Add cid_id column to parts table to link to cids table
ALTER TABLE parts ADD COLUMN cid_id UUID REFERENCES cids(id);

-- Add cid_id column to files table to link to cids table (if files table exists)
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'files') THEN
        ALTER TABLE files ADD COLUMN cid_id UUID REFERENCES cids(id);
        ALTER TABLE files ALTER COLUMN ipfs_cid DROP NOT NULL;
        CREATE INDEX idx_files_cid_id ON files(cid_id);
    END IF;
END $$;

-- Create indexes for efficient lookups
CREATE INDEX idx_objects_cid_id ON objects(cid_id);
CREATE INDEX idx_parts_cid_id ON parts(cid_id);

-- migrate:down

-- Remove indexes
DROP INDEX IF EXISTS idx_parts_cid_id;
DROP INDEX IF EXISTS idx_objects_cid_id;

-- Remove files table changes (if files table exists)
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'files') THEN
        DROP INDEX IF EXISTS idx_files_cid_id;
        ALTER TABLE files DROP COLUMN IF EXISTS cid_id;
        ALTER TABLE files ALTER COLUMN ipfs_cid SET NOT NULL;
    END IF;
END $$;

-- Remove cid_id columns
ALTER TABLE parts DROP COLUMN IF EXISTS cid_id;
ALTER TABLE objects DROP COLUMN IF EXISTS cid_id;

-- Add back NOT NULL constraint to ipfs_cid
ALTER TABLE objects ALTER COLUMN ipfs_cid SET NOT NULL;

-- Drop cid table
DROP INDEX IF EXISTS idx_cids_cid;
DROP TABLE IF EXISTS cids;
