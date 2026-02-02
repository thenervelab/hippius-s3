-- migrate:up
ALTER TABLE parts ALTER COLUMN ipfs_cid DROP NOT NULL;

-- migrate:down
UPDATE parts SET ipfs_cid = 'pending' WHERE ipfs_cid IS NULL;
ALTER TABLE parts ALTER COLUMN ipfs_cid SET NOT NULL;
