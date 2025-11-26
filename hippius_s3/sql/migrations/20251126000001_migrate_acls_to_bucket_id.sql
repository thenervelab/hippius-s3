-- migrate:up

-- This migration refactors ACL tables to use bucket_id and object_id instead of bucket_name and object_key.
-- This provides referential integrity via foreign keys and automatic cleanup via CASCADE DELETE.

-- ============================================================================
-- PHASE 1: Add New Columns (nullable initially)
-- ============================================================================

ALTER TABLE bucket_acls ADD COLUMN bucket_id UUID;

ALTER TABLE object_acls ADD COLUMN bucket_id UUID;
ALTER TABLE object_acls ADD COLUMN object_id UUID;

-- ============================================================================
-- PHASE 2: Populate New Columns from Existing Data
-- ============================================================================

UPDATE bucket_acls ba
SET bucket_id = b.bucket_id
FROM buckets b
WHERE ba.bucket_name = b.bucket_name;

UPDATE object_acls oa
SET bucket_id = b.bucket_id, object_id = o.object_id
FROM objects o
JOIN buckets b ON o.bucket_id = b.bucket_id
WHERE oa.bucket_name = b.bucket_name AND oa.object_key = o.object_key;

-- ============================================================================
-- PHASE 3: Handle Orphaned ACLs (delete ACLs for non-existent buckets/objects)
-- ============================================================================

DELETE FROM bucket_acls WHERE bucket_id IS NULL;

DELETE FROM object_acls WHERE bucket_id IS NULL OR object_id IS NULL;

-- ============================================================================
-- PHASE 4: Add Constraints and Foreign Keys
-- ============================================================================

ALTER TABLE bucket_acls ALTER COLUMN bucket_id SET NOT NULL;

ALTER TABLE object_acls ALTER COLUMN bucket_id SET NOT NULL;
ALTER TABLE object_acls ALTER COLUMN object_id SET NOT NULL;

ALTER TABLE bucket_acls DROP CONSTRAINT IF EXISTS bucket_acls_bucket_name_key;
ALTER TABLE object_acls DROP CONSTRAINT IF EXISTS object_acls_bucket_name_object_key_key;

ALTER TABLE bucket_acls ADD CONSTRAINT bucket_acls_bucket_id_key UNIQUE(bucket_id);
ALTER TABLE object_acls ADD CONSTRAINT object_acls_bucket_object_key UNIQUE(bucket_id, object_id);

ALTER TABLE bucket_acls
    ADD CONSTRAINT fk_bucket_acls_bucket
    FOREIGN KEY (bucket_id) REFERENCES buckets(bucket_id) ON DELETE CASCADE;

ALTER TABLE object_acls
    ADD CONSTRAINT fk_object_acls_bucket
    FOREIGN KEY (bucket_id) REFERENCES buckets(bucket_id) ON DELETE CASCADE;

ALTER TABLE object_acls
    ADD CONSTRAINT fk_object_acls_object
    FOREIGN KEY (object_id) REFERENCES objects(object_id) ON DELETE CASCADE;

-- ============================================================================
-- PHASE 5: Drop Old Columns and Indexes
-- ============================================================================

DROP INDEX IF EXISTS idx_bucket_acls_bucket_name;
DROP INDEX IF EXISTS idx_object_acls_bucket_object;

ALTER TABLE bucket_acls DROP COLUMN bucket_name;

ALTER TABLE object_acls DROP COLUMN bucket_name;
ALTER TABLE object_acls DROP COLUMN object_key;

-- ============================================================================
-- PHASE 6: Create New Indexes
-- ============================================================================

CREATE INDEX idx_bucket_acls_bucket_id ON bucket_acls(bucket_id);
CREATE INDEX idx_object_acls_bucket_object ON object_acls(bucket_id, object_id);

-- migrate:down

ALTER TABLE bucket_acls DROP CONSTRAINT IF EXISTS fk_bucket_acls_bucket;
ALTER TABLE object_acls DROP CONSTRAINT IF EXISTS fk_object_acls_bucket;
ALTER TABLE object_acls DROP CONSTRAINT IF EXISTS fk_object_acls_object;

DROP INDEX IF EXISTS idx_bucket_acls_bucket_id;
DROP INDEX IF EXISTS idx_object_acls_bucket_object;

ALTER TABLE bucket_acls ADD COLUMN bucket_name VARCHAR(255);
ALTER TABLE object_acls ADD COLUMN bucket_name VARCHAR(255);
ALTER TABLE object_acls ADD COLUMN object_key TEXT;

UPDATE bucket_acls ba
SET bucket_name = b.bucket_name
FROM buckets b
WHERE ba.bucket_id = b.bucket_id;

UPDATE object_acls oa
SET bucket_name = b.bucket_name, object_key = o.object_key
FROM objects o
JOIN buckets b ON o.bucket_id = b.bucket_id
WHERE oa.object_id = o.object_id;

ALTER TABLE bucket_acls ALTER COLUMN bucket_name SET NOT NULL;
ALTER TABLE object_acls ALTER COLUMN bucket_name SET NOT NULL;
ALTER TABLE object_acls ALTER COLUMN object_key SET NOT NULL;

ALTER TABLE bucket_acls DROP CONSTRAINT IF EXISTS bucket_acls_bucket_id_key;
ALTER TABLE object_acls DROP CONSTRAINT IF EXISTS object_acls_bucket_object_key;

ALTER TABLE bucket_acls ADD CONSTRAINT bucket_acls_bucket_name_key UNIQUE(bucket_name);
ALTER TABLE object_acls ADD CONSTRAINT object_acls_bucket_name_object_key_key UNIQUE(bucket_name, object_key);

ALTER TABLE bucket_acls DROP COLUMN bucket_id;
ALTER TABLE object_acls DROP COLUMN bucket_id;
ALTER TABLE object_acls DROP COLUMN object_id;

CREATE INDEX idx_bucket_acls_bucket_name ON bucket_acls(bucket_name);
CREATE INDEX idx_object_acls_bucket_object ON object_acls(bucket_name, object_key);
