-- migrate:up
-- Object Lock Tier 2: WORM state lives on the VERSION, never the key. Two versions of one key
-- can carry different modes and different expiry dates, which is why these are not on `objects`.
--
-- Retention and legal hold are INDEPENDENT protections: either one locks the version, and a
-- version with an expired retention but a live legal hold is still locked. Modelling them as one
-- field is the classic implementation bug.
ALTER TABLE object_versions
    ADD COLUMN IF NOT EXISTS object_lock_mode TEXT,
    ADD COLUMN IF NOT EXISTS object_lock_retain_until TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS object_lock_legal_hold BOOLEAN NOT NULL DEFAULT FALSE;

-- Only GOVERNANCE and COMPLIANCE exist in S3. NULL means "no retention" (a legal hold alone is
-- still a valid lock). Enforced here as well as in the handler so a direct DB write cannot invent
-- a third mode that the enforcement predicate would then not recognise.
ALTER TABLE object_versions
    DROP CONSTRAINT IF EXISTS object_versions_object_lock_mode_check;
ALTER TABLE object_versions
    ADD CONSTRAINT object_versions_object_lock_mode_check
    CHECK (object_lock_mode IS NULL OR object_lock_mode IN ('GOVERNANCE', 'COMPLIANCE'));

-- A mode without a date cannot express a retention, and a date without a mode cannot be enforced
-- (bypass rules are per-mode). They travel together or not at all.
ALTER TABLE object_versions
    DROP CONSTRAINT IF EXISTS object_versions_object_lock_retention_pair_check;
ALTER TABLE object_versions
    ADD CONSTRAINT object_versions_object_lock_retention_pair_check
    CHECK ((object_lock_mode IS NULL) = (object_lock_retain_until IS NULL));

-- Partial: locked versions are the rare case, and every read is "is THIS version locked".
CREATE INDEX IF NOT EXISTS idx_object_versions_locked
    ON object_versions (object_id, object_version)
    WHERE object_lock_retain_until IS NOT NULL OR object_lock_legal_hold;

-- migrate:down
DROP INDEX IF EXISTS idx_object_versions_locked;
ALTER TABLE object_versions
    DROP CONSTRAINT IF EXISTS object_versions_object_lock_retention_pair_check;
ALTER TABLE object_versions
    DROP CONSTRAINT IF EXISTS object_versions_object_lock_mode_check;
ALTER TABLE object_versions
    DROP COLUMN IF EXISTS object_lock_legal_hold,
    DROP COLUMN IF EXISTS object_lock_retain_until,
    DROP COLUMN IF EXISTS object_lock_mode;
