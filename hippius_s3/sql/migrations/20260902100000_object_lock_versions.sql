-- migrate:up
-- Object Lock Tier 2: WORM state lives on the VERSION, never the key. Two versions of one key
-- can carry different modes and different expiry dates, which is why these are not on `objects`.
--
-- Retention and legal hold are INDEPENDENT protections: either one locks the version, and a
-- version with an expired retention but a live legal hold is still locked. Modelling them as one
-- field is the classic implementation bug.
--
-- ONLINE SAFETY: `object_versions` is ~152M rows / ~79 GB in production and migrations run on
-- deploy against the live primary, so every statement here must be O(1) catalogue work:
--   * ADD COLUMN with a non-volatile DEFAULT has not rewritten the heap since PG11 — the default
--     is recorded in pg_attribute.attmissingval — so this is metadata-only even with NOT NULL.
--   * The CHECK constraints go in NOT VALID, which skips the verification scan. Adding them
--     validated seq-scans 79 GB under ACCESS EXCLUSIVE, stalling every read and write on the
--     table for the length of the scan — a production outage, not a slow deploy.
-- The verification scan and the index build are the two genuinely expensive steps; they are split
-- into the following two migrations so each can take a lock mode that does not block traffic.
-- docs/runbooks/object-lock-migration.md drives all three online, ahead of the deploy.
ALTER TABLE object_versions
    ADD COLUMN IF NOT EXISTS object_lock_mode TEXT,
    ADD COLUMN IF NOT EXISTS object_lock_retain_until TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS object_lock_legal_hold BOOLEAN NOT NULL DEFAULT FALSE;

-- Only GOVERNANCE and COMPLIANCE exist in S3. NULL means "no retention" (a legal hold alone is
-- still a valid lock). Enforced here as well as in the handler so a direct DB write cannot invent
-- a third mode that the enforcement predicate would then not recognise.
--
-- Guarded on pg_constraint rather than DROP-then-ADD: where the pre-step has already added AND
-- validated these, a blind re-create would silently demote a validated constraint to NOT VALID.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'object_versions'::regclass
          AND conname = 'object_versions_object_lock_mode_check'
    ) THEN
        ALTER TABLE object_versions
            ADD CONSTRAINT object_versions_object_lock_mode_check
            CHECK (object_lock_mode IS NULL OR object_lock_mode IN ('GOVERNANCE', 'COMPLIANCE'))
            NOT VALID;
    END IF;
END $$;

-- A mode without a date cannot express a retention, and a date without a mode cannot be enforced
-- (bypass rules are per-mode). They travel together or not at all.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'object_versions'::regclass
          AND conname = 'object_versions_object_lock_retention_pair_check'
    ) THEN
        ALTER TABLE object_versions
            ADD CONSTRAINT object_versions_object_lock_retention_pair_check
            CHECK ((object_lock_mode IS NULL) = (object_lock_retain_until IS NULL))
            NOT VALID;
    END IF;
END $$;

-- migrate:down
ALTER TABLE object_versions
    DROP CONSTRAINT IF EXISTS object_versions_object_lock_retention_pair_check;
ALTER TABLE object_versions
    DROP CONSTRAINT IF EXISTS object_versions_object_lock_mode_check;
ALTER TABLE object_versions
    DROP COLUMN IF EXISTS object_lock_legal_hold,
    DROP COLUMN IF EXISTS object_lock_retain_until,
    DROP COLUMN IF EXISTS object_lock_mode;
