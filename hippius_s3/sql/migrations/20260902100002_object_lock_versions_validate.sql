-- migrate:up

-- Promote the two Object Lock CHECK constraints from NOT VALID to validated.
--
-- Separated from 20260902100000 because this is the step that actually reads the table. VALIDATE
-- CONSTRAINT seq-scans the heap, but it takes only SHARE UPDATE EXCLUSIVE — reads and writes
-- continue throughout; it conflicts with other DDL and with autovacuum on this table, not with
-- traffic. That is the whole reason the constraints went in NOT VALID one migration earlier:
-- ADD CONSTRAINT ... (validated) would have done the same scan under ACCESS EXCLUSIVE.
--
-- Skipping this step is survivable — a NOT VALID CHECK is still enforced against every INSERT and
-- UPDATE from here on, and the only rows it leaves unverified are the pre-existing ones, which
-- carry NULL/false in all three columns by construction and therefore satisfy both predicates.
-- It is run anyway so production's schema matches staging's rather than quietly diverging.
--
-- A no-op when the constraint is already validated: Postgres checks convalidated first and skips
-- the scan, so re-running after the pre-step costs nothing.
ALTER TABLE object_versions VALIDATE CONSTRAINT object_versions_object_lock_mode_check;

ALTER TABLE object_versions VALIDATE CONSTRAINT object_versions_object_lock_retention_pair_check;

-- migrate:down

-- Irreversible by design: there is no ALTER ... INVALIDATE CONSTRAINT, and demoting a validated
-- constraint would mean dropping and re-adding it NOT VALID. The down migration of
-- 20260902100000 drops both constraints outright, which is the real undo.
SELECT 1;
