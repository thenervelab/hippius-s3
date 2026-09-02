-- $1 object_id (UUID), $2 object_version (BIGINT), $3 mode (TEXT, nullable),
-- $4 retain_until (TIMESTAMPTZ, nullable), $5 legal_hold (BOOLEAN, NULLABLE = leave unchanged)
--
-- A NULL $5 leaves the legal hold as it was. Retention and legal hold are independent protections
-- with independent endpoints, so writing one must never silently clear the other — passing FALSE
-- from the retention path would drop a live legal hold and release an object nobody asked to
-- release.
UPDATE object_versions
SET object_lock_mode = $3,
    object_lock_retain_until = $4,
    object_lock_legal_hold = COALESCE($5, object_lock_legal_hold)
WHERE object_id = $1 AND object_version = $2;
