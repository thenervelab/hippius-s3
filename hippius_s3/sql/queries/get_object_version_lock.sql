-- $1 object_id (UUID), $2 object_version (BIGINT)
--
-- Lock state for ONE version. Selected on its own rather than joined into the delete queries so
-- the enforcement predicate always sees the same three columns regardless of caller.
SELECT object_lock_mode, object_lock_retain_until, object_lock_legal_hold
FROM object_versions
WHERE object_id = $1 AND object_version = $2;
