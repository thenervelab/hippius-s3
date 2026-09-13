-- Exclusive variant of lock_object_row_by_id, for the tail of a conditional write only.
--
-- Same position as lock_object_row_by_id — the first statement of the tail transaction, before any
-- object_versions row is touched — so the lock order stays objects -> object_versions (see that
-- query's header for the deadlock it prevents).
--
-- FOR UPDATE rather than KEY SHARE because the conditional re-check that follows must not interleave
-- with another writer finalizing the same object. Every PUT tail takes at least KEY SHARE on this row
-- and FOR UPDATE conflicts with it, so the check sees each concurrent finalize either fully committed
-- or not started. Unconditional PUTs keep KEY SHARE and are not serialized against each other.
--
-- $1: object_id (uuid)
SELECT object_id
FROM objects
WHERE object_id = $1
FOR UPDATE
