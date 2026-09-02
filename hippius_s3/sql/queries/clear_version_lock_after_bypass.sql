-- $1 object_id (UUID), $2 object_version (BIGINT)
--
-- Clears the WORM state from a version that was just permanently deleted under an AUTHORISED
-- governance bypass. Only ever called on that path: a legal hold and a COMPLIANCE retention are
-- refused outright, so the only lock that can survive to this point is a GOVERNANCE retention the
-- bucket owner explicitly overrode with x-amz-bypass-governance-retention.
--
-- Without this the delete leaks its bytes forever. soft_delete_object_version sets only
-- deleted_at, so the row keeps a future object_lock_retain_until — and the enforcement predicate
-- in get_chunk_backend_identifiers has no concept of a bypass. It therefore still judges the
-- version locked and returns nothing, the unpinner reads that as "nothing to unpin" and drops the
-- request after its empty-retry budget, and find_versions_ready_for_reap /
-- find_objects_ready_for_hard_delete withhold the rows on the same predicate. The backend bytes,
-- on Arion and on every backup backend, are then stranded with no retry and no alert.
--
-- Clearing here rather than teaching the SQL gates about bypasses is deliberate: the gates must
-- keep holding with no API code running, which is what makes them the durability guarantee. The
-- authorisation decision belongs to the API, and this records its outcome in the one place every
-- downstream consumer already reads.
UPDATE object_versions
   SET object_lock_mode = NULL,
       object_lock_retain_until = NULL
 WHERE object_id = $1
   AND object_version = $2
   AND NOT object_lock_legal_hold;
