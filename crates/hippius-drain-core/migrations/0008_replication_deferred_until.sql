-- Backoff timestamp for a part whose drain deferred (the upload enqueue was not ready
-- because object_versions.address is not finalized yet). Without it, a deferred part is
-- released straight back to 'pending' and re-claimed on the very next poll, so the drain
-- spins on not-ready parts (an in-progress or abandoned MPU) and can starve the parts
-- that ARE ready to upload. `claim_part` skips a 'pending' row whose deferred_until is
-- still in the future, so a deferred part backs off until then; `release_part` (a
-- Ceph-write failure, which should retry promptly) clears it. NULL = not deferred.
ALTER TABLE cephor_replication_status
    ADD COLUMN deferred_until TIMESTAMPTZ;
