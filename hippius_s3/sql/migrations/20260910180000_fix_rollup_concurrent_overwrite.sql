-- migrate:up

-- Stop the storage counter over-billing under concurrent overwrites of the same object key.
--
-- THE BUG. `bucket_storage_usage` drifted UPWARD whenever two writers overwrote one key at the
-- same time. Measured: 200 concurrent same-key overwrites at concurrency 32 left the counter
-- +1592 bytes high, while 200 concurrent DISTINCT-key PUTs at the same concurrency drifted zero.
-- A mixed six-path workload over 960 operations drifted +2,020,864. Always upward, so it
-- over-bills, and it accumulated: a run that deleted every object it created left truth at 77
-- bytes and the counter at 930,235 -- WITH THE LEDGER DRAINED TO ZERO. A drained ledger is
-- therefore not evidence the counter is right, which is what made this hard to notice.
--
-- THE MECHANISM, read off an instrumented trace of three concurrent same-key PUTs rather than
-- reasoned about. Each row is one trigger firing:
--
--   # trigger          old->new  sizes    current_seen  v_old  v_new  emitted
--   1 versions_update  v1->v1    0->2000  1             0      2000   +2000
--   2 objects_update   v1->v2    -        2             0      0      0     <-- should be -2000
--   3 objects_update   v2->v3    -        3             0      0      0     <-- should be -1000
--   4 versions_update  v2->v2    0->1000  2             0      1000   +1000
--   5 versions_update  v3->v3    0->3000  3             0      3000   +3000
--
-- Row 2 is the leak: v_old reads 0 for v1 although row 1 had already COMMITTED v1 at 2000. Every
-- version is ADDED by its own finalize -- correctly, it is current at that instant -- and never
-- SUBTRACTED, because the successor's reserve computed the decrement from a size that was still 0
-- in its snapshot. Ledger 6000 against a truth of 3000.
--
-- WHY IT ONLY BITES THE TWO-STEP PUT. object_writer reserves a version at size 0 and sets the real
-- size in a SEPARATE statement and transaction. A single-statement upsert at the final size never
-- drifts, at any concurrency -- that was the bisection that located this.
--
-- WHY MARKING THE HELPERS VOLATILE DOES NOT FIX IT, since that is the obvious first idea and it
-- was tried: the reserve's UPDATE hits `objects` and the finalize's UPDATE hits `object_versions`
-- -- different rows in different tables. No lock conflict means no EvalPlanQual re-read, so the
-- trigger's lookup stays pinned to the firing statement's snapshot no matter what the function's
-- volatility says. (A forced lock conflict on the objects row DOES see the commit, which is why a
-- deterministic two-transaction interleaving reports "correct" and disagrees with a concurrency
-- stress. Both results are real; they measure different things.)
--
-- So the constraint documented in 20260910120000 -- "never repoint current_object_version AND edit
-- the outgoing version in one STATEMENT" -- was too narrow. The real hazard is ACROSS
-- TRANSACTIONS: the reserve's decrement is computed before the outgoing version's final size is
-- visible to it.
--
-- THE FIX. Lock the outgoing version row instead of reading it unlocked, so a concurrent finalize
-- of that row must serialise with us and we re-read the committed size. FOR NO KEY UPDATE rather
-- than FOR UPDATE: it does not conflict with the KEY SHARE locks the foreign keys take, so it
-- blocks a competing size change without blocking ordinary readers or FK checks.
--
-- LOCK-ORDER INVARIANT THIS INTRODUCES, and the one thing to preserve when touching these tables:
--
--     ALWAYS LOCK `objects` BEFORE `object_versions`. NEVER THE REVERSE.
--
-- This trigger now takes an object_versions lock while its firing statement already holds the
-- objects row -- so any transaction that locks object_versions first and objects second would
-- deadlock against every reserve. No current path does: the only transaction that touches both
-- (delete_object_endpoint.py) locks the objects row up front with `FOR UPDATE OF o` via
-- lock_object_and_get_version, deliberately and with a comment saying so. Verified empirically as
-- well as by inspection -- a six-path adversarial probe over 2,880 operations on one contended
-- object found zero deadlocks, and a control that deliberately injects the reverse order produces
-- 236, so that zero is a real zero rather than an insensitive test.
-- tests/unit/test_sql_lock_order.py fails if a new transaction breaks the invariant.
CREATE OR REPLACE FUNCTION storage_usage_objects_update_trigger()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    v_old bigint := 0;
    v_new bigint := 0;
    v_locked bigint;
BEGIN
    IF OLD.deleted_at IS NULL THEN
        -- FOR NO KEY UPDATE, not a plain read: see the header. Blocking here is the point.
        SELECT ov.size_bytes
          INTO v_locked
          FROM object_versions ov
         WHERE ov.object_id = OLD.object_id
           AND ov.object_version = OLD.current_object_version
           AND ov.deleted_at IS NULL
           AND NOT ov.is_delete_marker
           FOR NO KEY UPDATE;
        v_old := COALESCE(v_locked, 0);
    END IF;

    IF NEW.deleted_at IS NULL THEN
        -- The incoming version was written by this same statement, so it is visible without a lock.
        v_new := storage_usage_version_bytes(NEW.object_id, NEW.current_object_version);
    END IF;

    PERFORM storage_usage_apply(OLD.bucket_id, v_old, NEW.bucket_id, v_new);
    RETURN NULL;
END;
$$;

-- migrate:down

CREATE OR REPLACE FUNCTION storage_usage_objects_update_trigger()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    v_old bigint := 0;
    v_new bigint := 0;
BEGIN
    IF OLD.deleted_at IS NULL THEN
        v_old := storage_usage_version_bytes(OLD.object_id, OLD.current_object_version);
    END IF;
    IF NEW.deleted_at IS NULL THEN
        v_new := storage_usage_version_bytes(NEW.object_id, NEW.current_object_version);
    END IF;
    PERFORM storage_usage_apply(OLD.bucket_id, v_old, NEW.bucket_id, v_new);
    RETURN NULL;
END;
$$;
