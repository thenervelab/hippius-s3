-- migrate:up

-- Reinstate the version-row lock that 20260911090000 installed and that had to be pulled back out.
--
-- WHY IT WAS PULLED. That lock gave the objects trigger the order objects -> object_versions on
-- every overwrite, which is correct -- but two transactions took those rows the OTHER way round
-- through a FOREIGN KEY, with no SQL naming `objects` at all. `INSERT INTO parts` and
-- `INSERT INTO multipart_uploads` each carry an object_id FK, serviced by an implicit
--
--     SELECT 1 FROM ONLY objects x WHERE object_id = $1 FOR KEY SHARE OF x
--
-- issued at the INSERT, i.e. AFTER the `UPDATE object_versions` earlier in the same transaction.
-- The simple-PUT tail and the S4 append reserve were therefore object_versions -> objects, and
-- deadlocked against any concurrent same-key reserve: MEASURED at 4/48 failures at concurrency 8
-- and 46/192 at 32, each one a 500 returned after the whole body had been received and staged.
--
-- WHAT MADE IT SAFE TO PUT BACK. Both transactions now take the objects row up front via
-- lock_object_row_by_id.sql (PR #518), so their order conforms. That code must be DEPLOYED BEFORE
-- this migration applies -- which is why the fix shipped on its own, with no migration, instead of
-- being bundled here. Bundling them would have left a window lasting the whole fleet roll in which
-- the new trigger lock coexists with the old un-ordered code, which is exactly the shape of the
-- bug. With 0/528 deadlocks measured at concurrency 8/32/48 after the fix, and drift 0 at every
-- step, this restores the over-count fix without reopening the deadlock.
--
-- This is a forward migration rather than a re-run because 20260911090000 is already recorded as
-- applied on staging, where its function bodies were reverted out of band while #518 was written.
-- CREATE OR REPLACE is idempotent, so applying this to an environment that never saw the revert
-- (production) is a no-op that simply lands the same bodies.
--
-- The invariant this depends on, unchanged and now enforced by a guard that can see FK-implied
-- locks, SELECT ... FOR UPDATE, and SQL one level behind a helper call
-- (tests/unit/test_storage_usage_lock_order.py):
--
--     ALWAYS LOCK `objects` BEFORE `object_versions`. NEVER THE REVERSE.
SET LOCAL lock_timeout = '3s';

CREATE OR REPLACE FUNCTION storage_usage_objects_insert_trigger()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.deleted_at IS NULL THEN
        PERFORM storage_usage_emit(
            NEW.bucket_id,
            storage_usage_narrow(
                storage_usage_version_bytes_locked(NEW.object_id, NEW.current_object_version)::numeric
            )
        );
    END IF;
    RETURN NULL;
END;
$$;

-- Both sides locked, not just the outgoing one: the trigger also fires when only `deleted_at`
-- moves, where OLD and NEW name the SAME existing version that a concurrent finalize may be
-- resizing. When the two sides name one version the second call is a free no-op re-lock.
CREATE OR REPLACE FUNCTION storage_usage_objects_update_trigger()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    v_old bigint := 0;
    v_new bigint := 0;
BEGIN
    IF OLD.deleted_at IS NULL THEN
        v_old := storage_usage_version_bytes_locked(OLD.object_id, OLD.current_object_version);
    END IF;
    IF NEW.deleted_at IS NULL THEN
        v_new := storage_usage_version_bytes_locked(NEW.object_id, NEW.current_object_version);
    END IF;

    PERFORM storage_usage_apply(OLD.bucket_id, v_old, NEW.bucket_id, v_new);
    RETURN NULL;
END;
$$;

-- migrate:down

SET LOCAL lock_timeout = '3s';

-- Back to unlocked reads: no version lock, so no lock-order constraint and no deadlock hazard,
-- at the cost of the over-count under concurrent same-key overwrites.
CREATE OR REPLACE FUNCTION storage_usage_objects_insert_trigger()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.deleted_at IS NULL THEN
        PERFORM storage_usage_emit(
            NEW.bucket_id,
            storage_usage_narrow(storage_usage_version_bytes(NEW.object_id, NEW.current_object_version)::numeric)
        );
    END IF;
    RETURN NULL;
END;
$$;

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
