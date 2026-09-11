-- migrate:up

-- FIX: the rollup over-counted every superseded version under concurrent overwrites of one key.
--
-- WHAT WENT WRONG. A PUT is TWO transactions (hippius_s3/writer/object_writer.py): the reserve
-- points objects.current_object_version at a brand-new version whose size_bytes is 0, then --
-- after streaming the whole body, which can take minutes -- a separate transaction sets the real
-- size. `storage_usage_objects_update_trigger` computes its decrement by looking up the OUTGOING
-- version's size. Between another writer's reserve and its finalize, that size is still 0. So the
-- overwrite decremented 0 instead of the outgoing version's real size, while the outgoing version's
-- own finalize had already added its full size (it was still current when it ran). Every version
-- ever written was added exactly once and never subtracted.
--
-- Read off instrumented triggers, three concurrent same-key PUTs of 1000/2000/3000 bytes:
--
--    # trg              old->new  sizes      cur_seen  v_old  v_new  emitted
--    1 objects_update   v1->v2    -          2         0      0      +0      <-- should be -1000
--    2 versions_update  v1->v1    0->1000    1         0      1000   +1000
--    3 objects_update   v2->v3    -          3         0      0      +0      <-- should be -2000
--    4 versions_update  v2->v2    0->2000    2         0      2000   +2000
--    5 versions_update  v3->v3    0->3000    3         0      3000   +3000
--
--   ledger = 6000 (every version), truth = 3000 (the one that ended up current). The ledger total
--   was 1000+2000+3000 on EVERY run: the drift is exactly the sum of the superseded versions.
--   Measured drift with 12 rounds of the real statements: +0 at concurrency 1, +575k at 4, +3.48M
--   at 16. The same test with a single-statement upsert at the final size drifts 0 at every
--   concurrency, which is what isolated it to the two-step shape.
--
-- IT IS NOT A SNAPSHOT PROBLEM, which is what made it hard to find. An AFTER ROW trigger's reads
-- take a FRESH snapshot when the trigger fires, even a plain SELECT, even when the statement spent
-- time blocked on a row lock -- verified directly (a value committed by a third transaction while
-- the statement was blocked IS visible to the trigger). Declaring the helpers VOLATILE therefore
-- changes nothing, and did not. The defect is a MISSING LOCK: the reserve's UPDATE of `objects` and
-- the finalize's UPDATE of `object_versions` touch different rows in different tables, so nothing
-- made them serialise. "Fresh" only ever meant "whatever happened to be committed at that instant".
--
-- THE FIX. Read the outgoing version's size with a row lock instead of an unlocked lookup. The lock
-- conflicts with the concurrent finalize's UPDATE of that same row, so the two can no longer
-- overlap: whichever runs second blocks, and its (fresh) read then sees the other's committed work.
-- If the reserve wins, the finalize afterwards finds itself no longer current and adds nothing. If
-- the finalize wins, the reserve afterwards reads the real size and subtracts it. Both orderings
-- net to the truth.
--
-- The design's documented constraint was too narrow. It said: never repoint current_object_version
-- AND edit the outgoing version in the same STATEMENT. The real hazard is ACROSS TRANSACTIONS, and
-- production has been on it since the two-step PUT existed.
--
-- WHY `FOR NO KEY UPDATE` and not the alternatives:
--   * FOR KEY SHARE does not conflict with a plain UPDATE of size_bytes, so it would not serialise
--     anything -- the whole point.
--   * FOR SHARE would serialise correctly here (concurrent writers to one key already queue on the
--     objects row) but understates the intent: this trigger reads a value it is about to act on
--     exclusively.
--   * FOR UPDATE conflicts with FOR KEY SHARE, which is exactly what `parts.parts_object_version_fk`
--     takes on the parent object_versions row for every part INSERT. It would put a lock wait on the
--     MPU hot path to fix a billing count. FOR NO KEY UPDATE does not conflict with KEY SHARE.
--   * No form of this blocks plain readers; SELECT never takes a conflicting lock.
--
-- LOCK ORDER, AND THE INVARIANT THIS IMPOSES. The trigger runs while the statement already holds
-- the `objects` row lock, so its order is objects -> object_versions. Any transaction that takes
-- them the other way round can deadlock against a concurrent overwrite. Both existing paths that
-- lock an object_versions row and then an objects row were checked:
--   * the versioned DELETE (delete_object_endpoint) already takes `FOR UPDATE OF o` on the objects
--     row FIRST, in lock_object_and_get_version.sql, before soft_delete_object_version. Correct.
--   * abort_cleanup_orphan_version.sql locked the version row in a CTE and then updated `objects`.
--     That is the opposite order and could deadlock against an overwrite of the same key; it now
--     takes the objects row first, matching the house pattern.
-- So: A TRANSACTION THAT LOCKS AN object_versions ROW MUST LOCK ITS objects ROW FIRST. Written down
-- here, in hippius_s3/sql/CLAUDE.md, and asserted by
-- test_abort_multipart_racing_an_overwrite_does_not_deadlock.
--
-- WHAT IS DELIBERATELY *NOT* LOCKED:
--   * storage_usage_versions_update_trigger. It reads `objects` to decide whether its version still
--     counts, and that read needs no lock BECAUSE of the lock above: every statement that changes
--     which version counts now holds a conflicting lock on the version row this trigger is updating,
--     so the two are already serialised, and the read is fresh. Locking `objects` from here would
--     take the two rows in the opposite order and deadlock against every concurrent overwrite --
--     the common case, not an exotic one.
--   * storage_usage_objects_delete_trigger. It is a BEFORE DELETE trigger, which runs BEFORE the
--     statement locks the objects row, so a version lock taken here would be the wrong way round.
--     The exposure is a hard delete of a LIVE object racing a finalize, and that pair is not
--     reachable: hard_delete_object.sql requires `deleted_at IS NOT NULL` (so the trigger emits
--     nothing), and the only paths that hard-delete a live object do it by deleting its BUCKET --
--     which cascades the bucket's bucket_storage_usage row away and leaves its ledger rows to be
--     discarded by the compactor's join on `buckets`. Adding two locks per row there would tax
--     every bulk purge to fix a number that is deleted in the same statement.
SET LOCAL lock_timeout = '3s';

-- As storage_usage_version_bytes, but takes FOR NO KEY UPDATE on the row it reads.
--
-- plpgsql rather than SQL: a locking clause is not allowed inside the sub-SELECT the SQL form uses,
-- and this must NOT be marked STABLE -- it takes a lock, so it is not repeatable.
--
-- The qual is re-checked after the lock is granted, so a version another transaction soft-deleted
-- while we waited correctly falls through to 0 rather than being counted at its old size.
CREATE OR REPLACE FUNCTION storage_usage_version_bytes_locked(p_object_id uuid, p_object_version bigint)
RETURNS bigint
LANGUAGE plpgsql
AS $$
DECLARE
    v_bytes bigint;
BEGIN
    SELECT ov.size_bytes INTO v_bytes
    FROM object_versions ov
    WHERE ov.object_id = p_object_id
      AND ov.object_version = p_object_version
      AND ov.deleted_at IS NULL
      AND NOT ov.is_delete_marker
    FOR NO KEY UPDATE;

    RETURN COALESCE(v_bytes, 0);
END;
$$;

-- The insert side cannot race -- upsert_object_basic creates the objects row and the version it
-- points at in the same statement, so no other transaction can have the version yet. It uses the
-- locking read anyway: locking a row this transaction just inserted is free, and it means every
-- path that turns "which version is current" into bytes does so under a lock, so a future statement
-- that points a NEW objects row at an EXISTING version inherits the guarantee instead of quietly
-- reopening this bug.
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

-- Both sides are locked, not just the outgoing one. The trigger also fires when only `deleted_at`
-- moves (soft_delete_object, and a PUT reviving a soft-deleted key), and there OLD and NEW name the
-- SAME existing version -- which a concurrent finalize can be resizing right now. That case needs
-- the lock as much as the overwrite does; when the two sides name one version the second call is a
-- no-op re-lock of a row this transaction already holds.
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
--
-- NOTE: this restores migration 20260910120000's UNLOCKED bodies, i.e. it reverts 20260910180000
-- (the outgoing-only lock) as well as this file. That is deliberate and is the safe direction --
-- the half-fixed intermediate state over-bills AND carries the lock-order constraint -- but it
-- means a single-step rollback of this file leaves 20260910180000 recorded as applied while its
-- effect is gone. Roll both back, or forward-fix instead.

SET LOCAL lock_timeout = '3s';

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

DROP FUNCTION IF EXISTS storage_usage_version_bytes_locked(uuid, bigint);
