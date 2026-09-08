-- migrate:up

-- Per-bucket storage rollup, maintained by triggers, read by the billing-plan quota gate.
--
-- WHY A ROLLUP AT ALL
-- get_admin_account_stats.sql answers "how many bytes does this account store" with a live
-- SUM(object_versions.size_bytes) across buckets -> objects -> object_versions. That query already
-- times out for 10+ TB accounts (admin.py degrades to a null count rather than 500ing), and one
-- prod account holds 11.8M objects in a single bucket. A quota gate runs on every PUT, so the
-- answer has to be O(number of buckets), not O(number of objects). No index fixes that; only a
-- maintained aggregate does.
--
-- WHY BUCKET GRANULARITY, NOT ACCOUNT
-- 1. Write contention spreads across a user's buckets instead of funnelling into one row.
-- 2. `buckets` is the only table carrying main_account_id, so the account total is a ~10-100 row
--    indexed sum -- sub-millisecond, and cheap enough to recompute on the denial path.
-- 3. Recomputation is bounded per bucket, so the reconciler never has to hold one long scan.
--
-- WHY TRIGGERS AND NOT APPLICATION CODE
-- Roughly 26 code paths move bytes. Four of them (the janitor's hard-delete ring, purge_buckets.py,
-- nuke_user.py, purge_source_versions.py) run outside the request path, and object_versions
-- cascades from objects ON DELETE CASCADE -- so a row-level trigger sees them all and application
-- wiring cannot. An application funnel has to be remembered at every future write endpoint; a
-- trigger cannot be forgotten. The cost is action-at-a-distance, which is why this file is long.
--
-- ACCOUNTING DEFINITION (must match get_admin_account_stats.sql exactly)
--   billable = object_versions.size_bytes
--     WHERE the version IS objects.current_object_version
--       AND objects.deleted_at IS NULL
--       AND object_versions.deleted_at IS NULL
--       AND NOT object_versions.is_delete_marker
-- Superseded versions are deliberately NOT billed: they linger un-unpinned today (a known gap), and
-- billing users for our own retention bug is not defensible. Version tombstones also keep their
-- size_bytes after reap_deleted_version_parts, so summing all versions would over-count anyway.
--
-- DIVISION OF LABOUR BETWEEN THE TRIGGERS -- read this before adding one
--   * The `objects` triggers own WHICH VERSION IS CURRENT (insert, repoint, soft-delete, revival,
--     hard-delete).
--   * The `object_versions` trigger owns SIZE CHANGES TO A ROW THAT IS ALREADY CURRENT.
-- There is deliberately NO trigger on object_versions INSERT or DELETE. An inserted version is
-- either not current (create_migration_version inserts ABOVE current without bumping it) or it
-- became current in the same statement, in which case the `objects` trigger already accounted for
-- the transition -- covering it twice would double-count every new object. A deleted version row is
-- either part of an `objects` cascade (already handled below) or reap_deleted_version_parts acting
-- on an already-deleted version, which was never counted.
-- tests/integration/test_usage_triggers.py pins the exact trigger set so this cannot drift.
--
-- THE COUNTER IS A CACHE, NOT THE TRUTH. recompute_bucket_storage_usage.sql restores any row from
-- ground truth at any time. Drift is therefore bounded and repairable, and the quota gate only ever
-- trusts this number to ALLOW -- a denial re-checks against the authoritative SUM.

-- A long-running reader can otherwise queue behind CREATE TRIGGER while it holds ACCESS EXCLUSIVE
-- on object_versions (~152M rows / ~79 GB) and objects, stalling the whole data plane behind it.
-- The trigger creation itself is a catalog-only write and completes in milliseconds; it is the
-- WAIT that is dangerous. Failing fast and retrying the deploy is strictly better.
--
-- SET LOCAL, not SET: dbmate applies every pending migration over ONE connection, so a plain SET
-- would survive this COMMIT and still be in force for any later CREATE INDEX CONCURRENTLY, whose
-- wait-for-older-snapshots phase is lock_timeout-sensitive and would abort into an INVALID index.
SET LOCAL lock_timeout = '3s';

CREATE TABLE IF NOT EXISTS public.bucket_storage_usage (
    bucket_id        uuid PRIMARY KEY REFERENCES public.buckets(bucket_id) ON DELETE CASCADE,
    main_account_id  text        NOT NULL,
    bytes_used       bigint      NOT NULL DEFAULT 0,
    objects_count    bigint      NOT NULL DEFAULT 0,
    updated_at       timestamptz NOT NULL DEFAULT now(),
    -- Set by the reconciler. A NULL reconciled_at means "never verified against ground truth".
    reconciled_at    timestamptz NULL,
    reconciled_bytes bigint      NULL
);

-- The account total is the only hot read: SUM(bytes_used) for one main_account_id. INCLUDE keeps it
-- index-only. Built non-concurrently because the table is empty at this point in the migration.
CREATE INDEX IF NOT EXISTS idx_bucket_storage_usage_account
    ON public.bucket_storage_usage (main_account_id) INCLUDE (bytes_used);

-- Drives the reconciler's "least recently verified first" sweep.
CREATE INDEX IF NOT EXISTS idx_bucket_storage_usage_reconciled
    ON public.bucket_storage_usage (reconciled_at NULLS FIRST);


-- Billable bytes of ONE version row. IMMUTABLE so it can be inlined into the trigger bodies and the
-- recompute query, keeping the definition in exactly one place.
CREATE OR REPLACE FUNCTION public.usage_billable(
    p_size bigint,
    p_deleted_at timestamptz,
    p_is_delete_marker boolean
) RETURNS bigint
LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
    SELECT CASE
        WHEN p_deleted_at IS NULL AND NOT COALESCE(p_is_delete_marker, false)
        THEN COALESCE(p_size, 0)
        ELSE 0
    END
$$;


-- Billable bytes of a specific (object, version), or 0 when the row does not exist.
CREATE OR REPLACE FUNCTION public.usage_version_bytes(p_object_id uuid, p_version bigint)
RETURNS bigint
LANGUAGE sql STABLE AS $$
    SELECT COALESCE((
        SELECT public.usage_billable(ov.size_bytes, ov.deleted_at, ov.is_delete_marker)
        FROM public.object_versions ov
        WHERE ov.object_id = p_object_id
          AND ov.object_version = p_version
    ), 0)
$$;


-- The ONLY writer of bucket_storage_usage. Every trigger routes through here.
--
-- This function must be TOTAL: it runs inside the caller's transaction, so anything that raises
-- here fails a user's PUT. A missing `buckets` row therefore inserts nothing rather than erroring --
-- INSERT..SELECT yields zero rows and the statement is a no-op.
--
-- GREATEST(0, ...) clamps both counters, on BOTH arms. A counter driven negative must never read as
-- "this account stores negative bytes": get_account_storage_usage.sql SUMs across an account's
-- buckets, so one negative row would subtract from the account total and inflate its headroom.
--
-- The INSERT arm matters as much as the UPDATE arm, and is easy to miss. The triggers are created
-- before the backfill runs, so on deploy EVERY pre-existing bucket has no rollup row yet — the
-- first delete or overwrite on such a bucket arrives here as a negative delta with nothing to
-- conflict against, and an unclamped INSERT would store it verbatim.
--
-- The DO UPDATE arm deliberately adds p_bytes/p_objects rather than EXCLUDED.*: EXCLUDED carries
-- the INSERT arm's already-clamped values, so using it here would floor every decrement at 0 and
-- the counter could only ever grow.
CREATE OR REPLACE FUNCTION public.usage_apply(
    p_bucket_id uuid,
    p_bytes bigint,
    p_objects bigint
) RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
    IF p_bucket_id IS NULL OR (COALESCE(p_bytes, 0) = 0 AND COALESCE(p_objects, 0) = 0) THEN
        RETURN;
    END IF;

    INSERT INTO public.bucket_storage_usage AS bsu
        (bucket_id, main_account_id, bytes_used, objects_count)
    SELECT p_bucket_id,
           b.main_account_id,
           GREATEST(0, COALESCE(p_bytes, 0)),
           GREATEST(0, COALESCE(p_objects, 0))
    FROM public.buckets b
    WHERE b.bucket_id = p_bucket_id
    ON CONFLICT (bucket_id) DO UPDATE
        SET bytes_used    = GREATEST(0, bsu.bytes_used    + COALESCE(p_bytes, 0)),
            objects_count = GREATEST(0, bsu.objects_count + COALESCE(p_objects, 0)),
            updated_at    = now();
END $$;


-- TRIGGER 1/4 -- object_versions AFTER UPDATE.
-- Owns size changes to a row that is ALREADY the current version of a live object.
--
-- The guard is load-bearing in both directions: it rejects updates to superseded versions (never
-- counted) and to versions of a soft-deleted object (already decremented by trigger 2), so a
-- version being reaped or rewritten out of band cannot move the counter.
--
-- Covers, with no application changes: the simple-PUT finalize (update_object_version_metadata),
-- MPU complete, the S4 append's `SET size_bytes = size_bytes + $3`, and the versioned-DELETE
-- soft-delete of the current version.
CREATE OR REPLACE FUNCTION public.usage_tg_object_versions_update()
RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    v_bucket_id uuid;
    v_delta     bigint;
BEGIN
    v_delta := public.usage_billable(NEW.size_bytes, NEW.deleted_at, NEW.is_delete_marker)
             - public.usage_billable(OLD.size_bytes, OLD.deleted_at, OLD.is_delete_marker);

    IF v_delta = 0 THEN
        RETURN NULL;
    END IF;

    SELECT o.bucket_id INTO v_bucket_id
    FROM public.objects o
    WHERE o.object_id = NEW.object_id
      AND o.current_object_version = NEW.object_version
      AND o.deleted_at IS NULL;

    IF v_bucket_id IS NULL THEN
        RETURN NULL;
    END IF;

    PERFORM public.usage_apply(v_bucket_id, v_delta, 0);
    RETURN NULL;
END $$;


-- TRIGGER 2/4 -- objects AFTER UPDATE OF current_object_version, deleted_at.
-- Owns every change of WHICH version is current, plus soft-delete and revival.
--
-- old_bytes reads the OLD current version's CURRENT state, which is correct precisely because no
-- statement in this codebase both rewrites the outgoing current version AND repoints in one go:
--   * upsert_object_basic bumps current_object_version and inserts the new version at size 0 in one
--     statement, leaving the outgoing version untouched -> delta = 0 - old = -old, and the finalize
--     UPDATE then adds +new via trigger 1. Net = new - old.
--   * a versioned DELETE soft-deletes the version FIRST (trigger 1 charges -old), then repoints;
--     by then usage_version_bytes of the outgoing version is already 0, so this trigger adds only
--     +successor rather than double-subtracting.
--
-- AFTER ROW triggers are queued and fired at end-of-statement, after CommandCounterIncrement, so
-- the version row that upsert_object_basic's `ins_version` CTE created IS visible here. That is the
-- one Postgres-semantics assumption this design rests on; it is pinned by
-- tests/integration/test_usage_triggers.py::test_reserve_and_finalize_is_a_single_net_delta.
CREATE OR REPLACE FUNCTION public.usage_tg_objects_update()
RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    v_old_bytes   bigint;
    v_new_bytes   bigint;
    v_obj_delta   bigint;
BEGIN
    v_old_bytes := CASE WHEN OLD.deleted_at IS NULL
        THEN public.usage_version_bytes(OLD.object_id, OLD.current_object_version) ELSE 0 END;
    v_new_bytes := CASE WHEN NEW.deleted_at IS NULL
        THEN public.usage_version_bytes(NEW.object_id, NEW.current_object_version) ELSE 0 END;

    v_obj_delta := (CASE WHEN NEW.deleted_at IS NULL THEN 1 ELSE 0 END)
                 - (CASE WHEN OLD.deleted_at IS NULL THEN 1 ELSE 0 END);

    PERFORM public.usage_apply(NEW.bucket_id, v_new_bytes - v_old_bytes, v_obj_delta);
    RETURN NULL;
END $$;


-- TRIGGER 3/4 -- objects AFTER INSERT. A brand-new key.
-- The current version is normally a reserve row at size 0, so this usually contributes bytes 0 and
-- objects +1; the finalize UPDATE adds the bytes through trigger 1. Paths that insert a version at
-- its final size in the same statement (the v5 copy fast path) are picked up here directly.
CREATE OR REPLACE FUNCTION public.usage_tg_objects_insert()
RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.deleted_at IS NOT NULL THEN
        RETURN NULL;
    END IF;
    PERFORM public.usage_apply(
        NEW.bucket_id,
        public.usage_version_bytes(NEW.object_id, NEW.current_object_version),
        1
    );
    RETURN NULL;
END $$;


-- TRIGGER 4/4 -- objects BEFORE DELETE. Hard-delete, and every out-of-band script.
--
-- BEFORE, not AFTER: object_versions cascades from this row, so the child rows must still be
-- readable when we price them.
--
-- The `deleted_at IS NULL` guard is what makes the janitor a no-op. hard_delete_object only ever
-- removes rows that were soft-deleted at least an hour ago, and trigger 2 already zeroed those --
-- charging again here would double-decrement. A script that deletes a LIVE object (nuke_user.py,
-- purge_buckets.py) has not been accounted for anywhere else, and is charged correctly.
CREATE OR REPLACE FUNCTION public.usage_tg_objects_delete()
RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.deleted_at IS NULL THEN
        PERFORM public.usage_apply(
            OLD.bucket_id,
            -public.usage_version_bytes(OLD.object_id, OLD.current_object_version),
            -1
        );
    END IF;
    RETURN OLD;
END $$;


DROP TRIGGER IF EXISTS trg_usage_object_versions_update ON public.object_versions;
CREATE TRIGGER trg_usage_object_versions_update
    AFTER UPDATE OF size_bytes, deleted_at, is_delete_marker ON public.object_versions
    FOR EACH ROW EXECUTE FUNCTION public.usage_tg_object_versions_update();

DROP TRIGGER IF EXISTS trg_usage_objects_update ON public.objects;
CREATE TRIGGER trg_usage_objects_update
    AFTER UPDATE OF current_object_version, deleted_at ON public.objects
    FOR EACH ROW EXECUTE FUNCTION public.usage_tg_objects_update();

DROP TRIGGER IF EXISTS trg_usage_objects_insert ON public.objects;
CREATE TRIGGER trg_usage_objects_insert
    AFTER INSERT ON public.objects
    FOR EACH ROW EXECUTE FUNCTION public.usage_tg_objects_insert();

DROP TRIGGER IF EXISTS trg_usage_objects_delete ON public.objects;
CREATE TRIGGER trg_usage_objects_delete
    BEFORE DELETE ON public.objects
    FOR EACH ROW EXECUTE FUNCTION public.usage_tg_objects_delete();

-- NOTE ON BACKFILL: the triggers are created BEFORE the backfill runs (scripts/backfill is a
-- separate operational step, not part of this migration) so no write can land in the gap. The
-- backfill SETs each row from ground truth rather than adding to it, so a bucket written while the
-- backfill is in flight converges instead of double-counting.
--
-- NOTE ON FUTURE BULK MIGRATIONS: anything that rewrites object_versions or objects in bulk will
-- fire these triggers once per row. Such a migration must ALTER TABLE ... DISABLE TRIGGER USER,
-- do its work, re-enable, and then recompute the affected buckets.

-- migrate:down

DROP TRIGGER IF EXISTS trg_usage_objects_delete ON public.objects;
DROP TRIGGER IF EXISTS trg_usage_objects_insert ON public.objects;
DROP TRIGGER IF EXISTS trg_usage_objects_update ON public.objects;
DROP TRIGGER IF EXISTS trg_usage_object_versions_update ON public.object_versions;

DROP FUNCTION IF EXISTS public.usage_tg_objects_delete();
DROP FUNCTION IF EXISTS public.usage_tg_objects_insert();
DROP FUNCTION IF EXISTS public.usage_tg_objects_update();
DROP FUNCTION IF EXISTS public.usage_tg_object_versions_update();
DROP FUNCTION IF EXISTS public.usage_apply(uuid, bigint, bigint);
DROP FUNCTION IF EXISTS public.usage_version_bytes(uuid, bigint);
DROP FUNCTION IF EXISTS public.usage_billable(bigint, timestamptz, boolean);

DROP TABLE IF EXISTS public.bucket_storage_usage;
