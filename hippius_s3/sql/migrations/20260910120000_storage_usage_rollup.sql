-- migrate:up

-- A MAINTAINED per-bucket storage counter, so "how many bytes does this account store" stops being
-- an O(objects) scan.
--
-- WHY. get_account_storage_bytes.sql is the canonical definition of billable storage and it is
-- O(objects the account owns). One prod plan account owns a single bucket holding millions of live
-- objects, where that aggregate takes ~64s; the plans-cacher's chunked walk got it under the 30s
-- per-statement ceilings but still spends minutes of replica work every 10 minutes recomputing a
-- number that moved by a handful of objects. That cost only grows with the bucket. Every other
-- object store maintains this number incrementally instead (Ceph RGW caches per-bucket stats,
-- Swift updates container DBs asynchronously); this is that.
--
-- THE SHAPE. Triggers INSERT into an append-only ledger; a background compactor folds the ledger
-- into a per-bucket rollup; the account total is an indexed SUM over the rollup.
--
--   objects / object_versions  --row triggers, INSERT only-->  storage_delta_ledger
--                                                                       |
--                                 compactor: DELETE ... RETURNING,      v
--                                 fold, add                      bucket_storage_usage
--                                                                       |
--                                       account total = indexed SUM  <--+
--
-- INSERT, NOT `UPDATE ... SET bytes = bytes + delta`. That distinction is the whole point. A hot
-- counter row serialises every writer touching one bucket on a single row lock and leaves one dead
-- tuple per write for vacuum to chase — on the multi-million-object bucket that motivated this,
-- that is the entire write path funnelled through one page. An append-only INSERT has neither
-- property. The cost is that the number is eventually consistent by however far the compactor
-- lags, which is acceptable: the only consumer is the plans-cacher, whose figure is already a
-- 10-minute-old estimate by design.
--
-- WHY TRIGGERS RATHER THAN APPLICATION CALL SITES. There are ~26 statements that move bytes, and
-- three of the paths that move the most (nuke_user.py, purge_buckets.py, the janitor's
-- hard_delete_object) move them through `ON DELETE CASCADE` — no SQL in those paths names
-- object_versions at all. A helper the application must remember to call cannot see a cascade, and
-- would be forgotten by the 27th write path. Triggers are action-at-a-distance and cut against the
-- house style, which is why they are documented here, in hippius_s3/sql/CLAUDE.md, and in
-- workers/CLAUDE.md rather than only in the schema.
--
-- ORDERING WITH THE BACKFILL. The triggers are created HERE, before any backfill, so no write can
-- slip between "counter exists" and "counter is seeded". Until
-- storage_usage_rollup_state.backfilled_at is set, the rollup holds only deltas-since-migration and
-- is NOT a total; usage_service refuses to serve it and the plans-cacher keeps its previous roll.
-- Seed it with hippius_s3/scripts/backfill_bucket_storage_usage.py (k8s Job manifest at
-- k8s/backfill-bucket-storage-usage-job.yaml).
--
-- LOCKING. `CREATE TRIGGER` takes ACCESS EXCLUSIVE on the table. It is metadata-only (no rewrite,
-- so table size is irrelevant), but a long-running reader can make it WAIT while holding that
-- lock, which stalls the whole data plane behind it. SET LOCAL lock_timeout fails the migration
-- fast instead; the whole migration is one transaction so a timeout leaves nothing partial behind,
-- and the db-migrations Job's backoffLimit is the retry. SET LOCAL rather than SET because dbmate
-- applies every pending migration over ONE connection and a session-scoped SET would leak into the
-- next migration.
SET LOCAL lock_timeout = '3s';

-- The ledger. Insert-only, drained by the compactor, so steady-state size is one compaction batch.
--
-- No FK on bucket_id, deliberately: this table is written from inside a trigger on the user's write
-- path, so it must never be able to raise. A `DELETE FROM buckets` cascade emits deltas for a
-- bucket row that is already gone; the compactor discards those by joining `buckets`.
CREATE TABLE IF NOT EXISTS storage_delta_ledger (
    ledger_id   bigserial PRIMARY KEY,
    bucket_id   uuid NOT NULL,
    delta_bytes bigint NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now()
);

-- Drives recompute_bucket_storage_usage's per-bucket drain. Without it, each recompute (and so each
-- of the ~46k backfill steps) scans the whole ledger.
CREATE INDEX IF NOT EXISTS idx_storage_delta_ledger_bucket ON storage_delta_ledger (bucket_id);

-- The rollup. One row per bucket, independent of whether the bucket is soft-deleted: bucket
-- liveness and ownership are applied at READ time by joining `buckets`, so a bucket being
-- soft-deleted, revived, or transferred needs no trigger and no rewrite here.
--
-- bytes_used is NOT clamped and CAN go negative. Negative means the ledger has drifted, and a
-- billing number that is visibly wrong is worth far more than one silently floored at zero — a
-- clamp on the counter would also permanently bias it high, because every later increment would
-- start from 0 instead of from the true (negative) value. The read path clamps for its own
-- consumers and reports the anomaly; the reconciler repairs it.
CREATE TABLE IF NOT EXISTS bucket_storage_usage (
    bucket_id     uuid PRIMARY KEY REFERENCES buckets(bucket_id) ON DELETE CASCADE,
    bytes_used    bigint NOT NULL,
    updated_at    timestamptz NOT NULL DEFAULT now(),
    -- Last full recompute. NULLS FIRST on this column is the reconciler's work queue.
    recomputed_at timestamptz NULL
);

-- One row, ever. `backfilled_at IS NULL` is the only thing standing between a half-seeded rollup
-- and a wrong number on a customer's bill, so it is a row in the schema rather than a config flag
-- someone can set by hand on the wrong environment.
CREATE TABLE IF NOT EXISTS storage_usage_rollup_state (
    singleton     boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    backfilled_at timestamptz NULL
);
INSERT INTO storage_usage_rollup_state (singleton) VALUES (true) ON CONFLICT DO NOTHING;

-- ---------------------------------------------------------------------------------------------
-- Contribution helpers. Between them these two functions ARE get_account_storage_bytes.sql's
-- predicate, split by which table owns each half of it. Change one and you have changed what
-- customers are billed; tests/integration/test_storage_usage_rollup.py asserts the rollup against
-- that query after every write path.
-- ---------------------------------------------------------------------------------------------

-- Bytes a version contributes GIVEN that it is the current version of a live object. 0 when the row
-- is absent, soft-deleted, or a delete marker.
CREATE OR REPLACE FUNCTION storage_usage_version_bytes(p_object_id uuid, p_object_version bigint)
RETURNS bigint
LANGUAGE sql
STABLE
AS $$
    SELECT COALESCE((
        SELECT ov.size_bytes
        FROM object_versions ov
        WHERE ov.object_id = p_object_id
          AND ov.object_version = p_object_version
          AND ov.deleted_at IS NULL
          AND NOT ov.is_delete_marker
    ), 0)::bigint
$$;

-- Whether this version is the current version of a live object, i.e. whether its bytes count.
CREATE OR REPLACE FUNCTION storage_usage_version_is_current(p_object_id uuid, p_object_version bigint)
RETURNS boolean
LANGUAGE sql
STABLE
AS $$
    SELECT EXISTS (
        SELECT 1
        FROM objects o
        WHERE o.object_id = p_object_id
          AND o.current_object_version = p_object_version
          AND o.deleted_at IS NULL
    )
$$;

CREATE OR REPLACE FUNCTION storage_usage_bucket_of_object(p_object_id uuid)
RETURNS uuid
LANGUAGE sql
STABLE
AS $$
    SELECT o.bucket_id FROM objects o WHERE o.object_id = p_object_id
$$;

-- ---------------------------------------------------------------------------------------------
-- Emission. Every trigger funnels through here so the "never raise, never write a no-op" rules
-- live in exactly one place.
-- ---------------------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION storage_usage_emit(p_bucket_id uuid, p_delta bigint)
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    -- TOTAL BY CONSTRUCTION. A NULL bucket means the objects row has already gone (a cascade), and
    -- a zero delta means nothing moved; both must write nothing rather than raise, because a raise
    -- here aborts a customer's PUT. There is no other way for this INSERT to fail: the table has no
    -- FK, no CHECK and no unique constraint beyond its own generated key.
    IF p_bucket_id IS NULL OR p_delta IS NULL OR p_delta = 0 THEN
        RETURN;
    END IF;

    INSERT INTO storage_delta_ledger (bucket_id, delta_bytes) VALUES (p_bucket_id, p_delta);
END;
$$;

-- Two-sided move. Netted into one ledger row when the bucket does not change (which is always in
-- practice: no object ever moves bucket), split when it does.
CREATE OR REPLACE FUNCTION storage_usage_apply(
    p_old_bucket uuid,
    p_old_bytes bigint,
    p_new_bucket uuid,
    p_new_bytes bigint
)
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    IF p_old_bucket IS NOT DISTINCT FROM p_new_bucket THEN
        PERFORM storage_usage_emit(p_new_bucket, p_new_bytes - p_old_bytes);
    ELSE
        PERFORM storage_usage_emit(p_old_bucket, -p_old_bytes);
        PERFORM storage_usage_emit(p_new_bucket, p_new_bytes);
    END IF;
END;
$$;

-- ---------------------------------------------------------------------------------------------
-- Trigger functions.
--
-- THE SPLIT: `objects` owns "which version counts" (current_object_version, deleted_at, bucket_id)
-- and `object_versions` owns "how many bytes that version is" (size_bytes, deleted_at,
-- is_delete_marker). Each side reads the other's current state to turn its own OLD/NEW into bytes.
--
-- That works because no single statement in this codebase both repoints objects.current_object_
-- version AND modifies an EXISTING object_versions row for the same object. The four statements
-- that touch both tables at once — upsert_object_basic, upsert_object_with_cid,
-- upsert_object_multipart, insert_delete_marker — all INSERT a brand-new version and point at it,
-- and there is deliberately no INSERT trigger on object_versions (see below), so exactly one side
-- speaks for each fact. If you ever write a statement that repoints current_object_version and
-- soft-deletes or resizes the outgoing version in the same statement, this decomposition breaks:
-- AFTER ROW triggers all fire at end-of-statement and would each see the other's finished work.
-- Use two statements, as soft_delete_object_version + repoint_current_version_after_delete do.
-- ---------------------------------------------------------------------------------------------

-- AFTER, not BEFORE: upsert_object_basic inserts the objects row and its first object_versions row
-- in ONE statement, and only an AFTER ROW trigger — which fires at end-of-statement, after every
-- data-modifying CTE has run — can see that version row. Pinned by
-- test_reserve_in_one_statement_is_seen_by_the_trigger.
--
-- COROLLARY, and the one real constraint this design imposes on future code: an objects row and its
-- first object_versions row MUST be created by the same statement. Split across two statements the
-- objects INSERT trigger finds no version yet and there is no version INSERT trigger to catch up,
-- so the object is under-counted until the reconciler recomputes its bucket. Every production path
-- does it in one statement; some Rust test fixtures do not, which is why this is written down.
CREATE OR REPLACE FUNCTION storage_usage_objects_insert_trigger()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.deleted_at IS NULL THEN
        PERFORM storage_usage_emit(
            NEW.bucket_id,
            storage_usage_version_bytes(NEW.object_id, NEW.current_object_version)
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

-- BEFORE, unlike its INSERT and UPDATE siblings, and this is load-bearing.
--
-- object_versions.object_id REFERENCES objects ON DELETE CASCADE, and a cascade is implemented as
-- an AFTER DELETE trigger named RI_ConstraintTrigger_*. AFTER ROW triggers fire in alphabetical
-- order by trigger name, so 'RI_...' beats any lowercase name: by the time an AFTER DELETE trigger
-- of ours ran, the version rows would already be gone, every lookup would return 0, and a hard
-- delete of a LIVE object (a `DELETE FROM buckets` cascade, purge_buckets.py, nuke_user.py) would
-- silently never decrement — bytes billed forever with no row left to explain them. BEFORE ROW
-- triggers are guaranteed to run before the row is deleted and therefore before any cascade.
--
-- MUST RETURN OLD. Returning NULL from a BEFORE DELETE trigger CANCELS the delete.
CREATE OR REPLACE FUNCTION storage_usage_objects_delete_trigger()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF OLD.deleted_at IS NULL THEN
        PERFORM storage_usage_emit(
            OLD.bucket_id,
            -storage_usage_version_bytes(OLD.object_id, OLD.current_object_version)
        );
    END IF;
    RETURN OLD;
END;
$$;

CREATE OR REPLACE FUNCTION storage_usage_versions_update_trigger()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    v_old bigint := 0;
    v_new bigint := 0;
BEGIN
    IF OLD.deleted_at IS NULL
       AND NOT OLD.is_delete_marker
       AND storage_usage_version_is_current(OLD.object_id, OLD.object_version) THEN
        v_old := OLD.size_bytes;
    END IF;
    IF NEW.deleted_at IS NULL
       AND NOT NEW.is_delete_marker
       AND storage_usage_version_is_current(NEW.object_id, NEW.object_version) THEN
        v_new := NEW.size_bytes;
    END IF;

    PERFORM storage_usage_apply(
        storage_usage_bucket_of_object(OLD.object_id), v_old,
        storage_usage_bucket_of_object(NEW.object_id), v_new
    );
    RETURN NULL;
END;
$$;

-- AFTER is correct here even under a cascade, and needs no ordering guarantee: when these rows go
-- because their objects row went, the objects row is already deleted, so
-- storage_usage_version_is_current is false, the contribution is 0, and nothing is emitted. The
-- decrement for that case came from the BEFORE DELETE trigger on objects. Direct version deletes
-- (the janitor's reap, the ops scripts) still see their parent and are counted normally.
CREATE OR REPLACE FUNCTION storage_usage_versions_delete_trigger()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF OLD.deleted_at IS NULL
       AND NOT OLD.is_delete_marker
       AND storage_usage_version_is_current(OLD.object_id, OLD.object_version) THEN
        PERFORM storage_usage_emit(storage_usage_bucket_of_object(OLD.object_id), -OLD.size_bytes);
    END IF;
    RETURN NULL;
END;
$$;

-- ---------------------------------------------------------------------------------------------
-- The triggers. EXACTLY these five, and the set is pinned by
-- tests/integration/test_storage_usage_rollup.py::test_trigger_set_is_exactly_as_designed.
--
-- THERE IS NO INSERT TRIGGER ON object_versions, AND ADDING THE "OBVIOUSLY MISSING ONE" IS A BUG.
-- upsert_object_basic is a single statement whose CTEs both upsert `objects` and insert
-- `object_versions`; the objects trigger already counts the new version, so a version INSERT
-- trigger would count it a second time. A version that is inserted WITHOUT becoming current
-- (create_migration_version) contributes nothing and needs no trigger; a version that becomes
-- current is always accompanied by an objects INSERT or an objects UPDATE that moves
-- current_object_version, which is what the objects triggers fire on.
--
-- The WHEN clauses are not an optimisation detail: without them every envelope write, address
-- write, lock write, tagging write and status write on object_versions — the hottest UPDATEs in
-- the schema — would run two correlated subqueries to discover that nothing moved.
-- ---------------------------------------------------------------------------------------------

DROP TRIGGER IF EXISTS objects_storage_delta_ins ON objects;
CREATE TRIGGER objects_storage_delta_ins
    AFTER INSERT ON objects
    FOR EACH ROW
    EXECUTE FUNCTION storage_usage_objects_insert_trigger();

DROP TRIGGER IF EXISTS objects_storage_delta_upd ON objects;
CREATE TRIGGER objects_storage_delta_upd
    AFTER UPDATE ON objects
    FOR EACH ROW
    WHEN (
        OLD.current_object_version IS DISTINCT FROM NEW.current_object_version
        OR OLD.deleted_at IS DISTINCT FROM NEW.deleted_at
        OR OLD.bucket_id IS DISTINCT FROM NEW.bucket_id
    )
    EXECUTE FUNCTION storage_usage_objects_update_trigger();

DROP TRIGGER IF EXISTS objects_storage_delta_del ON objects;
CREATE TRIGGER objects_storage_delta_del
    BEFORE DELETE ON objects
    FOR EACH ROW
    EXECUTE FUNCTION storage_usage_objects_delete_trigger();

DROP TRIGGER IF EXISTS object_versions_storage_delta_upd ON object_versions;
CREATE TRIGGER object_versions_storage_delta_upd
    AFTER UPDATE ON object_versions
    FOR EACH ROW
    WHEN (
        OLD.size_bytes IS DISTINCT FROM NEW.size_bytes
        OR OLD.deleted_at IS DISTINCT FROM NEW.deleted_at
        OR OLD.is_delete_marker IS DISTINCT FROM NEW.is_delete_marker
    )
    EXECUTE FUNCTION storage_usage_versions_update_trigger();

DROP TRIGGER IF EXISTS object_versions_storage_delta_del ON object_versions;
CREATE TRIGGER object_versions_storage_delta_del
    AFTER DELETE ON object_versions
    FOR EACH ROW
    EXECUTE FUNCTION storage_usage_versions_delete_trigger();

-- ---------------------------------------------------------------------------------------------
-- Recompute. Used by the backfill (every bucket, once) and by the reconciler (a rolling slice,
-- forever). SETS the rollup rather than adding to it, so it converges instead of double-counting.
-- ---------------------------------------------------------------------------------------------

-- Serialises a recompute against the compactor. A recompute reads the world and overwrites the
-- counter; a compactor deletes ledger rows and adds them to the counter. Interleaved, the
-- recompute's overwrite can discard a delta the compactor has already consumed, permanently, and
-- silently — which would leave the reconciler's drift metric non-zero forever and destroy the one
-- signal that says the ledger is wrong. The compactor takes this with pg_TRY_advisory_xact_lock and
-- skips the cycle if a recompute holds it; the recompute waits.
--
-- A single global lock rather than one per bucket: it is one lock to reason about and cannot
-- deadlock, and a recompute holds it only for its own statement. The backfill runs one bucket per
-- transaction precisely so compaction interleaves between buckets rather than stalling for hours.
CREATE OR REPLACE FUNCTION storage_usage_rollup_lock_key()
RETURNS integer
LANGUAGE sql
IMMUTABLE
AS $$
    SELECT hashtext('hippius.storage_usage_rollup')
$$;

CREATE OR REPLACE FUNCTION recompute_bucket_storage_usage(
    p_bucket_id uuid,
    OUT o_bytes_before bigint,
    OUT o_bytes_after bigint
)
LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM pg_advisory_xact_lock(storage_usage_rollup_lock_key(), 0);

    SELECT bsu.bytes_used INTO o_bytes_before
    FROM bucket_storage_usage bsu
    WHERE bsu.bucket_id = p_bucket_id;

    -- ONE STATEMENT, therefore ONE SNAPSHOT, and that is what makes this safe against concurrent
    -- writers. A ledger row visible to this snapshot is dropped here AND its effect is in the
    -- aggregate below, so it is counted exactly once. A ledger row committed after this snapshot is
    -- invisible to the DELETE, survives, and is NOT in the aggregate, so it is folded in later —
    -- also exactly once. Split into two statements (READ COMMITTED gives each its own snapshot) a
    -- write landing in between would be counted twice.
    WITH superseded AS (
        DELETE FROM storage_delta_ledger WHERE bucket_id = p_bucket_id
    ), truth AS (
        SELECT COALESCE(SUM(ov.size_bytes), 0)::bigint AS bytes
        FROM objects o
        JOIN object_versions ov
          ON ov.object_id = o.object_id
         AND ov.object_version = o.current_object_version
         AND ov.deleted_at IS NULL
         AND NOT ov.is_delete_marker
        WHERE o.bucket_id = p_bucket_id
          AND o.deleted_at IS NULL
    )
    INSERT INTO bucket_storage_usage AS bsu (bucket_id, bytes_used, updated_at, recomputed_at)
    SELECT p_bucket_id, t.bytes, now(), now()
    FROM truth t
    -- A bucket that no longer exists gets no row: the FK would raise, and the answer is 0 anyway.
    WHERE EXISTS (SELECT 1 FROM buckets b WHERE b.bucket_id = p_bucket_id)
    ON CONFLICT (bucket_id) DO UPDATE
       SET bytes_used = EXCLUDED.bytes_used,
           updated_at = now(),
           recomputed_at = now()
    RETURNING bsu.bytes_used INTO o_bytes_after;

    o_bytes_before := COALESCE(o_bytes_before, 0);
    o_bytes_after := COALESCE(o_bytes_after, 0);
END;
$$;

-- migrate:down

SET LOCAL lock_timeout = '3s';

DROP TRIGGER IF EXISTS object_versions_storage_delta_del ON object_versions;
DROP TRIGGER IF EXISTS object_versions_storage_delta_upd ON object_versions;
DROP TRIGGER IF EXISTS objects_storage_delta_del ON objects;
DROP TRIGGER IF EXISTS objects_storage_delta_upd ON objects;
DROP TRIGGER IF EXISTS objects_storage_delta_ins ON objects;

DROP FUNCTION IF EXISTS storage_usage_versions_delete_trigger();
DROP FUNCTION IF EXISTS storage_usage_versions_update_trigger();
DROP FUNCTION IF EXISTS storage_usage_objects_delete_trigger();
DROP FUNCTION IF EXISTS storage_usage_objects_update_trigger();
DROP FUNCTION IF EXISTS storage_usage_objects_insert_trigger();
DROP FUNCTION IF EXISTS recompute_bucket_storage_usage(uuid);
DROP FUNCTION IF EXISTS storage_usage_apply(uuid, bigint, uuid, bigint);
DROP FUNCTION IF EXISTS storage_usage_emit(uuid, bigint);
DROP FUNCTION IF EXISTS storage_usage_bucket_of_object(uuid);
DROP FUNCTION IF EXISTS storage_usage_version_is_current(uuid, bigint);
DROP FUNCTION IF EXISTS storage_usage_version_bytes(uuid, bigint);
DROP FUNCTION IF EXISTS storage_usage_rollup_lock_key();

DROP TABLE IF EXISTS storage_usage_rollup_state;
DROP TABLE IF EXISTS bucket_storage_usage;
DROP TABLE IF EXISTS storage_delta_ledger;
