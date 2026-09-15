-- Make the rollup's VERIFIER survive a bucket it can never aggregate.
--
-- The maintained path is O(delta) and fine at any size. The verifier recomputes from scratch, which
-- is O(bucket) -- and prod's largest bucket holds 136,221,023 of ~168,697,344 objects, 80.7% of the
-- table. At that selectivity the planner correctly picks a parallel seq scan over `objects` (167 GB)
-- hash-joined to `object_versions` (92 GB), cost 15,032,678. No index and no timeout fixes that; the
-- aggregate is simply not completable inside a reconcile cycle.
--
-- Three things went wrong at once because of it, all measured on prod 2026-09-15:
--
--   1. The recompute times out, so its UPSERT never commits, so `recomputed_at` is never stamped.
--      The work queue is `ORDER BY recomputed_at ASC NULLS FIRST`, so the bucket stays at the HEAD
--      FOREVER: 168 failed attempts in 24h, i.e. every single cycle.
--   2. Each attempt held the GLOBAL advisory lock for the full reconcile timeout. compact_once uses
--      pg_try_advisory_xact_lock on the same key, so every compaction attempt returned 0 and the
--      ledger grew unfolded -- oldest row aged to 412s against a normal 2s.
--   3. Two such buckets x a 300s timeout against a 300s interval meant the lock was held more than
--      the whole interval, continuously, and the other ~3,355 buckets lost verification slots (the
--      reconciler completed 47 of 50 per pass).
--
-- So: `attempted_at` separates "we tried" from "we succeeded" and becomes the queue key, a per-bucket
-- lock key stops one bucket's verification blocking another's compaction, and `churn_bytes` gives a
-- rigorous tolerance bound so an oversized bucket can be verified in indexed key-range slices across
-- many cycles instead of one impossible aggregate. A slice is ~12,000x cheaper: cost 1,212 against
-- 15,032,678, index scan both sides.

-- migrate:up

SET LOCAL lock_timeout = '3s';

-- ATTEMPTED vs RECOMPUTED. The queue orders on `attempted_at` so a bucket that cannot be aggregated
-- rotates out of the head after one try instead of pinning it; `recomputed_at` keeps its old meaning
-- (last SUCCESSFUL verification) and is what tells you a bucket is actually being checked. Reading
-- `recomputed_at IS DISTINCT FROM attempted_at` is how you find buckets whose last attempt failed.
ALTER TABLE bucket_storage_usage
    ADD COLUMN IF NOT EXISTS attempted_at timestamptz NULL;

-- Consecutive failed recomputes. This is the ROUTING signal for sliced verification: rather than
-- hardcode an object-count threshold that would need retuning as the estate grows, a bucket earns
-- the slow path by actually failing the fast one. Reset to 0 on success.
ALTER TABLE bucket_storage_usage
    ADD COLUMN IF NOT EXISTS recompute_failures integer NOT NULL DEFAULT 0;

-- MONOTONIC ABSOLUTE CHURN, accumulated by the compactor as SUM(ABS(delta_bytes)).
--
-- This exists to make sliced verification rigorous rather than approximate. A slice sweep reads the
-- bucket across many cycles, so its total is smeared over a window during which writes land; naively
-- comparing that total to the counter would report drift on every busy bucket. Churn bounds it: the
-- true total cannot have moved by more than the absolute delta that passed through the ledger while
-- the sweep ran, so |slice_total - counter| > (churn_end - churn_start) is a ONE-SIDED test that
-- cannot false-positive on ordinary traffic.
--
-- ABS, not the signed sum: +1 GB followed by -1 GB nets to zero but genuinely moved the number twice,
-- and a signed bound would admit exactly the drift this is meant to catch.
ALTER TABLE bucket_storage_usage
    ADD COLUMN IF NOT EXISTS churn_bytes bigint NOT NULL DEFAULT 0;

COMMENT ON COLUMN bucket_storage_usage.attempted_at IS
    'Last reconcile ATTEMPT, success or failure. The reconciler queue key.';
COMMENT ON COLUMN bucket_storage_usage.recomputed_at IS
    'Last SUCCESSFUL full recompute. Lagging attempted_at means the last attempt failed.';
COMMENT ON COLUMN bucket_storage_usage.recompute_failures IS
    'Consecutive failed recomputes. At/above the configured threshold the bucket is verified in slices.';
COMMENT ON COLUMN bucket_storage_usage.churn_bytes IS
    'Monotonic SUM(ABS(delta_bytes)) folded by the compactor. Bounds in-flight movement during a slice sweep.';

-- The reconciler's queue index. Without it the ORDER BY is a sort of every live bucket every cycle.
CREATE INDEX IF NOT EXISTS idx_bucket_storage_usage_attempted
    ON bucket_storage_usage (attempted_at ASC NULLS FIRST);

-- Sweep state for sliced verification. One row per bucket currently mid-sweep; deleted when the
-- sweep completes, so the table stays at roughly the number of oversized buckets (2 on prod).
CREATE TABLE IF NOT EXISTS bucket_storage_verify_state (
    bucket_id         uuid PRIMARY KEY REFERENCES buckets(bucket_id) ON DELETE CASCADE,
    -- Keyset cursor on objects.object_key. '' starts a sweep; object_key is NOT NULL and unique per
    -- bucket, so it is a total order and a strict `>` cursor cannot skip or repeat a row.
    cursor_key        text NOT NULL DEFAULT '',
    partial_bytes     bigint NOT NULL DEFAULT 0,
    objects_scanned   bigint NOT NULL DEFAULT 0,
    slices_done       integer NOT NULL DEFAULT 0,
    started_at        timestamptz NOT NULL DEFAULT now(),
    -- Captured at sweep start so the tolerance is (churn_now - start_churn_bytes).
    start_churn_bytes bigint NOT NULL DEFAULT 0
);

COMMENT ON TABLE bucket_storage_verify_state IS
    'In-progress sliced verification. A bucket too large to aggregate in one statement is summed in '
    'indexed object_key ranges across cycles; this holds the cursor and running total.';

-- Per-bucket lock key, to be passed as the SECOND argument of pg_advisory_xact_lock. The first stays
-- storage_usage_rollup_lock_key() so every rollup lock shares one namespace and cannot collide with
-- an unrelated advisory lock elsewhere in the schema.
--
-- Why this matters: the second argument used to be a hardcoded 0, which made every recompute and
-- every compaction contend on ONE lock estate-wide. A 300s recompute of one bucket therefore stalled
-- compaction for every other bucket -- the measured cause of the 412s ledger lag.
--
-- SAFE TO CHANGE IN ONE STEP, unlike the version-lock change that had to ship as two releases. A
-- pod on the old code would take (rollup_key, 0) while a pod on the new code takes
-- (rollup_key, bucket_key); those do not conflict, so a mixed fleet could interleave a compaction
-- and a recompute on the same bucket and double-count. There is no such window here: the only two
-- callers are the usage-rollup worker, which is `replicas: 1` with `strategy: Recreate` precisely so
-- a deploy never runs two pods (k8s/base/workers-deployments.yaml:1194), and the backfill Job, which
-- is run by hand against a pinned image. The API pods only INSERT into the ledger via triggers and
-- take neither lock.
CREATE OR REPLACE FUNCTION storage_usage_bucket_lock_key(p_bucket_id uuid)
RETURNS integer
LANGUAGE sql
IMMUTABLE
AS $$
    SELECT hashtext(p_bucket_id::text)
$$;

-- Recompute one bucket, now under a PER-BUCKET lock, stamping the attempt and clearing the failure
-- counter. Body is otherwise unchanged from 20260912090000 -- the same one-statement
-- DELETE-then-aggregate that makes it safe against concurrent writers.
CREATE OR REPLACE FUNCTION recompute_bucket_storage_usage(
    p_bucket_id uuid,
    OUT o_bytes_before bigint,
    OUT o_bytes_after bigint
)
LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM pg_advisory_xact_lock(storage_usage_rollup_lock_key(), storage_usage_bucket_lock_key(p_bucket_id));

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
    INSERT INTO bucket_storage_usage AS bsu (bucket_id, bytes_used, updated_at, recomputed_at, attempted_at)
    SELECT p_bucket_id, t.bytes, now(), now(), now()
    FROM truth t
    -- A bucket that no longer exists gets no row: the FK would raise, and the answer is 0 anyway.
    WHERE EXISTS (SELECT 1 FROM buckets b WHERE b.bucket_id = p_bucket_id)
    ON CONFLICT (bucket_id) DO UPDATE
       SET bytes_used = EXCLUDED.bytes_used,
           updated_at = now(),
           recomputed_at = now(),
           attempted_at = now(),
           recompute_failures = 0
    RETURNING bsu.bytes_used INTO o_bytes_after;

    o_bytes_before := COALESCE(o_bytes_before, 0);
    o_bytes_after := COALESCE(o_bytes_after, 0);
END;
$$;

-- migrate:down

SET LOCAL lock_timeout = '3s';

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

DROP FUNCTION IF EXISTS storage_usage_bucket_lock_key(uuid);
DROP TABLE IF EXISTS bucket_storage_verify_state;
DROP INDEX IF EXISTS idx_bucket_storage_usage_attempted;
ALTER TABLE bucket_storage_usage DROP COLUMN IF EXISTS churn_bytes;
ALTER TABLE bucket_storage_usage DROP COLUMN IF EXISTS recompute_failures;
ALTER TABLE bucket_storage_usage DROP COLUMN IF EXISTS attempted_at;
