-- Find soft-deleted objects whose chunks have all been confirmed deleted
-- from every backend.
--
-- Guard against two failure modes:
-- 1. Pure "no rows" false positive: the old query used only NOT EXISTS
--    over `chunk_backend`, which returned true if zero rows exist at all
--    (e.g. a crash during upload left no chunk_backend rows). That silently
--    hard-deleted object metadata without a real deletion audit trail.
-- 2. Partial unpin: some backends confirmed delete but others still have
--    live rows. The existing NOT EXISTS clause covers this.
--
-- This version additionally requires that AT LEAST ONE chunk_backend row
-- exists per expected chunk — i.e. the object was actually replicated at
-- some point, so the "all deleted" signal is meaningful.
--
-- Parameters: $1 = batch size (max objects SCANNED per call).
--             $2 = cursor deleted_at, $3 = cursor object_id (keyset position).
--
-- We materialise a small batch of soft-deleted candidates FIRST, then apply
-- the EXISTS/NOT EXISTS checks to just that batch. Without this, the planner
-- estimates millions of matching deleted rows and folds the EXISTS into a
-- parallel hash join that full-scans chunk_backend (~52M rows) + part_chunks +
-- parts on every call — a ~135 GiB read storm that saturated the data disk and
-- stalled the primary (see oom-psql-postmortem.md). AS MATERIALIZED + LIMIT
-- forces per-object index probes; the janitor drains the backlog over cycles.
--
-- HEAD-OF-LINE FIX (2026-07-24): the batch used to be a bare `ORDER BY deleted_at
-- LIMIT $1` — always the SAME oldest N objects. Those oldest objects are permanently
-- un-ready: their unpins were lost when the unpin queue was cleared during the
-- redis-queues unpin-overrun incident, so they still hold LIVE chunk_backend rows and
-- can never satisfy the NOT EXISTS. Measured on prod: of the 2000 oldest candidates
-- 0 were ready, while 1577/2000 RECENT ones were. The janitor therefore re-scanned the
-- same doomed batch every cycle and hard-deleted NOTHING (`hard_deleted=0` in every
-- cycle) while ~33M ready objects queued behind them — leaving their `parts` rows in
-- place, which in turn kept their FS cache dirs pinned (the replication gate reads an
-- all-unpinned part as "unreplicated" and protects it). That is what filled the pool.
--
-- The keyset cursor ($2,$3) makes the scan ADVANCE past un-ready objects instead of
-- restarting at the stuck head. The caller persists the cursor in `janitor_state` and
-- wraps back to the epoch on a short page, so stuck objects are revisited once per full
-- sweep (cheap) instead of blocking every cycle (fatal).
--
-- Returns EVERY scanned candidate with a `ready` flag — not just the ready ones — so the
-- caller can advance the cursor over un-ready objects too. Returning only ready rows
-- would leave the cursor pinned at the stuck head exactly as before.
WITH candidates AS MATERIALIZED (
    SELECT object_id, deleted_at
    FROM objects
    WHERE deleted_at IS NOT NULL
      AND deleted_at < now() - INTERVAL '1 hour'  -- grace period
      AND (deleted_at, object_id) > ($2::timestamptz, $3::uuid)
    ORDER BY deleted_at, object_id  -- uses idx_objects_deleted
    LIMIT $1
)
SELECT
    c.object_id,
    c.deleted_at,
    (
        EXISTS (
            SELECT 1
            FROM parts p
            JOIN part_chunks pc ON pc.part_id = p.part_id
            JOIN chunk_backend cb ON cb.chunk_id = pc.id
            WHERE p.object_id = c.object_id
        )
        AND NOT EXISTS (
            SELECT 1
            FROM parts p
            JOIN part_chunks pc ON pc.part_id = p.part_id
            JOIN chunk_backend cb ON cb.chunk_id = pc.id
            WHERE p.object_id = c.object_id
              AND NOT cb.deleted
        )
    ) AS ready
FROM candidates c
ORDER BY c.deleted_at, c.object_id;
