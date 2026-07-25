-- Walk a keyset RING over soft-deleted objects and report, per row, whether it is ready to be
-- hard-deleted. The caller (gc_soft_deleted_objects) hard-deletes only the ready rows but advances
-- its durable cursor over the ENTIRE returned slice — so a permanently-unready object can no longer
-- sit at the head of an oldest-first batch and block every row behind it forever.
--
-- Head-of-line story (the bug this replaces): the previous shape materialised the oldest $1
-- soft-deleted objects FIRST, then FILTERED to just the ready ones. A never-ready head — e.g. a
-- CopyObject destination, which is created with ZERO chunk_backend rows and so could never satisfy
-- the old EXISTS(any row) audit guard — kept re-occupying that oldest slot every cycle. The filter
-- returned nothing, hard_deleted stayed 0 across cycles, `parts` never cascade-deleted, and the FS
-- bytes those parts protect were never reclaimed while the Ceph pool filled. Returning the whole
-- slice + a ring cursor means the head is scanned once and stepped past, not re-fetched forever.
--
-- Per-row readiness (`ready` boolean), NOT a WHERE filter:
--   ready = NOT (any live backend copy remains)
--           AND ( (object was replicated at least once)  -- the "all deleted" signal is meaningful
--                 OR deleted_at < now() - INTERVAL '24 hours' )  -- aged relaxation, see below
--   1. A live chunk_backend row (deleted=false) means either a partial unpin or a revival — never
--      ready.
--   2. With no live rows AND at least one chunk_backend row ever, "all deleted" is a real,
--      audit-backed unpin — ready.
--   3. Aged relaxation: with ZERO chunk_backend rows, the object was never replicated (the
--      CopyObject-destination population). The EXISTS(any row) audit guard would keep it immortal:
--      never hard-deleted -> `parts` never cascades -> stale-reap protects its FS bytes forever.
--      But the object is soft-DELETED — that is user delete intent — so after a 24h grace (covering
--      any in-flight unpin still recording chunk_backend rows) we accept the zero-row state as
--      ready. Fresh (<24h) zero-row rows stay unready so a lagging unpin can still land its rows.
--
-- Ring cursor: $2 = cursor deleted_at (timestamptz), $3 = cursor object_id (uuid). The candidates
-- CTE takes soft-deleted rows past the 1h grace with (deleted_at, object_id) > ($2, $3), ordered by
-- (deleted_at, object_id), LIMIT $1. The caller persists the last slice row as the next cursor and
-- resets to (epoch, nil-uuid) on an empty slice to wrap the ring.
--
-- Parameters: $1 = batch size, $2 = cursor deleted_at, $3 = cursor object_id.
--
-- We materialise the keyset slice FIRST, then compute readiness per row. Without AS MATERIALIZED the
-- planner estimates millions of matching deleted rows and folds the EXISTS/NOT EXISTS into a
-- parallel hash join that full-scans chunk_backend (~52M rows) + part_chunks + parts on every call —
-- a ~135 GiB read storm that saturated the data disk and stalled the primary (see
-- oom-psql-postmortem.md). MATERIALIZED + LIMIT forces per-object index probes on the small slice.
WITH candidates AS MATERIALIZED (
    SELECT object_id, deleted_at
    FROM objects
    WHERE deleted_at IS NOT NULL
      AND deleted_at < now() - INTERVAL '1 hour'  -- grace period
      AND (deleted_at, object_id) > ($2, $3)      -- keyset ring cursor
    ORDER BY deleted_at, object_id  -- keyset order; deleted_at range uses idx_objects_deleted
    LIMIT $1
)
SELECT
    c.object_id,
    c.deleted_at,
    (
        NOT EXISTS (
            SELECT 1
            FROM parts p
            JOIN part_chunks pc ON pc.part_id = p.part_id
            JOIN chunk_backend cb ON cb.chunk_id = pc.id
            WHERE p.object_id = c.object_id
              AND NOT cb.deleted
        )
        AND (
            EXISTS (
                SELECT 1
                FROM parts p
                JOIN part_chunks pc ON pc.part_id = p.part_id
                JOIN chunk_backend cb ON cb.chunk_id = pc.id
                WHERE p.object_id = c.object_id
            )
            OR c.deleted_at < now() - INTERVAL '24 hours'
        )
    ) AS ready
FROM candidates c
-- The caller derives the next ring cursor from the LAST returned row (`rows[-1]`), so the result
-- order is load-bearing, not cosmetic. A bare `FROM candidates c` inherits the CTE's order only
-- incidentally — SQL guarantees no ordering without ORDER BY, and a future plan shape (parallel
-- scan, a different join strategy) could return the slice in any order. `rows[-1]` would then not
-- be the maximum keyset position, and the cursor would either jump PAST unscanned rows (silently
-- skipping objects until the next lap) or move backwards (re-scanning). Ordering here makes the
-- caller's `rows[-1]` correct by construction; it is free, since `candidates` is already sorted on
-- exactly this key and is at most $1 rows.
ORDER BY c.deleted_at, c.object_id;
