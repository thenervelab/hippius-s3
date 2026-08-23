-- Walk a keyset RING over soft-deleted object_versions and report, per row, whether its backend
-- copies are all confirmed gone. Mirrors find_objects_ready_for_hard_delete's shape AND its
-- readiness discipline: the caller reaps only the ready rows but advances its durable cursor over
-- the ENTIRE returned slice, so a permanently-unready head cannot block everything behind it.
--
-- Unlike the object-level gate this is VERSION-scoped (p.object_version = c.object_version): the
-- point is to reclaim one superseded version while its siblings stay live.
--
-- Per-row readiness, NOT a WHERE filter:
--   ready = NOT (any live backend copy for THIS version)
--           AND ( (this version was replicated at least once)   -- "all deleted" is meaningful
--                 OR deleted_at < now() - INTERVAL '24 hours' ) -- aged relaxation
--
-- The 24h relaxation is copied deliberately from the object-level gate, which documents why one
-- hour is not enough: with ZERO chunk_backend rows we cannot tell "never replicated" (a CopyObject
-- destination, which never gets rows of its own) from "the upload is still in flight". The drain
-- and arion-uploader have run hours behind before (see the upload-DLQ incidents), and reaping a
-- version whose upload has not landed yet destroys the parts rows that insert_chunk_backend needs:
-- it does `INSERT ... SELECT pc.id FROM part_chunks pc WHERE pc.part_id = $1`, so with the row gone
-- it inserts NOTHING, silently, after the bytes are already on Arion. That is a permanent backend
-- orphan with no DB record — the exact failure this whole change exists to prevent.
--
-- We materialise the keyset slice FIRST, then compute readiness per row. Without AS MATERIALIZED
-- the planner folds the EXISTS/NOT EXISTS into a hash join that full-scans chunk_backend (~343M
-- rows) — the read storm that stalled the primary (see oom-psql-postmortem.md).
--
-- Parameters: $1 = batch size, $2 = cursor deleted_at, $3 = cursor object_id, $4 = cursor object_version
WITH candidates AS MATERIALIZED (
    SELECT object_id, object_version, deleted_at
    FROM object_versions
    WHERE deleted_at IS NOT NULL
      AND deleted_at < now() - INTERVAL '1 hour'  -- grace period
      AND (deleted_at, object_id, object_version) > ($2, $3, $4)  -- keyset ring cursor
    ORDER BY deleted_at, object_id, object_version
    LIMIT $1
)
SELECT
    c.object_id,
    c.object_version,
    c.deleted_at,
    (
        NOT EXISTS (
            SELECT 1
            FROM parts p
            JOIN part_chunks pc ON pc.part_id = p.part_id
            JOIN chunk_backend cb ON cb.chunk_id = pc.id
            WHERE p.object_id = c.object_id
              AND p.object_version = c.object_version
              AND NOT cb.deleted
        )
        AND (
            EXISTS (
                SELECT 1
                FROM parts p
                JOIN part_chunks pc ON pc.part_id = p.part_id
                JOIN chunk_backend cb ON cb.chunk_id = pc.id
                WHERE p.object_id = c.object_id
                  AND p.object_version = c.object_version
            )
            OR c.deleted_at < now() - INTERVAL '24 hours'
        )
    ) AS ready
FROM candidates c
-- The caller derives the next cursor from the LAST row, so the order is load-bearing: a bare
-- `FROM candidates` inherits the CTE's order only incidentally.
ORDER BY c.deleted_at, c.object_id, c.object_version
