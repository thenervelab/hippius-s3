-- Walk a keyset RING over soft-deleted object_versions and report, per row, whether the version's
-- backend copies are all confirmed gone. Mirrors find_objects_ready_for_hard_delete's shape and
-- discipline: the caller reaps only the ready rows but advances its durable cursor over the ENTIRE
-- returned slice, so a permanently-unready head cannot block everything behind it forever.
--
-- Unlike the object-level gate this is VERSION-scoped (p.object_version = c.object_version): the
-- point is to reclaim one superseded version while its siblings stay live.
--
-- The 1h grace covers an unpin still in flight — the version is already invisible to reads (every
-- read query filters ov.deleted_at IS NOT NULL), so there is no rush.
--
-- Zero chunk_backend rows counts as ready: a CopyObject destination reuses the source's backend
-- identifiers and never gets rows of its own, so requiring EXISTS would make those versions
-- immortal (the same trap documented on find_objects_ready_for_hard_delete).
--
-- We materialise the keyset slice FIRST, then compute readiness per row. Without AS MATERIALIZED
-- the planner folds the NOT EXISTS into a hash join that full-scans chunk_backend (~336M rows) —
-- the read storm that stalled the primary (see oom-psql-postmortem.md).
--
-- Parameters: $1 = batch size, $2 = cursor deleted_at, $3 = cursor object_id, $4 = cursor object_version
WITH candidates AS MATERIALIZED (
    SELECT object_id, object_version, deleted_at
    FROM object_versions
    WHERE deleted_at IS NOT NULL
      AND deleted_at < now() - INTERVAL '1 hour'
      AND (deleted_at, object_id, object_version) > ($2, $3, $4)
    ORDER BY deleted_at, object_id, object_version
    LIMIT $1
)
SELECT
    c.object_id,
    c.object_version,
    c.deleted_at,
    NOT EXISTS (
        SELECT 1
        FROM parts p
        JOIN part_chunks pc ON pc.part_id = p.part_id
        JOIN chunk_backend cb ON cb.chunk_id = pc.id
        WHERE p.object_id = c.object_id
          AND p.object_version = c.object_version
          AND NOT cb.deleted
    ) AS ready
FROM candidates c
-- The caller derives the next cursor from the LAST row, so the order is load-bearing: a bare
-- `FROM candidates` inherits the CTE's order only incidentally.
ORDER BY c.deleted_at, c.object_id, c.object_version
