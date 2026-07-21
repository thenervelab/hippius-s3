-- G2 replication-gate sentinel: live, serveable chunks that lack full-union backend
-- coverage — the exact population the janitor's replication gate must NEVER let be
-- reclaimed, so any row here is a chunk at data-loss risk the moment its SSD/pool copy
-- is evicted (and a standing alarm under >=95% disk pressure).
--
-- A chunk is a VIOLATION when ALL of:
--   * its object is LIVE (objects.deleted_at IS NULL) — a soft-deleted object's chunks
--     are meant to lose coverage as the unpinner clears them, so they are not at risk;
--   * its version is SERVEABLE (object_versions.address IS NOT NULL) — a NULL-address
--     version is mid-upload/aborted, not yet durable, and is the reaper's/orphan-GC's
--     concern, not a replication-gap;
--   * some REQUIRED backend has no live (NOT deleted) chunk_backend row for the chunk; AND
--   * the part landed longer ago than the replication SLA ($4) — the grace valve. address
--     is stamped at PUT completion, but the chunk_backend coverage row is written much later
--     by the async drain→pool→backend pipeline, so EVERY servable chunk is briefly
--     under-covered while it replicates NORMALLY. Without this window the sentinel counts
--     that in-flight replication as a durability breach and pages continuously under upload
--     traffic — defeating the one alarm that matters. parts.uploaded_at is the part's landing
--     time (NOT NULL, so the interval compare has no three-valued-logic surprise; it is reset
--     only by a fresh overwrite of the same part, which legitimately restarts replication —
--     it is never bumped by drain re-claim/defer churn, unlike updated_at). Only a chunk still
--     under-covered PAST the SLA is genuinely stuck. Chosen over cephor_replication_status
--     .landed_at (the A21 sweep's idle signal) because uploaded_at is already joined here and
--     is the same signal the sibling mtime-cleaner uses to protect recently-landed parts;
--     keying on landed_at would require a new join and couple the sentinel to the drain schema.
--
-- The required set mirrors is_replicated_on_all_backends exactly: per-version
-- upload_backends (or ['ipfs'] for a migration version, or the caller's config default
-- $2 when the column is empty/NULL) UNIONed with the configured backup_backends ($1).
-- This is what makes the sentinel catch the C10 divergence (a backup backend required by
-- the gate but never enqueued leaves every chunk missing that backend's row).
--
-- Bounded by $3 so a breach is detected and sampled cheaply without an unbounded result;
-- the caller treats a full page as ">= limit". The EXISTS/NOT EXISTS pair uses the
-- chunk_backend (chunk_id) live index rather than aggregating every backend per chunk.
--
-- Parameters:
--   $1 backup_backends          TEXT[]  — configured HIPPIUS_BACKUP_BACKENDS
--   $2 default_upload_backends  TEXT[]  — config.upload_backends (fallback for legacy rows)
--   $3 limit                    INT     — max violating chunks to return
--   $4 replication_sla_seconds  INT     — grace window; only parts landed longer ago than
--                                         this (normal replication latency + margin) count
WITH live_versions AS (
    SELECT
        ov.object_id,
        ov.object_version,
        ARRAY(
            SELECT DISTINCT unnest(
                (CASE
                    WHEN ov.version_type = 'migration' THEN ARRAY['ipfs']::text[]
                    WHEN ov.upload_backends IS NOT NULL AND cardinality(ov.upload_backends) > 0 THEN ov.upload_backends
                    ELSE $2::text[]
                END) || $1::text[]
            )
        ) AS required
    FROM object_versions ov
    JOIN objects o ON o.object_id = ov.object_id
    WHERE o.deleted_at IS NULL
      AND ov.address IS NOT NULL
)
SELECT lv.object_id, lv.object_version, pc.id AS chunk_id
FROM live_versions lv
JOIN parts p ON p.object_id = lv.object_id AND p.object_version = lv.object_version
JOIN part_chunks pc ON pc.part_id = p.part_id
WHERE p.uploaded_at < now() - make_interval(secs => $4)
  AND EXISTS (
    SELECT 1
    FROM unnest(lv.required) AS req(backend)
    WHERE NOT EXISTS (
        SELECT 1 FROM chunk_backend cb
        WHERE cb.chunk_id = pc.id AND cb.backend = req.backend AND NOT cb.deleted
    )
)
LIMIT $3
