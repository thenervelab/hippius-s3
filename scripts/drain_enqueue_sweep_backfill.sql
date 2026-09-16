-- One-off backfill for the enqueue-sweep head-of-line block (2026-08-27 .. 2026-09-14).
--
-- Run ONCE against the PRIMARY (postgres-nvme-rw) after the drain image with the fixed
-- worklist is rolled out. Idempotent: it only removes rows the drain has nothing to do for,
-- so re-running finds nothing. It exists to shrink the not-ready head the worklist's address
-- filter otherwise steps over on every poll.
--
--   psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f scripts/drain_enqueue_sweep_backfill.sql
--
-- The rows: `replicated` with no publish, for a version that has no address but is already
-- fully on arion — pre-cutover objects the reconciler re-recorded from read-promoted SSD
-- copies. The drain never published them and never will, so the truthful disposition is to
-- DELETE the row, not to stamp it as published: cache ownership keys on residency (see
-- `evictable_parts`), so a resident copy stays evictable without its row, and the reconciler
-- reads "resident, no row" as the promoted copy it is rather than re-recording it. Rows whose
-- object_versions row is gone are left to gc_terminal_status_rows.
--
-- The candidate set is computed ONCE into a temp table (the expensive per-chunk coverage
-- check) and applied in a single DELETE. `bool_and` over a part with no part_chunks rows is
-- NULL, which fails the WHERE — an in-flight part whose placeholder insert failed is never a
-- candidate — and the 24h landed_at gate keeps a row inside its normal drain -> Complete
-- window out of the set regardless.

\timing on

CREATE TEMP TABLE backfill_drop AS
SELECT s.object_id, s.version, s.part_number
FROM cephor_replication_status s
JOIN object_versions ov
  ON ov.object_id = s.object_id::uuid AND ov.object_version = s.version
JOIN parts p
  ON p.object_id = ov.object_id AND p.object_version = ov.object_version
 AND p.part_number = s.part_number
WHERE s.status = 'replicated' AND s.upload_enqueued_at IS NULL
  AND s.landed_at < now() - interval '24 hours'
  AND ov.address IS NULL
  AND (SELECT bool_and(EXISTS (SELECT 1 FROM chunk_backend cb
                               WHERE cb.chunk_id = pc.id AND cb.backend = 'arion' AND NOT cb.deleted))
       FROM part_chunks pc WHERE pc.part_id = p.part_id);

ANALYZE backfill_drop;
SELECT count(*) AS rows_to_drop FROM backfill_drop;

DELETE FROM cephor_replication_status s
USING backfill_drop b
WHERE s.object_id = b.object_id AND s.version = b.version AND s.part_number = b.part_number
  AND s.status = 'replicated' AND s.upload_enqueued_at IS NULL;

-- What remains on the worklist: the publishable backlog per node.
SELECT s.node_id, count(*) AS publishable, min(s.landed_at) AS oldest
FROM cephor_replication_status s
JOIN object_versions ov ON ov.object_id = s.object_id::uuid AND ov.object_version = s.version
WHERE s.status = 'replicated' AND s.upload_enqueued_at IS NULL AND ov.address IS NOT NULL
GROUP BY 1 ORDER BY 1;
