-- One-off backfill for the enqueue-sweep head-of-line block (2026-08-27 .. 2026-09-14).
--
-- Run ONCE against the PRIMARY (postgres-nvme-rw) after the drain image with the fixed
-- worklist is rolled out. Everything here is idempotent and only touches rows the fixed
-- sweep can never publish, so re-running is harmless; it exists to shrink the not-ready head
-- the worklist's address filter otherwise has to step over on every poll.
--
--   psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f scripts/drain_enqueue_sweep_backfill.sql
--
-- 1. Rows whose version already has every chunk on arion but no address (pre-cutover objects
--    re-recorded from read-promoted SSD copies): stamp upload_enqueued_at — there is nothing
--    to publish, and the stamp is what takes a row off the worklist.
-- 2. Rows whose object_versions row is gone (hard-deleted objects): delete, unless the part is
--    still resident on a node's SSD (the evictor owns it; gc_terminal_status_rows reaps the
--    row once the copy is gone).
-- 3. Report what is left: these are the rows the sweep WILL now publish.

\timing on

-- (1) ~476k rows on prod; batched so no single statement holds a long lock on the hot table.
DO $$
DECLARE
    n bigint;
    total bigint := 0;
BEGIN
    LOOP
        WITH batch AS (
            SELECT s.object_id, s.version, s.part_number
            FROM cephor_replication_status s
            JOIN object_versions ov
              ON ov.object_id = s.object_id::uuid AND ov.object_version = s.version
            JOIN parts p
              ON p.object_id = ov.object_id AND p.object_version = ov.object_version
             AND p.part_number = s.part_number
            WHERE s.status = 'replicated' AND s.upload_enqueued_at IS NULL
              AND ov.address IS NULL
              AND NOT EXISTS (
                  SELECT 1 FROM part_chunks pc
                  WHERE pc.part_id = p.part_id
                    AND NOT EXISTS (
                        SELECT 1 FROM chunk_backend cb
                        WHERE cb.chunk_id = pc.id AND cb.backend = 'arion' AND NOT cb.deleted
                    )
              )
            LIMIT 5000
        )
        UPDATE cephor_replication_status s
        SET upload_enqueued_at = now()
        FROM batch b
        WHERE s.object_id = b.object_id AND s.version = b.version AND s.part_number = b.part_number;
        GET DIAGNOSTICS n = ROW_COUNT;
        total := total + n;
        RAISE NOTICE 'stamped % already-on-arion rows (total %)', n, total;
        EXIT WHEN n = 0;
        PERFORM pg_sleep(0.2);
    END LOOP;
END $$;

-- (2) ~150k rows per node on prod.
DO $$
DECLARE
    n bigint;
    total bigint := 0;
BEGIN
    LOOP
        WITH batch AS (
            SELECT s.object_id, s.version, s.part_number
            FROM cephor_replication_status s
            WHERE s.status = 'replicated' AND s.upload_enqueued_at IS NULL
              AND NOT EXISTS (
                  SELECT 1 FROM object_versions ov
                  WHERE ov.object_id = s.object_id::uuid AND ov.object_version = s.version
              )
              AND NOT EXISTS (
                  SELECT 1 FROM cephor_ssd_residency r
                  WHERE r.object_id = s.object_id AND r.version = s.version AND r.part_number = s.part_number
              )
            LIMIT 5000
        )
        DELETE FROM cephor_replication_status s
        USING batch b
        WHERE s.object_id = b.object_id AND s.version = b.version AND s.part_number = b.part_number;
        GET DIAGNOSTICS n = ROW_COUNT;
        total := total + n;
        RAISE NOTICE 'deleted % gone-version rows (total %)', n, total;
        EXIT WHEN n = 0;
        PERFORM pg_sleep(0.2);
    END LOOP;
END $$;

-- (3) What remains on the worklist: the publishable backlog per node.
SELECT s.node_id, count(*) AS publishable, min(s.landed_at) AS oldest
FROM cephor_replication_status s
JOIN object_versions ov ON ov.object_id = s.object_id::uuid AND ov.object_version = s.version
WHERE s.status = 'replicated' AND s.upload_enqueued_at IS NULL AND ov.address IS NOT NULL
GROUP BY 1 ORDER BY 1;
