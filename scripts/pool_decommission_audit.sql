-- Pool decommission audit — READ ONLY. Run on the read replica:
--
--   psql "$DATABASE_READONLY_URL" -v ON_ERROR_STOP=1 -f scripts/pool_decommission_audit.sql
--
-- The CephFS pool may be unmounted only when NO live, servable part depends on it: every
-- chunk of every live version must have a live row on every required backend. Until then a
-- pool part can be the only copy (pool-era 'replicated' rows the legacy enqueue sweep has
-- not published yet, OVH-only parts, failed uploads). Every section below must report zero
-- before the pool goes; the last two are the breakdown of what is still holding it.
--
-- Predicates mirror find_underreplicated_live_chunks.sql (the janitor's replication gate):
-- live = objects.deleted_at IS NULL; servable = object_versions.address IS NOT NULL;
-- required backends = the version's upload_backends (or the deployment default) ∪ backup
-- backends. Set :required to the deployment's current union, e.g. '{arion}'.

\set required '{arion}'
\timing on
SET statement_timeout = '900s';

-- 1. Live, servable chunks lacking a live row on some required backend. This is the number
--    that must be zero. (The janitor's sentinel caps at 500; this counts them all.)
SELECT count(*) AS live_chunks_missing_a_required_backend
FROM object_versions ov
JOIN objects o ON o.object_id = ov.object_id
JOIN parts p ON p.object_id = ov.object_id AND p.object_version = ov.object_version
JOIN part_chunks pc ON pc.part_id = p.part_id
WHERE o.deleted_at IS NULL
  AND ov.address IS NOT NULL
  AND EXISTS (
      SELECT 1 FROM unnest(:'required'::text[]) AS req(backend)
      WHERE NOT EXISTS (
          SELECT 1 FROM chunk_backend cb
          WHERE cb.chunk_id = pc.id AND cb.backend = req.backend AND NOT cb.deleted));

-- 2. Pool-era rows the legacy enqueue sweep still owes a publish for (bytes in the pool,
--    request never sent). Must be zero: this is the 2026-08-27 backlog draining.
SELECT s.node_id, count(*) AS replicated_unpublished
FROM cephor_replication_status s
JOIN object_versions ov ON ov.object_id = s.object_id::uuid AND ov.object_version = s.version
WHERE s.status = 'replicated' AND s.upload_enqueued_at IS NULL AND ov.address IS NOT NULL
GROUP BY 1 ORDER BY 1;

-- 3. Parts whose ONLY live backend row is on a backend outside the required set (e.g. an
--    OVH-only part after an arion upload failed). Must be zero before the ovh rows are
--    soft-deleted, or those objects become unreadable.
SELECT count(DISTINCT p.part_id) AS parts_only_on_a_non_required_backend
FROM parts p
JOIN object_versions ov ON ov.object_id = p.object_id AND ov.object_version = p.object_version
JOIN objects o ON o.object_id = ov.object_id
JOIN part_chunks pc ON pc.part_id = p.part_id
WHERE o.deleted_at IS NULL AND ov.address IS NOT NULL
  AND EXISTS (SELECT 1 FROM chunk_backend cb WHERE cb.chunk_id = pc.id AND NOT cb.deleted)
  AND NOT EXISTS (
      SELECT 1 FROM chunk_backend cb
      WHERE cb.chunk_id = pc.id AND NOT cb.deleted AND cb.backend = ANY(:'required'::text[]));

-- 4. Breakdown of section 1 by the version's drain state, to see what is holding the pool.
SELECT COALESCE(s.status, '(no drain row)') AS drain_status,
       (s.upload_enqueued_at IS NOT NULL) AS published,
       count(DISTINCT (p.object_id, p.object_version, p.part_number)) AS parts,
       pg_size_pretty(sum(p.size_bytes)::bigint) AS bytes
FROM object_versions ov
JOIN objects o ON o.object_id = ov.object_id
JOIN parts p ON p.object_id = ov.object_id AND p.object_version = ov.object_version
LEFT JOIN cephor_replication_status s
       ON s.object_id = p.object_id::text AND s.version = p.object_version AND s.part_number = p.part_number
WHERE o.deleted_at IS NULL
  AND ov.address IS NOT NULL
  AND EXISTS (
      SELECT 1 FROM part_chunks pc
      CROSS JOIN unnest(:'required'::text[]) AS req(backend)
      WHERE pc.part_id = p.part_id
        AND NOT EXISTS (
            SELECT 1 FROM chunk_backend cb
            WHERE cb.chunk_id = pc.id AND cb.backend = req.backend AND NOT cb.deleted))
GROUP BY 1, 2 ORDER BY 3 DESC;

-- 5. Upload DLQ is the other place a part can be stuck; the audit is not done while the DLQ
--    holds entries for live objects. Check separately:
--      redis-cli -u "$REDIS_QUEUES_URL" LLEN arion_upload_requests:dlq
