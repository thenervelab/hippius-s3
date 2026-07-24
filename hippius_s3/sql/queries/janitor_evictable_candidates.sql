-- Evictable-part FILTER for the janitor's SQL-driven discovery phase. It is the
-- `find_underreplicated_live_chunks` sentinel with its coverage predicate FLIPPED: instead
-- of "some required backend is missing for some chunk" it returns parts where EVERY expected
-- chunk has a live chunk_backend row for EVERY required backend — i.e. the fully-replicated,
-- safe-to-evict population.
--
-- SLICE-THEN-FILTER: this query does NOT scan or paginate fs_cache_inventory. It filters a GIVEN
-- set of inventory tuples — the page produced by janitor_inventory_slice.sql — passed in as three
-- parallel arrays and expanded with `unnest ... WITH ORDINALITY`. The cursor advances by the SLICE
-- rows scanned, not by this query's output, so a slice that is 100% non-candidates still advances
-- the ring (the stall fix). All the safety semantics below are unchanged from the scan-based form;
-- only the pagination scaffolding (keyset predicate / ORDER BY cached_at / LIMIT) is gone.
--
-- PREFILTER ONLY. The worker re-runs the authoritative per-part replication gate
-- (`is_replicated_on_all_backends`) on its own connection immediately before deleting, so a
-- row returned here that has since lost coverage is simply skipped at delete time. This query
-- never authorizes a delete on its own; it only bounds which parts the worker bothers to stat.
-- Consequently any divergence degrades to "part not evicted this cycle", never an unsafe delete.
--
-- Required-backend set per version — mirrors is_replicated_on_all_backends /
-- find_underreplicated_live_chunks EXACTLY (keep them in lockstep): version_type='migration'
-- forces ['ipfs']; else the version's own upload_backends when non-empty; else the caller's
-- config default ($5) for legacy NULL rows — all UNIONed with the configured backup_backends
-- ($4). The flipped NOT EXISTS below is what makes a backup backend the gate requires but the
-- enqueuer never pushed to (the C10 divergence) keep a part OUT of the candidate set.
--
-- expected_chunks guard: a part still materialising (fewer part_chunks rows than its size
-- implies) must NOT be evicted. expected = CEIL(size_bytes / chunk_size_bytes) using the SAME
-- formula and 4 MiB legacy fallback as count_chunk_backends.sql. Zero part_chunks rows never
-- qualify (the CopyObject destination population: fresh ciphertext, zero chunk_backend rows —
-- excluded here by the coverage anti-join anyway, but the >0 guard makes the intent explicit).
--
-- CAST DIRECTION (load-bearing): the slice tuples' object_id arrives as TEXT (fs_cache_inventory
-- stores it as TEXT), parts.object_id is uuid. We cast the SLICE side (s.oid::uuid) so
-- parts.object_id stays a bare indexed column and the join can drive off idx_parts_object_id.
-- Casting parts.object_id::text instead would turn it into an expression, defeat every parts index
-- (no leading-column probe), and force a seq scan / hash join over the whole parts table. Precondition:
-- inventory rows are always UUID-shaped — producers insert DB uuids and the walk backfill skips
-- non-uuid dirnames (_safe_object_id enforces the shape); a malformed row would raise on the
-- cast, which is the correct fail-loud direction for a corrupt index entry.
--
-- OUTPUT ORDER: rows are returned in the input slice order (ORDER BY s.ord). The worker processes
-- candidates through a concurrent pool, so order affects only stable/deterministic processing, not
-- correctness; matching the slice keeps oldest-first intent without re-reading cached_at here.
--
-- CONTRACT (LOCKED — the SQL-eviction worker binds positionally against this exact order; do NOT
-- reorder/renumber params or reorder the returned columns). Returns exactly (object_id,
-- object_version, part_number).
--
-- Parameters:
--   $1 object_ids               TEXT[]   — slice tuples' object_id (parallel array; unnest WITH ORDINALITY)
--   $2 object_versions          BIGINT[] — slice tuples' object_version (parallel array)
--   $3 part_numbers             BIGINT[] — slice tuples' part_number (parallel array)
--   $4 backup_backends          TEXT[]   — configured HIPPIUS_BACKUP_BACKENDS
--   $5 default_upload_backends  TEXT[]   — config.upload_backends (fallback for NULL rows)
--   $6 max_age_seconds          INT      — age gate; a part qualifies only if uploaded_at is older
--   $7 ignore_age               BOOL     — pressure override; TRUE bypasses the age gate
WITH slice AS (
    SELECT oid, ver, pnum, ord
    FROM unnest($1::text[], $2::bigint[], $3::bigint[]) WITH ORDINALITY AS s(oid, ver, pnum, ord)
),
required_sets AS (
    SELECT
        ov.object_id,
        ov.object_version,
        ARRAY(
            SELECT DISTINCT unnest(
                (CASE
                    WHEN ov.version_type = 'migration' THEN ARRAY['ipfs']::text[]
                    WHEN ov.upload_backends IS NOT NULL AND cardinality(ov.upload_backends) > 0 THEN ov.upload_backends
                    ELSE $5::text[]
                END) || $4::text[]
            )
        ) AS required
    FROM object_versions ov
)
SELECT s.oid AS object_id, s.ver AS object_version, s.pnum AS part_number
FROM slice s
JOIN parts p
  ON p.object_id = s.oid::uuid
 AND p.object_version = s.ver
 AND p.part_number = s.pnum
JOIN required_sets rs
  ON rs.object_id = p.object_id AND rs.object_version = p.object_version
WHERE ($7 OR p.uploaded_at < now() - make_interval(secs => $6))
  -- Expected chunk population fully present (not mid-materialisation), in ONE part_chunks probe:
  -- count >= GREATEST(expected, 1) folds in the old "> 0" guard — a part whose size implies 0 chunks
  -- still needs at least one row present, and when expected >= 1 the >= already implies > 0. Exactly
  -- equivalent to the old (count > 0 AND count >= expected) pair, at half the part_chunks scans.
  AND (SELECT count(*) FROM part_chunks pc WHERE pc.part_id = p.part_id)
      >= GREATEST(CEIL(p.size_bytes::float / GREATEST(COALESCE(p.chunk_size_bytes, 4194304), 1))::int, 1)
  -- …and NO chunk is missing ANY required backend (flipped sentinel: the sentinel returns rows
  -- where this inner NOT EXISTS holds; we keep only rows where it holds for NONE of the chunks).
  AND NOT EXISTS (
    SELECT 1
    FROM part_chunks pc
    CROSS JOIN unnest(rs.required) AS req(backend)
    WHERE pc.part_id = p.part_id
      AND NOT EXISTS (
          SELECT 1 FROM chunk_backend cb
          WHERE cb.chunk_id = pc.id AND cb.backend = req.backend AND NOT cb.deleted
      )
  )
ORDER BY s.ord
