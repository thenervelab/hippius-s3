-- Evictable-part PREFILTER for the janitor's SQL-driven discovery phase. It is the
-- `find_underreplicated_live_chunks` sentinel with its coverage predicate FLIPPED: instead
-- of "some required backend is missing for some chunk" it returns parts where EVERY expected
-- chunk has a live chunk_backend row for EVERY required backend — i.e. the fully-replicated,
-- safe-to-evict population — joined to fs_cache_inventory (the parts actually resident on the
-- cache FS) and walked oldest-first via a keyset cursor.
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
-- config default ($2) for legacy NULL rows — all UNIONed with the configured backup_backends
-- ($1). The flipped NOT EXISTS below is what makes a backup backend the gate requires but the
-- enqueuer never pushed to (the C10 divergence) keep a part OUT of the candidate set.
--
-- expected_chunks guard: a part still materialising (fewer part_chunks rows than its size
-- implies) must NOT be evicted. expected = CEIL(size_bytes / chunk_size_bytes) using the SAME
-- formula and 4 MiB legacy fallback as count_chunk_backends.sql. Zero part_chunks rows never
-- qualify (the CopyObject destination population: fresh ciphertext, zero chunk_backend rows —
-- excluded here by the coverage anti-join anyway, but the >0 guard makes the intent explicit).
--
-- CAST DIRECTION (load-bearing): fs_cache_inventory.object_id is TEXT, parts.object_id is uuid.
-- We cast the INVENTORY side (inv.object_id::uuid) so parts.object_id stays a bare indexed
-- column and the join can drive off idx_parts_object_id. Casting parts.object_id::text instead
-- would turn it into an expression, defeat every parts index (no leading-column probe), and
-- force a seq scan / hash join over the whole parts table per keyset page. Precondition:
-- inventory rows are always UUID-shaped — producers insert DB uuids and the walk backfill skips
-- non-uuid dirnames (_safe_object_id enforces the shape); a malformed row would raise on the
-- cast, which is the correct fail-loud direction for a corrupt index entry.
--
-- KEYSET contract: rows are ordered by (cached_at, object_id, object_version, part_number) — the
-- exact column order of index fs_cache_inventory_cached_at — and the row-value comparison returns
-- only rows STRICTLY AFTER the cursor tuple, so paging is gap-free and duplicate-free as the
-- caller advances the cursor to each page's last row. The cursor columns are all NOT NULL in the
-- inventory table, so the caller must pass a non-NULL cursor — a real datetime for $6 (an epoch/min
-- datetime is the cold-start sentinel, strictly below every row) and '' / 0 / 0 for $7/$8/$9 — never
-- NULL (a NULL element makes the row comparison NULL and drops every row).
--
-- CONTRACT (LOCKED — the SQL-eviction worker in run_janitor_in_loop binds positionally against
-- this exact order; do NOT reorder/renumber params or reorder the returned columns). Returns
-- exactly (object_id, object_version, part_number, cached_at).
--
-- Parameters:
--   $1 backup_backends          TEXT[]      — configured HIPPIUS_BACKUP_BACKENDS
--   $2 default_upload_backends  TEXT[]      — config.upload_backends (fallback for NULL rows)
--   $3 page_limit               INT         — keyset page size (caller treats a short page as end-of-ring)
--   $4 max_age_seconds          INT         — age gate; a part qualifies only if uploaded_at is older
--   $5 ignore_age               BOOL        — pressure override; TRUE bypasses the age gate
--   $6 cursor_cached_at         TIMESTAMPTZ — keyset cursor cached_at, bound as a native datetime by
--                                             the caller (cold-start sentinel = an epoch/min datetime)
--   $7 cursor_object_id         TEXT        — keyset cursor: last page's object_id   (start '')
--   $8 cursor_object_version    BIGINT      — keyset cursor: last page's object_version (start 0)
--   $9 cursor_part_number       BIGINT      — keyset cursor: last page's part_number (start 0)
WITH required_sets AS (
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
)
SELECT inv.object_id, inv.object_version, inv.part_number, inv.cached_at
FROM fs_cache_inventory inv
JOIN parts p
  ON p.object_id = inv.object_id::uuid
 AND p.object_version = inv.object_version
 AND p.part_number = inv.part_number
JOIN required_sets rs
  ON rs.object_id = p.object_id AND rs.object_version = p.object_version
WHERE ($5 OR p.uploaded_at < now() - make_interval(secs => $4))
  AND (inv.cached_at, inv.object_id, inv.object_version, inv.part_number)
      > ($6, $7, $8, $9)
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
ORDER BY inv.cached_at, inv.object_id, inv.object_version, inv.part_number
LIMIT $3
