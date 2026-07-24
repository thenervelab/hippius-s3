-- CURSOR-ADVANCING scan window for the janitor's SQL-driven eviction phase. A pure keyset slice
-- of fs_cache_inventory in (cached_at, object_id, object_version, part_number) order — the exact
-- column order of index fs_cache_inventory_cached_at — so it is served index-only and its cost is
-- bounded by LIMIT, never by inventory sparseness. This is the ONLY query that advances the ring
-- cursor: the evictability filter (janitor_evictable_candidates.sql) runs SEPARATELY over the rows
-- this slice returns, so a window that is 100% non-candidates (e.g. a head of millions of
-- non-replicated parts) STILL advances the cursor by the rows it scanned. That decoupling is the
-- whole point — the old single-query design re-scanned from the same cursor every cycle and, on an
-- inventory whose head is mostly non-candidates, deterministically re-timed-out and never advanced.
--
-- KEYSET contract: the row-value comparison returns only rows STRICTLY AFTER the cursor tuple, so
-- paging is gap-free and duplicate-free as the caller advances the cursor to each page's last row.
-- The cursor columns are all NOT NULL in the inventory table, so the caller must pass a non-NULL
-- cursor — a real datetime for $2 (an epoch/min datetime is the cold-start sentinel, strictly below
-- every row) and '' / 0 / 0 for $3/$4/$5 — never NULL (a NULL element makes the comparison NULL and
-- drops every row).
--
-- CONTRACT (LOCKED — the SQL-eviction worker binds positionally against this exact order; do NOT
-- reorder/renumber params or reorder the returned columns). Returns (object_id, object_version,
-- part_number, cached_at); the caller advances the cursor to the LAST returned row.
--
-- Parameters:
--   $1 page_limit            INT         — rows scanned per page (caller treats a short page as end-of-ring)
--   $2 cursor_cached_at      TIMESTAMPTZ — keyset cursor cached_at, bound as a native datetime by the
--                                          caller (cold-start sentinel = an epoch/min datetime)
--   $3 cursor_object_id      TEXT        — keyset cursor: last page's object_id   (start '')
--   $4 cursor_object_version BIGINT      — keyset cursor: last page's object_version (start 0)
--   $5 cursor_part_number    BIGINT      — keyset cursor: last page's part_number (start 0)
SELECT object_id, object_version, part_number, cached_at
FROM fs_cache_inventory
WHERE (cached_at, object_id, object_version, part_number) > ($2, $3, $4, $5)
ORDER BY cached_at, object_id, object_version, part_number
LIMIT $1
