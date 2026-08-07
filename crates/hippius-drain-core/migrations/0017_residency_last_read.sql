-- Make read-tier eviction recency-aware instead of FIFO.
--
-- Eviction ordered on `resident_at` — when a part JOINED the cache, which says nothing about
-- whether anyone is still reading it. That is FIFO replacement, and FIFO is a poor fit for the
-- workload this tier exists for: a training set re-read every epoch is maximally skewed, and
-- FIFO evicts by arrival order, so the parts about to be re-read are exactly the ones most
-- likely to go. Each eviction then costs a peer-or-pool read plus a local write to put the same
-- part back — thrash that gets worse the more valuable the cache is.
--
-- WHY A NEW COLUMN RATHER THAN BUMPING resident_at
--
-- Overloading `resident_at` was considered and rejected. It would leave the column meaning
-- "when it became resident OR when it was last read", contradicting migration 0016's own
-- description and the evictor's FIFO documentation, and there would be no way afterwards to ask
-- how long a part had actually been cached. A nullable column keeps both facts and makes the
-- fallback explicit at the point of use: COALESCE(last_read_at, resident_at).
--
-- NULL is the correct default and needs no backfill. A part nobody has read since this shipped
-- has no read recency, and treating its residency time as its last-read time is exactly the old
-- behaviour — so the change degrades to FIFO for the existing population and becomes recency as
-- reads land. There is no window where eviction is ordered on nothing.
ALTER TABLE cephor_ssd_residency
    ADD COLUMN IF NOT EXISTS last_read_at TIMESTAMPTZ;

-- The eviction cursor's index has to match the ORDER BY expression or the planner sorts the
-- whole resident set (~2M rows per node) on every pass — which would reintroduce, in Postgres,
-- precisely the O(resident) cost the walk-free evictor was built to avoid.
CREATE INDEX IF NOT EXISTS cephor_ssd_residency_recency_idx
    ON cephor_ssd_residency (node_id, (COALESCE(last_read_at, resident_at)));

-- The FIFO index is now dead: every eviction query orders on the expression above, and the
-- part-lookup index (cephor_ssd_residency_part_idx) serves the peer resolver. Dropped rather
-- than left in place — an unused index on a table taking a write per promotion and per sampled
-- read is pure write amplification.
DROP INDEX IF EXISTS cephor_ssd_residency_evict_idx;
