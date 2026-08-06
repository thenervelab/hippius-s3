-- Track which parts are RESIDENT on which node's ingest SSD.
--
-- Until now "replicated" and "gone from the SSD" were the same event: drain_part unlinked the
-- local copy as the last step of its happy path, so the SSD held exactly the undrained backlog.
-- Retaining replicated parts to serve reads from local NVMe (~705 MB/s, ~6 ms per chunk)
-- instead of the CephFS pool (~94 MB/s, ~40 ms, measured on node1 2026-08-06) breaks that
-- equivalence, so residency needs its own state.
--
-- Residency is its OWN TABLE keyed by node, not a pair of columns on
-- cephor_replication_status, and that is the load-bearing decision here. That table's node_id
-- is stamped by record_landed_part — it is the INGESTING node — so residency columns on it
-- could only ever express "the ingest node still holds its own copy". That is true today but
-- false the moment a read promotes a part onto a node that did not ingest it: the copy would
-- live on node B while the row said node A, node B's node-scoped evictor would never select
-- it, and it would leak on that disk forever with nothing able to reclaim it. Residency is
-- inherently per (node, part), so it is modelled that way.
--
-- bytes is denormalized from parts.size_bytes rather than joined at eviction time: the
-- eviction cursor runs under disk pressure and `parts` is ~140M rows, so the sum that decides
-- how far down the worklist to walk should not depend on a join to it. A part whose size is
-- unknown records 0 — it is still evictable, it just frees no *accounted* bytes.
--
-- A row exists iff the part is believed present on that node's SSD. Eviction DELETEs the row
-- rather than tombstoning it, so the table stays bounded by what is actually on disk (~2M rows
-- per node at 3.84 TB) instead of growing without limit alongside a table that is ~99.98%
-- terminal.
--
-- Nothing backfills this. Prod's ~11.08M 'replicated' rows (2026-08-06) had their SSD copies
-- unlinked long before retention existed, so they are correctly absent: no 11M-row UPDATE, and
-- no phantom multi-terabyte cache in the heartbeat.
CREATE TABLE IF NOT EXISTS cephor_ssd_residency (
    node_id     TEXT        NOT NULL,
    object_id   TEXT        NOT NULL,
    version     BIGINT      NOT NULL,
    part_number BIGINT      NOT NULL,
    resident_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    bytes       BIGINT      NOT NULL DEFAULT 0,
    PRIMARY KEY (node_id, object_id, version, part_number)
);

-- The evictor's worklist cursor and the heartbeat's cache_bytes sum. node_id leads so the
-- equality is an index condition; resident_at follows so the FIFO eviction order reads
-- straight off the index. This is what keeps eviction O(evicted) rather than O(resident) —
-- the janitor's walk-based eviction is readdir-bound at ~36 obj/s and starves its own tail,
-- and this table exists partly so that mistake is not repeated on the ingest tier.
CREATE INDEX IF NOT EXISTS cephor_ssd_residency_evict_idx
    ON cephor_ssd_residency (node_id, resident_at);

-- The peer resolver's access path: "which node holds THIS part on flash". The eviction index
-- above leads with node_id, so it cannot serve a lookup keyed on the part. This one runs on
-- the read path when a chunk misses locally, so it has to be an index lookup rather than a
-- scan of a table that grows to ~2M rows per node.
CREATE INDEX IF NOT EXISTS cephor_ssd_residency_part_idx
    ON cephor_ssd_residency (object_id, version, part_number);
