-- migrate:up

-- BLAKE3 of the object plaintext, computed in flight on the write pipeline. Surfaced as the
-- "Arion hash" in ListObjects Owner.ID and in the console listings.
--
-- This deliberately does NOT reuse object_versions.ipfs_cid. That column is a dead relic of the
-- pre-Arion manifest architecture (last writer removed in 6beb93df, index dropped in
-- 20260528120000 as "nothing reads it"), but it is still read as a REAL CID by the destructive
-- ops scripts — nuke_user.py, purge_buckets.py, purge_source_versions.py,
-- cleanup_migration_versions.py, export_legacy_unpin_worklist.py — which do
-- `COALESCE(c.cid, ov.ipfs_cid)` guarded only by `IS NOT NULL / != '' / != 'pending'`. A 64-hex
-- BLAKE3 digest passes all three, so writing one there would feed plaintext digests into the
-- unpin worklist as though they were pins. Same reason cid_id stays NULL: it resolves through
-- the same COALESCE.
--
-- No index. Nothing looks objects up BY digest; it is projected alongside rows already being
-- read by key. An index here would only re-add the write amplification 20260528120000 removed.
ALTER TABLE object_versions ADD COLUMN body_blake3 text;

-- migrate:down

ALTER TABLE object_versions DROP COLUMN IF EXISTS body_blake3;
