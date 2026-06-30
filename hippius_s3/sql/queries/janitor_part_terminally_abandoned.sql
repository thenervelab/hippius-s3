-- Is this CephFS-pool part SAFE for the janitor to reclaim as a terminally-abandoned
-- upload? Returns one row with `abandoned` = TRUE only when BOTH conditions hold:
--
--   (a) the drain's replication row is terminal-'failed' — the MPU reaper or an abort
--       marked it (fail_replication_status_for_version.sql). A 'failed' row is NEVER
--       re-claimed (claim_part filters status IN ('pending','draining')) and never
--       rewritten by the reconciler (record_landed's UPSERT only sets node_id), so the
--       drain will never touch this part's pool bytes again — they leak forever.
--
--   (b) the object version is UNSERVABLE — the api never wrote `address`, AND the GET
--       download filter `(size_bytes > 0 OR md5_hash <> '')` cannot be satisfied (an
--       incomplete/abandoned version is created with size_bytes=0, md5_hash='').
--
-- BOTH are mandatory. 'failed' alone is NOT sufficient: the drain's corruption-path
-- mark_failed has no servability guard and can mark a part of a *servable* simple-PUT
-- version 'failed'. Condition (b) — exactly the reaper's "abandoned upload" predicate
-- (address IS NULL) plus the literal download-servability filter — guarantees this can
-- never delete bytes a live GET could serve.
--
-- Params: $1 object_id (text; cast to uuid for object_versions), $2 object_version
-- (bigint), $3 part_number (bigint).
SELECT
  EXISTS (
    SELECT 1
    FROM cephor_replication_status crs
    WHERE crs.object_id = $1
      AND crs.version = $2
      AND crs.part_number = $3
      AND crs.status = 'failed'
  )
  AND EXISTS (
    SELECT 1
    FROM object_versions ov
    WHERE ov.object_id = $1::uuid
      AND ov.object_version = $2
      AND ov.address IS NULL
      AND ov.size_bytes <= 0
      AND COALESCE(ov.md5_hash, '') = ''
  ) AS abandoned
