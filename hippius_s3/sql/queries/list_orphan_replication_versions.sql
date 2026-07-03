-- A21 orphan sweep: object versions whose drain replication rows leaked and will churn
-- the drain forever. The abandoned-MPU reaper (list_abandoned_versions.sql) keys on
-- multipart_uploads, but an abort DELETEs that header row (and its parts) before the
-- best-effort terminal-mark — so an abort that dies in that window leaves cephor rows the
-- reaper can never see again. This query keys DIRECTLY on cephor_replication_status, so it
-- is the true backstop: it finds the leaked version from the drain rows alone.
--
-- A version is selected to be marked 'failed' iff ALL hold:
--   (a) it still has an ACTIVE drain row (status IN ('pending','draining')) — a
--       'replicated' row is legitimately done and a 'failed' row is already terminal, so
--       touching either would only churn the sweep;
--   (b) the version is UNSERVABLE — address IS NULL AND size_bytes<=0 AND md5_hash='' —
--       the exact download-servability predicate janitor_part_terminally_abandoned.sql
--       uses. The size/md5 clauses are NON-redundant with address IS NULL: address is
--       written AFTER size/md5 and in a separate step, so a fully-servable version briefly
--       has address=NULL (the mid-finalize window). The size/md5 guard is what keeps such
--       a version — or a servable simple-PUT whose part the drain's corruption path marked
--       active — from being swept and stranding a live GET. Do NOT "simplify" to address-only.
--   (c) its most-recently-landed part is older than the grace window (MAX(landed_at)) — the
--       last-activity valve. landed_at is stamped once when a part first lands and is never
--       bumped by drain re-claim/defer churn or the reconciler's node-only UPSERT, so it is
--       a true idle signal: a still-arriving upload keeps landing FRESH parts and is spared,
--       mirroring the reaper's per-upload "no part in the last N seconds" gate but sourced
--       purely from the drain rows (which survive the MPU-header delete). updated_at would
--       be useless here — the drain's own defer loop bumps it every poll, which IS the churn.
--
-- object_id is cast to uuid for the join (cephor stores it as text; object_versions keys on
-- uuid), matching the janitor query's cast. age_seconds is measured from the OLDEST part
-- (MIN landed_at) so the reaper reports the version's true replication lag. One row per
-- (object_id, version); the caller marks all of that version's active rows terminal.
-- Parameters: $1: stale_seconds (int) — the grace window.
SELECT crs.object_id,
       crs.version,
       EXTRACT(EPOCH FROM (now() - MIN(crs.landed_at)))::float8 AS age_seconds
FROM cephor_replication_status crs
JOIN object_versions ov
       ON ov.object_id = crs.object_id::uuid
      AND ov.object_version = crs.version
WHERE crs.status IN ('pending', 'draining')
  AND ov.address IS NULL
  AND ov.size_bytes <= 0
  AND COALESCE(ov.md5_hash, '') = ''
GROUP BY crs.object_id, crs.version
HAVING MAX(crs.landed_at) < now() - make_interval(secs => $1)
ORDER BY age_seconds DESC
LIMIT 2000
