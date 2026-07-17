-- A21 orphan sweep: object versions whose drain replication rows leaked and will churn
-- the drain forever. The abandoned-MPU reaper (list_abandoned_versions.sql) keys on
-- multipart_uploads, but an abort DELETEs that header row (and its parts) before the
-- best-effort terminal-mark — so an abort that dies in that window leaves cephor rows the
-- reaper can never see again. This query keys DIRECTLY on cephor_replication_status, so it
-- is the true backstop: it finds the leaked version from the drain rows alone.
--
-- A version is selected to be marked 'failed' iff ALL hold:
--   (a) it still has a NON-TERMINAL, NOT-YET-UPLOADED drain row — status IN
--       ('pending','draining'), OR status='replicated' with upload_enqueued_at IS NULL. The
--       latter is the Tier-2 decoupled-commit class: a part that drained to the pool before its
--       object's address was written (an in-flight MPU) commits 'replicated' immediately, and
--       its backend upload is enqueued LATER by the enqueue sweep. If the object is then
--       ABANDONED the address never lands, so the part sits 'replicated' + unenqueued forever —
--       invisible to the old pending/draining-only sweep, its pool copy pinned by the janitor's
--       replication gate (no chunk_backend rows are ever written). Including it flips it 'failed'
--       so janitor_part_terminally_abandoned.sql can reclaim the pool copy. A 'replicated' row
--       that WAS enqueued (upload_enqueued_at set) means the address existed → the object
--       completed → legitimately done, so it is excluded; a 'failed' row is already terminal. The
--       version-level UNSERVABLE gate (b) still protects a servable version whose part is only
--       momentarily replicated+unenqueued (an inline enqueue that hit a transient Redis blip):
--       such a version can serve a GET, so it is never selected;
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
WHERE (crs.status IN ('pending', 'draining')
       OR (crs.status = 'replicated' AND crs.upload_enqueued_at IS NULL))
  AND ov.address IS NULL
  AND ov.size_bytes <= 0
  AND COALESCE(ov.md5_hash, '') = ''
GROUP BY crs.object_id, crs.version
HAVING MAX(crs.landed_at) < now() - make_interval(secs => $1)
ORDER BY age_seconds DESC
LIMIT 2000
