-- Aged pending/draining orphan gauge — the soak-gate feed for the A21 leak.
--
-- The 6h-soak gate (S14/R1) asserts only the `replicated`-on-SSD count, so it is BLIND to
-- A21 orphans, which are stuck in `pending`/`draining` (never reaching `replicated`). A
-- re-introduced leak (e.g. the abort-path bug list_orphan_replication_versions.sql exists to
-- catch) would therefore be invisible to the gate. This counts the standing population of
-- that leak so the soak gate can assert it is bounded and its slope is ~ 0.
--
-- It counts the EXACT population list_orphan_replication_versions.sql sweeps — an object
-- version that is (a) still ACTIVE on the drain (status IN ('pending','draining')),
-- (b) UNSERVABLE (the abandoned/leaked shape: address IS NULL AND size_bytes <= 0 AND
-- md5_hash = ''), and (c) idle past the grace (its most-recently-landed part older than
-- $1 seconds — landed_at is the true idle signal, never bumped by drain re-claim/defer
-- churn) — but returns a single count rather than a page to mark. The servability predicate
-- MUST stay in lockstep with janitor_part_terminally_abandoned.sql (and the Rust reclaim
-- gate's Store::servable_parts): all three encode the one definition of "servable".
--
-- Purely a SELECT: safe to run every janitor cycle. One row per (object_id, version) inside
-- the subquery, so the outer count is a version count (the unit the sweep and the leak alarm
-- both reason in), not a part count.
--
-- Parameter: $1 stale_seconds (int) — the idle grace window.
SELECT count(*)::bigint AS aged_pending_orphans
FROM (
    SELECT crs.object_id, crs.version
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
) AS aged
