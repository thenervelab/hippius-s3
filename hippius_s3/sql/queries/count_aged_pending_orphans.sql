-- Aged pending/draining orphan gauge — the soak-gate feed for the A21 leak.
--
-- The 6h-soak gate (S14/R1) asserts only the `replicated`-on-SSD count, so it is BLIND to
-- A21 orphans, which are stuck in `pending`/`draining` (never reaching `replicated`). A
-- re-introduced leak (e.g. the abort-path bug list_orphan_replication_versions.sql exists to
-- catch) would therefore be invisible to the gate. This counts the standing population of
-- that leak so the soak gate can assert it is bounded and its slope is ~ 0.
--
-- It counts the same population SHAPE list_orphan_replication_versions.sql sweeps — an object
-- version that is (a) still non-terminal + not-yet-uploaded on the drain (status IN
-- ('pending','draining') OR 'replicated' with upload_enqueued_at IS NULL — the Tier-2
-- decoupled-commit orphan the sweep also flips 'failed'),
-- (b) UNSERVABLE (the abandoned/leaked shape: address IS NULL AND size_bytes <= 0 AND
-- md5_hash = ''), (c) idle past the grace (its most-recently-landed part older than
-- $1 seconds — landed_at is the true idle signal, never bumped by drain re-claim/defer
-- churn), and (d) NOT DLQ-protected — the sweep skips any object_id parked in an upload/unpin
-- DLQ (`sweep_orphan_replication_versions`: `if str(object_id) in dlq_object_ids: continue`),
-- so a DLQ-parked orphan is one the sweep will never clear. Without $2 the pure-SQL gauge
-- would count it forever — a phantom backlog tripping the aged-pending-orphan-backlog alert on
-- data that is not a leak. NOTE on the grace: $1 is the gauge's short LEAK-VISIBILITY window
-- (default 1h), deliberately SMALLER than the sweep's 48h eligibility grace, so at any instant
-- this set is a strict SUPERSET of what the sweep is currently eligible to clear (it also counts
-- fresh 1h–48h orphans the sweep hasn't reached yet) — same shape, tighter grace, by design. The
-- servability predicate MUST stay in lockstep with janitor_part_terminally_abandoned.sql (and
-- the Rust reclaim gate's Store::servable_parts): all three encode the one definition of
-- "servable". It returns a single count rather than a page to mark.
--
-- Purely a SELECT: safe to run every janitor cycle. One row per (object_id, version) inside
-- the subquery, so the outer count is a version count (the unit the sweep and the leak alarm
-- both reason in), not a part count.
--
-- Parameters:
--   $1 stale_seconds (int) — the idle grace window. This is the gauge's LEAK-VISIBILITY window
--     (config.aged_orphan_gauge_grace_seconds), deliberately DECOUPLED from — and smaller than —
--     the reaper sweep's 48h eligibility grace so a leak re-introduced during a short soak
--     registers instead of hiding below the reaper's window.
--   $2 dlq_object_ids (text[]) — object_ids currently parked in any DLQ, built the SAME way the
--     reaper builds its set (str(object_id) form). Matches crs.object_id (stored as text) so the
--     exclusion is byte-for-byte the reaper's `str(object_id) in dlq_object_ids`. An empty array
--     excludes nothing (ANY over an empty array is false), so a healthy stack with no DLQ entries
--     counts the full leak population.
SELECT count(*)::bigint AS aged_pending_orphans
FROM (
    SELECT crs.object_id, crs.version
    FROM cephor_replication_status crs
    JOIN object_versions ov
           ON ov.object_id = crs.object_id::uuid
          AND ov.object_version = crs.version
    WHERE (crs.status IN ('pending', 'draining')
           OR (crs.status = 'replicated' AND crs.upload_enqueued_at IS NULL))
      AND ov.address IS NULL
      AND ov.size_bytes <= 0
      AND COALESCE(ov.md5_hash, '') = ''
      AND NOT (crs.object_id = ANY($2::text[]))
    GROUP BY crs.object_id, crs.version
    HAVING MAX(crs.landed_at) < now() - make_interval(secs => $1)
) AS aged
