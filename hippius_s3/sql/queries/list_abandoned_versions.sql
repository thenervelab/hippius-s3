-- Abandoned multipart uploads: a version whose parts landed but whose
-- object_versions.address was never written (the api writes it at PUT/MPU-complete),
-- initiated more than $1 seconds ago AND with no part uploaded in the last $1 seconds.
-- A completed or simple upload always has a non-NULL address, so it is never matched;
-- the NULL-address + age pair distinguishes a genuinely abandoned upload (the drain
-- would defer its enqueue as not-ready forever) from a still-in-flight one.
-- The last-activity gate is the safety valve: a user may initiate an MPU, upload parts
-- slowly, pause for days, then resume + complete. Keying on initiated_at alone would
-- reap that upload mid-flight; requiring NO part in the last $1 seconds means any part
-- upload resets the clock, so only a genuinely inactive upload is reaped.
-- is_completed=false is a belt-and-suspenders guard so a completed upload can't be
-- selected. Returns one row per (upload_id, object_id, object_version) to reap, plus
-- age_seconds (how long ago the upload was initiated) so the reaper can report its lag.
-- Parameters: $1: stale_seconds (int)
--
-- SHAPE IS LOAD-BEARING — pick the candidate uploads FIRST, then join their parts.
--
-- This selected the same rows by joining multipart_uploads to parts and ordering the
-- result by mu.upload_id. No index yields that ordering for this predicate, so rather
-- than sort, the planner drove the join from `parts` — scanning ~140M rows (cost 166M,
-- 4 parallel workers) because reading parts by (upload_id, uploaded_at) arrives
-- pre-sorted on upload_id — and applied the selective mu filters only after the join.
-- idx_multipart_uploads_initiated_at, which selects the candidates directly, went
-- unused. Measured in prod on 2026-07-23: a single execution ran for 96 MINUTES.
--
-- That is not merely slow. A 96-minute statement pins its snapshot, so the xmin horizon
-- stops advancing and VACUUM cannot reclaim ANY dead tuple newer than it, database-wide.
-- cephor_replication_status then sat at ~499k dead tuples through 429 autovacuum runs,
-- its hot partial indexes bloated to ~116k entries read per scan to return 28 live rows,
-- the drain-agent's 10s heartbeat query slowed to ~2s, replication fell behind, and a
-- cross-node read-after-write took 83s — long enough to time out a client at 60s.
--
-- Ordering by initiated_at instead lets idx_multipart_uploads_initiated_at supply the
-- order so LIMIT stops early, and materialising the candidates first bounds the parts
-- join to those 2000 uploads. Same rows, 96 minutes -> ~11s. Oldest-first is also the
-- order the reaper wants, since it reports the oldest reaped age as its lag.
WITH candidates AS MATERIALIZED (
    SELECT mu.upload_id, mu.initiated_at
    FROM multipart_uploads mu
    WHERE COALESCE(mu.is_completed, false) = false
      AND mu.initiated_at < now() - make_interval(secs => $1)
      -- Only uploads that actually have parts. The join below is an INNER join, so a
      -- partless upload can never be reaped — and prod carries 874k of them (97% of the
      -- incomplete backlog). Without this gate they are the OLDEST candidates, so they
      -- would fill every one of the 2000 slots and the reaper would reap nothing at all.
      AND EXISTS (
            SELECT 1 FROM parts pe
            WHERE pe.upload_id = mu.upload_id
          )
      AND NOT EXISTS (
            SELECT 1 FROM parts pr
            WHERE pr.upload_id = mu.upload_id
              AND pr.uploaded_at >= now() - make_interval(secs => $1)
          )
      -- The address gate, hoisted into the candidate set so it cannot be defeated by the
      -- LIMIT: filtering after the limit would let 2000 non-reapable uploads occupy every
      -- slot and stall the reaper on the same rows every cycle.
      AND NOT EXISTS (
            SELECT 1 FROM parts p2
            JOIN object_versions ov2
              ON ov2.object_id = p2.object_id AND ov2.object_version = p2.object_version
            WHERE p2.upload_id = mu.upload_id
              AND ov2.address IS NOT NULL
          )
    ORDER BY mu.initiated_at
    LIMIT 2000
)
SELECT DISTINCT c.upload_id, p.object_id, p.object_version,
       EXTRACT(EPOCH FROM (now() - c.initiated_at))::float8 AS age_seconds
FROM candidates c
JOIN parts p ON p.upload_id = c.upload_id
ORDER BY age_seconds DESC
