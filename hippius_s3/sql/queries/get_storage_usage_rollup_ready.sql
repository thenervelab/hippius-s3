-- Whether the rollup has been seeded, i.e. whether its counters are totals rather than
-- deltas-since-migration.
--
-- The read path asks the same question inline (get_account_storage_bytes_rollup.sql returns it as
-- `ready`). The reconciler needs it on its own, because BEFORE the backfill every counter is 0
-- while ground truth is the bucket's whole contents -- so a recompute legitimately moves the number
-- by the bucket's full size, which is seeding and not drift. Reporting that as drift would fire
-- once per bucket across the estate, with a message blaming a write path, and would teach whoever
-- reads it to ignore the one alert this design actually depends on.
SELECT backfilled_at IS NOT NULL AS ready
FROM storage_usage_rollup_state
