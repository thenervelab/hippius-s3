-- Live bucket ids for an account. Step 1 of the chunked storage count in usage_service.py.
--
-- Cheap and index-driven (idx_buckets_main_account): a few hundred rows at most, milliseconds.
-- Split out from the count itself deliberately -- when the account id reaches the planner as a
-- LITERAL it uses that index (~5 buffers), but reached through a JOIN it fell back to a parallel
-- scan over all ~46k buckets with a filter (~47k buffers), because n_distinct on objects.bucket_id
-- is badly wrong. See 20260909120000_fix_objects_bucket_id_n_distinct.sql.
--
-- Parameters: $1: main_account_id (SS58)
SELECT bucket_id
FROM buckets
WHERE main_account_id = $1
  AND deleted_at IS NULL
ORDER BY bucket_id
