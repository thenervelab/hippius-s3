-- Declare the rollup seeded. Until this lands, usage_service refuses to serve the rollup and the
-- plans-cacher keeps its previous roll -- see get_account_storage_bytes_rollup.sql.
--
-- Set by hippius_s3/scripts/backfill_bucket_storage_usage.py only after every bucket has been
-- recomputed, so a backfill that dies part way through leaves the flag unset and the previous
-- behaviour intact.
UPDATE storage_usage_rollup_state
   SET backfilled_at = now()
 WHERE singleton
RETURNING backfilled_at
