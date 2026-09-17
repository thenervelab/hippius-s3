-- A completed sweep is a successful verification, so it clears the failure counter and stamps both
-- timestamps exactly as a full recompute would -- the bucket has been checked end to end, just in
-- pieces. Without this an oversized bucket would keep its failure count and sit at the queue head.
--
-- It does NOT write bytes_used. A sweep MEASURES; only recompute_bucket_storage_usage() may SET the
-- counter, because only it does so inside the single snapshot that also discards the ledger rows its
-- aggregate already accounts for. Writing a smeared multi-cycle total here would corrupt the counter
-- with whatever landed mid-sweep.
UPDATE bucket_storage_usage
   SET recomputed_at = now(),
       attempted_at = now(),
       recompute_failures = 0
 WHERE bucket_id = $1
RETURNING bytes_used, churn_bytes
