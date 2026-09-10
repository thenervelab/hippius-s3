-- Total billable bytes stored by an account. THE definition of "storage used" in this codebase.
--
-- O(number of objects the account owns): measured ~90-190ms for a 1k-object account and ~0.2-1.0s
-- for a 1M-object account, depending on the plan the planner picks. The largest prod account holds
-- 11.8M objects in one bucket, where this is seconds and admin.py already degrades to a null count
-- on timeout. So it must never run on a routine request path.
--
-- NO RUNTIME CALLER, AND THAT IS DELIBERATE -- DO NOT DELETE THIS AS DEAD SQL. The plans-cacher
-- walks each bucket in keyset pages instead (get_bucket_storage_bytes_page.sql), because this form
-- cannot finish for a bucket of millions of objects inside the 30s ceilings that apply. This query
-- survives as the CANONICAL DEFINITION, and as the oracle the chunked walk is asserted against case
-- by case in tests/integration/test_usage_service_chunked.py. Delete it and the chunked path has
-- nothing independent left to be checked for correctness against.
--
-- Nothing on the request path runs either form -- the quota gate reads the cached result and does
-- not re-check, not even to confirm a denial, which is why the refresh interval is the enforcement
-- lag in both directions.
--
-- Keep in sync with get_admin_account_stats.sql and console_list_buckets.sql -- those two disagreed
-- with each other until 2026-09, and they are the numbers an operator and a customer each see.
--
-- Parameters: $1: main_account_id (SS58)
SELECT COALESCE(SUM(ov.size_bytes), 0)::bigint AS bytes_used
FROM buckets b
JOIN objects o
  ON o.bucket_id = b.bucket_id
 AND o.deleted_at IS NULL
JOIN object_versions ov
  ON ov.object_id = o.object_id
 AND ov.object_version = o.current_object_version
 AND ov.deleted_at IS NULL
 AND NOT ov.is_delete_marker
WHERE b.main_account_id = $1
  AND b.deleted_at IS NULL
