-- Drop sweep state. Called when a sweep completes, so the table stays at roughly the number of
-- buckets currently too large to aggregate in one statement (2 on prod).
DELETE FROM bucket_storage_verify_state WHERE bucket_id = $1
