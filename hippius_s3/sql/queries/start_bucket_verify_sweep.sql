-- Begin (or restart) a sliced verification sweep, capturing the churn watermark it will be judged
-- against. ON CONFLICT resets rather than errors so an abandoned sweep -- worker restarted mid-sweep,
-- cursor left partway -- is simply started again rather than resumed from state nobody can vouch for.
INSERT INTO bucket_storage_verify_state
    (bucket_id, cursor_key, partial_bytes, objects_scanned, slices_done, started_at, start_churn_bytes)
SELECT $1, '', 0, 0, 0, now(), COALESCE(bsu.churn_bytes, 0)
FROM buckets b
LEFT JOIN bucket_storage_usage bsu ON bsu.bucket_id = b.bucket_id
WHERE b.bucket_id = $1
ON CONFLICT (bucket_id) DO UPDATE
   SET cursor_key = '',
       partial_bytes = 0,
       objects_scanned = 0,
       slices_done = 0,
       started_at = now(),
       start_churn_bytes = EXCLUDED.start_churn_bytes
RETURNING start_churn_bytes
