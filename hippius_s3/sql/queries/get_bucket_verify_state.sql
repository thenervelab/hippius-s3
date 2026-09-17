-- In-progress sliced verification for one bucket, plus the counter values the comparison needs.
--
-- One row joined from two tables so the sweep's start churn and the CURRENT churn come from the same
-- snapshot; read separately, a fold landing between the two reads would widen or narrow the tolerance
-- by an amount neither reader could account for.
SELECT
    vs.cursor_key,
    vs.partial_bytes,
    vs.objects_scanned,
    vs.slices_done,
    vs.started_at,
    vs.start_churn_bytes,
    COALESCE(bsu.bytes_used, 0) AS bytes_used,
    COALESCE(bsu.churn_bytes, 0) AS churn_bytes
FROM bucket_storage_verify_state vs
LEFT JOIN bucket_storage_usage bsu ON bsu.bucket_id = vs.bucket_id
WHERE vs.bucket_id = $1
