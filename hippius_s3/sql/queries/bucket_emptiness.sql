-- What stops DeleteBucket: live versions / delete markers, and open uploads. Run it inside the
-- transaction that holds lock_bucket_for_delete, so both answers are taken after every in-flight
-- insert that references the bucket has committed.
--
-- has_versions replaced a `list_objects` probe, which only saw keys whose newest version is live
-- content. A versioned bucket whose keys were all hidden behind delete markers read as empty, so
-- DeleteBucket soft-deleted it and orphaned every non-current version under it — Object-Locked
-- ones included. It now follows AWS: every version AND every delete marker must be gone.
--
-- The serveable predicate is the one list_objects uses. It skips the zero-byte reserved row an
-- aborted multipart upload leaves on a brand-new key, which no client can see or delete and would
-- otherwise wedge the bucket forever — even when the upload locked it at initiate, since a lock on
-- a row with no data protects nothing. A multipart upload still in progress is caught by
-- has_open_uploads instead. A simple PUT that is still streaming is visible to neither arm — its
-- upload row and serveable version appear together in its final transaction — which is the
-- residual race documented in bucket_delete_endpoint.py.
--
-- No arm for locked versions under a soft-deleted object. No API path leaves one — an unversioned
-- DELETE and a versioned DELETE of a locked version are both refused — only the purger and the ops
-- scripts can, and neither goes through DeleteBucket. The SQL gates keep those bytes regardless.
-- Such an arm would also have to walk every soft-deleted object of a freshly emptied bucket, under
-- the API statement timeout.
--
-- Both arms are EXISTS probes on the bucket's own index ranges (idx_objects_bucket_prefix_active,
-- idx_mpu_bucket_key_incomplete), so a non-empty bucket answers from its first rows.
--
-- Parameters: $1: bucket_id (uuid)
SELECT EXISTS (
           SELECT 1
           FROM objects o
           JOIN object_versions ov ON ov.object_id = o.object_id
           WHERE o.bucket_id = $1
             AND o.deleted_at IS NULL
             AND ov.deleted_at IS NULL
             AND (ov.is_delete_marker OR ov.size_bytes > 0 OR (ov.md5_hash IS NOT NULL AND ov.md5_hash != ''))
       ) AS has_versions,
       EXISTS (
           SELECT 1
           FROM multipart_uploads mu
           WHERE mu.bucket_id = $1
             AND mu.is_completed = FALSE
       ) AS has_open_uploads
