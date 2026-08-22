-- Set a bucket's versioning state.
-- Parameters: $1: bucket_id (uuid), $2: status ('Enabled' | 'Suspended')
UPDATE buckets
   SET versioning_status = $2
 WHERE bucket_id = $1
