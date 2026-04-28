SELECT bucket_id, bucket_name
FROM buckets
WHERE bucket_id = ANY($1::uuid[])
