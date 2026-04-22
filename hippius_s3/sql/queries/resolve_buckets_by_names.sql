SELECT bucket_name, bucket_id, main_account_id
FROM buckets
WHERE bucket_name = ANY($1)
