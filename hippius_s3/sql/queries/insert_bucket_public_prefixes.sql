-- Replace-set helper. The caller deletes this bucket's rows in the same transaction.
-- Parameters: $1: bucket_id, $2: prefixes text[]
INSERT INTO bucket_public_prefixes (bucket_id, prefix)
SELECT $1, prefix
FROM unnest($2::text[]) AS prefix
