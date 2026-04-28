INSERT INTO sub_token_scopes (access_key_id, account_id, permission, bucket_scope, bucket_ids)
VALUES ($1, $2, $3, $4, $5::uuid[])
ON CONFLICT (access_key_id)
DO UPDATE SET
    account_id   = EXCLUDED.account_id,
    permission   = EXCLUDED.permission,
    bucket_scope = EXCLUDED.bucket_scope,
    bucket_ids   = EXCLUDED.bucket_ids,
    updated_at   = NOW()
RETURNING access_key_id, account_id, permission, bucket_scope, bucket_ids, created_at, updated_at
