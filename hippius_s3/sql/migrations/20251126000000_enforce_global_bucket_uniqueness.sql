-- migrate:up

-- This migration restores global bucket name uniqueness as required by S3 spec.
-- Any buckets with duplicate names across accounts will be automatically renamed.
-- The oldest bucket keeps its name, newer ones get renamed to "<bucket>-<timestamp>".

-- Rename conflicting buckets (keep oldest, rename newer ones)
DO $$
DECLARE
    bucket_record RECORD;
    new_name TEXT;
    timestamp_suffix TEXT;
BEGIN
    FOR bucket_record IN
        SELECT
            b.bucket_id,
            b.bucket_name,
            b.created_at,
            b.main_account_id,
            ROW_NUMBER() OVER (PARTITION BY b.bucket_name ORDER BY b.created_at ASC) as rn
        FROM buckets b
        WHERE b.bucket_name IN (
            SELECT bucket_name
            FROM buckets
            GROUP BY bucket_name
            HAVING COUNT(DISTINCT main_account_id) > 1
        )
    LOOP
        IF bucket_record.rn > 1 THEN
            timestamp_suffix := TO_CHAR(bucket_record.created_at, 'YYYYMMDDHH24MISS');
            new_name := bucket_record.bucket_name || '-' || timestamp_suffix;

            RAISE NOTICE 'Renaming bucket % (account: %, created: %) to %',
                bucket_record.bucket_name,
                bucket_record.main_account_id,
                bucket_record.created_at,
                new_name;

            UPDATE buckets
            SET bucket_name = new_name
            WHERE bucket_id = bucket_record.bucket_id;
        END IF;
    END LOOP;
END $$;

-- Drop the composite unique constraint
ALTER TABLE buckets DROP CONSTRAINT buckets_name_owner_unique;

-- Add global unique constraint on bucket_name
ALTER TABLE buckets ADD CONSTRAINT buckets_bucket_name_key UNIQUE(bucket_name);

-- Keep the index on (bucket_name, main_account_id) for efficient owner lookups
-- The idx_buckets_name_owner index already exists and is still useful

-- migrate:down

-- Drop the global unique constraint
ALTER TABLE buckets DROP CONSTRAINT buckets_bucket_name_key;

-- Re-add the composite unique constraint (user-scoped bucket names)
ALTER TABLE buckets ADD CONSTRAINT buckets_name_owner_unique UNIQUE(bucket_name, main_account_id);
