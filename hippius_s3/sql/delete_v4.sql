-- ============================================================================
-- Query 1: Delete all v4 object versions
-- ============================================================================
-- Delete all storage_version=4 object versions and their associated data.
-- v4 used legacy NaCl SecretBox encryption with per-bucket keys from the keystore DB.
-- Going forward, only v5 (AES-256-GCM envelope encryption via KMS) is supported.
--
-- Cascade behavior:
--   object_versions → parts (ON DELETE CASCADE)
--   parts → part_chunks (ON DELETE CASCADE)
--   part_chunks → chunk_backend (ON DELETE CASCADE)
--
-- Step 1: Point current_object_version away from v4 versions (for objects that have a v5 version)
-- Step 2: Delete v4 object_versions rows
-- Step 3: Delete orphaned objects (objects with no remaining versions)

BEGIN;

-- Objects whose current version is v4 but also have a v5 version: swap to latest v5
UPDATE objects o
SET current_object_version = sub.latest_v5
FROM (
    SELECT ov.object_id, MAX(ov.object_version) AS latest_v5
    FROM object_versions ov
    WHERE ov.storage_version = 5
      AND ov.kek_id IS NOT NULL
    GROUP BY ov.object_id
) sub
WHERE o.object_id = sub.object_id
  AND EXISTS (
    SELECT 1 FROM object_versions ov2
    WHERE ov2.object_id = o.object_id
      AND ov2.object_version = o.current_object_version
      AND ov2.storage_version = 4
  );

-- Delete all v4 object_versions (cascades to parts → part_chunks → chunk_backend)
DELETE FROM object_versions
WHERE storage_version = 4;

-- Delete objects that no longer have any versions
DELETE FROM objects o
WHERE NOT EXISTS (
    SELECT 1 FROM object_versions ov
    WHERE ov.object_id = o.object_id
);

COMMIT;


-- ============================================================================
-- Query 2: Delete broken v5 object versions (missing envelope metadata)
-- ============================================================================
-- Delete v5 object versions that are missing KMS envelope metadata (kek_id IS NULL).
-- These objects were created with storage_version=5 but never had their encryption
-- envelope properly configured (wrapped_dek and kek_id are NULL). They cannot be
-- downloaded — the reader raises "v5_missing_envelope_metadata" at read time.
-- This is a data integrity cleanup for objects that were never fully initialized.
--
-- Cascade behavior is the same as above.

BEGIN;

-- Objects whose current version is a broken v5 but also have a healthy v5 version: swap
UPDATE objects o
SET current_object_version = sub.latest_healthy
FROM (
    SELECT ov.object_id, MAX(ov.object_version) AS latest_healthy
    FROM object_versions ov
    WHERE ov.storage_version = 5
      AND ov.kek_id IS NOT NULL
    GROUP BY ov.object_id
) sub
WHERE o.object_id = sub.object_id
  AND EXISTS (
    SELECT 1 FROM object_versions ov2
    WHERE ov2.object_id = o.object_id
      AND ov2.object_version = o.current_object_version
      AND ov2.storage_version = 5
      AND ov2.kek_id IS NULL
  );

-- Delete broken v5 versions (storage_version=5 but no envelope encryption)
DELETE FROM object_versions
WHERE storage_version = 5
  AND kek_id IS NULL;

-- Delete objects that no longer have any versions
DELETE FROM objects o
WHERE NOT EXISTS (
    SELECT 1 FROM object_versions ov
    WHERE ov.object_id = o.object_id
);

COMMIT;
