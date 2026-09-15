-- Retire a storage backend that is no longer in the pinned set (STORAGE_BACKENDS).
--
-- Run ONCE against the PRIMARY, after the code that pins the set is rolled out and the
-- retired backend's workers are scaled to zero:
--
--   psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -v backend=<name> -f scripts/retire_backend.sql
--
-- Two things the retired backend leaves behind, both of which would otherwise block GC:
--
--   1. object_versions.upload_backends still lists it for every version written while it was
--      configured. The gates already intersect that list with the pinned set, so this is
--      hygiene — but a value nothing requires should not sit in 100M+ rows as if it did.
--   2. chunk_backend rows for it are still live (deleted = false). hard_delete_object.sql
--      requires ZERO live rows on ANY backend, so an object with a live row on a backend no
--      unpinner will ever service can never be hard-deleted; and nothing else soft-deletes
--      them now that the delete fan-out no longer names the backend. Soft-deleting them is
--      what the retired backend's unpinner would have done on the object's deletion; the
--      remote copies are abandoned with the backend.
--
-- Batched with a COMMIT per batch (a procedure, so the loop can commit): on prod this touches
-- ~176M version rows and the retired backend's whole chunk_backend set, and one statement
-- would hold locks and write a WAL burst for the duration. Idempotent: a re-run finds nothing.

\timing on
SET statement_timeout = '0';

CREATE OR REPLACE PROCEDURE pg_temp.retire_backend(retired text, batch int)
LANGUAGE plpgsql AS $$
DECLARE
    n bigint;
    total_versions bigint := 0;
    total_rows bigint := 0;
BEGIN
    LOOP
        WITH picked AS (
            SELECT object_id, object_version
            FROM object_versions
            WHERE upload_backends @> ARRAY[retired]
            LIMIT batch
            FOR UPDATE SKIP LOCKED
        )
        UPDATE object_versions ov
        SET upload_backends = array_remove(ov.upload_backends, retired)
        FROM picked
        WHERE ov.object_id = picked.object_id AND ov.object_version = picked.object_version;
        GET DIAGNOSTICS n = ROW_COUNT;
        total_versions := total_versions + n;
        COMMIT;
        EXIT WHEN n = 0;
    END LOOP;
    RAISE NOTICE 'object_versions rows cleared of %: %', retired, total_versions;

    LOOP
        WITH picked AS (
            SELECT chunk_id, backend
            FROM chunk_backend
            WHERE backend = retired AND NOT deleted
            LIMIT batch
            FOR UPDATE SKIP LOCKED
        )
        UPDATE chunk_backend cb
        SET deleted = true, deleted_at = now()
        FROM picked
        WHERE cb.chunk_id = picked.chunk_id AND cb.backend = picked.backend;
        GET DIAGNOSTICS n = ROW_COUNT;
        total_rows := total_rows + n;
        COMMIT;
        EXIT WHEN n = 0;
    END LOOP;
    RAISE NOTICE 'chunk_backend rows soft-deleted for %: %', retired, total_rows;
END;
$$;

CALL pg_temp.retire_backend(:'backend', 20000);

-- What is left: must be zero rows on both.
SELECT count(*) AS versions_still_listing_it FROM object_versions WHERE upload_backends @> ARRAY[:'backend'];
SELECT count(*) AS live_rows_still_on_it FROM chunk_backend WHERE backend = :'backend' AND NOT deleted;
