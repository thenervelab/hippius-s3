-- migrate:up
--
-- One live S3 name per (bucket_id, object_key). Promote of Harbor Copy+Delete
-- must steal dest from a soft-deleted objects row without minting a new
-- object_id (v5 AAD is bound to object_id).

CREATE OR REPLACE FUNCTION promote_object_name(p_bucket_id uuid, p_primary_key text)
RETURNS uuid
LANGUAGE plpgsql
AS $$
DECLARE
    v_object_id uuid;
    v_dest_key text;
    v_live uuid;
BEGIN
    SELECT o.object_id INTO v_object_id
    FROM objects o
    WHERE o.bucket_id = p_bucket_id
      AND o.object_key = p_primary_key
      AND o.deleted_at IS NULL;
    IF v_object_id IS NULL THEN
        RETURN NULL;
    END IF;

    SELECT n.object_key INTO v_dest_key
    FROM object_names n
    WHERE n.object_id = v_object_id
      AND n.bucket_id = p_bucket_id
    ORDER BY n.object_key
    LIMIT 1;
    IF v_dest_key IS NULL THEN
        RETURN NULL;
    END IF;

    SELECT o.object_id INTO v_live
    FROM objects o
    WHERE o.bucket_id = p_bucket_id
      AND o.object_key = v_dest_key
      AND o.deleted_at IS NULL
      AND o.object_id <> v_object_id;
    IF v_live IS NOT NULL THEN
        RAISE EXCEPTION 's3_name_conflict: live primary occupies %', v_dest_key
            USING ERRCODE = '23505';
    END IF;

    UPDATE objects
    SET object_key = '#deleted/' || object_id::text
    WHERE bucket_id = p_bucket_id
      AND object_key = v_dest_key
      AND deleted_at IS NOT NULL
      AND object_id <> v_object_id;

    UPDATE objects
    SET object_key = v_dest_key
    WHERE object_id = v_object_id;

    DELETE FROM object_names
    WHERE bucket_id = p_bucket_id
      AND object_key = v_dest_key;

    RETURN v_object_id;
END;
$$;

CREATE OR REPLACE FUNCTION reject_duplicate_live_s3_name()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_TABLE_NAME = 'object_names' THEN
        IF EXISTS (
            SELECT 1 FROM objects o
            WHERE o.bucket_id = NEW.bucket_id
              AND o.object_key = NEW.object_key
              AND o.deleted_at IS NULL
              AND o.object_id IS DISTINCT FROM NEW.object_id
        ) THEN
            RAISE EXCEPTION 'duplicate live s3 name'
                USING ERRCODE = '23505';
        END IF;
        RETURN NEW;
    END IF;

    IF NEW.deleted_at IS NULL AND EXISTS (
        SELECT 1 FROM object_names n
        WHERE n.bucket_id = NEW.bucket_id
          AND n.object_key = NEW.object_key
          AND n.object_id IS DISTINCT FROM NEW.object_id
    ) THEN
        RAISE EXCEPTION 'duplicate live s3 name'
            USING ERRCODE = '23505';
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS object_names_reject_duplicate_live ON object_names;
CREATE TRIGGER object_names_reject_duplicate_live
    BEFORE INSERT OR UPDATE OF bucket_id, object_key, object_id
    ON object_names
    FOR EACH ROW
    EXECUTE FUNCTION reject_duplicate_live_s3_name();

DROP TRIGGER IF EXISTS objects_reject_duplicate_live_name ON objects;
CREATE TRIGGER objects_reject_duplicate_live_name
    BEFORE INSERT OR UPDATE OF bucket_id, object_key, deleted_at
    ON objects
    FOR EACH ROW
    EXECUTE FUNCTION reject_duplicate_live_s3_name();

-- migrate:down

DROP TRIGGER IF EXISTS objects_reject_duplicate_live_name ON objects;
DROP TRIGGER IF EXISTS object_names_reject_duplicate_live ON object_names;
DROP FUNCTION IF EXISTS reject_duplicate_live_s3_name();
DROP FUNCTION IF EXISTS promote_object_name(uuid, text);
