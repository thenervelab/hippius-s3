-- migrate:up
-- Shift parts.part_number from 0-based to 1-based for objects that have 0-based data
-- Safe to run once; wrapped in a transaction by dbmate

-- Sanity check: abort if shifting would cause duplicates
DO $do$
BEGIN
  IF EXISTS (
    WITH zero_based AS (
      SELECT object_id
      FROM parts
      GROUP BY object_id
      HAVING MIN(part_number) = 0
    ),
    shifted AS (
      SELECT p.object_id, (p.part_number + 1) AS new_part_number
      FROM parts p
      JOIN zero_based z ON z.object_id = p.object_id
    )
    SELECT 1
    FROM shifted
    GROUP BY object_id, new_part_number
    HAVING COUNT(*) > 1
  ) THEN
    RAISE EXCEPTION 'Cannot shift parts to 1-based; collisions detected';
  END IF;
END
$do$ LANGUAGE plpgsql;

-- Perform the shift without violating constraints: drop, shift, reapply
ALTER TABLE parts DROP CONSTRAINT IF EXISTS parts_object_id_part_number_key;

WITH zero_based AS (
  SELECT object_id
  FROM parts
  GROUP BY object_id
  HAVING MIN(part_number) = 0
)
UPDATE parts p
SET part_number = p.part_number + 1
FROM zero_based z
WHERE p.object_id = z.object_id;

ALTER TABLE parts ADD CONSTRAINT parts_object_id_part_number_key UNIQUE (object_id, part_number);

-- migrate:down
-- Revert 1-based back to 0-based for objects that were shifted (those having min(part_number) = 1)
-- Sanity check for down shift
DO $do$
BEGIN
  IF EXISTS (
    WITH one_based AS (
      SELECT object_id
      FROM parts
      GROUP BY object_id
      HAVING MIN(part_number) = 1
    ),
    shifted AS (
      SELECT p.object_id, (p.part_number - 1) AS new_part_number
      FROM parts p
      JOIN one_based z ON z.object_id = p.object_id
    )
    SELECT 1
    FROM shifted
    GROUP BY object_id, new_part_number
    HAVING COUNT(*) > 1
  ) THEN
    RAISE EXCEPTION 'Cannot revert parts to 0-based; collisions detected';
  END IF;
END
$do$ LANGUAGE plpgsql;

ALTER TABLE parts DROP CONSTRAINT IF EXISTS parts_object_id_part_number_key;

WITH one_based AS (
  SELECT object_id
  FROM parts
  GROUP BY object_id
  HAVING MIN(part_number) = 1
)
UPDATE parts p
SET part_number = p.part_number - 1
FROM one_based z
WHERE p.object_id = z.object_id;

ALTER TABLE parts ADD CONSTRAINT parts_object_id_part_number_key UNIQUE (object_id, part_number);
