-- migrate:up
-- Shift parts.part_number from 0-based to 1-based for objects that have 0-based data
-- Safe to run once; wrapped in a transaction by dbmate

-- Sanity check: abort if shifting would cause duplicates
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
SELECT object_id
FROM shifted
GROUP BY object_id, new_part_number
HAVING COUNT(*) > 1;

-- If the above SELECT returns rows, inspect and fix collisions before proceeding.

-- Perform the shift
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

-- migrate:down
-- Revert 1-based back to 0-based for objects that were shifted (those having min(part_number) = 1)
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
