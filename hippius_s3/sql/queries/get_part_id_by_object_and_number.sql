-- Parameters: $1: object_id (UUID), $2: part_number (INT)
SELECT part_id
FROM parts
WHERE object_id = $1 AND part_number = $2
LIMIT 1;
