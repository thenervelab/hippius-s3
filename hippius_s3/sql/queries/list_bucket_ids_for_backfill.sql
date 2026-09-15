-- One keyset page of bucket ids for the one-shot backfill.
--
-- EVERY bucket, including soft-deleted ones, unlike the reconciler's queue: the backfill is the
-- single moment at which every counter in the table is established, and a bucket left unseeded
-- would sit at "deltas since the migration" with nothing to ever correct it. The reconciler can
-- afford to skip soft-deleted buckets afterwards because their totals are read by nobody.
--
-- Keyset on the primary key rather than OFFSET so a resumed run does not re-walk its prefix.
--
-- Parameters: $1: bucket_id cursor (exclusive; pass the all-zero uuid to start), $2: page size
SELECT bucket_id
FROM buckets
WHERE bucket_id > $1
ORDER BY bucket_id
LIMIT $2
