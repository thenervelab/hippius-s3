-- $1 part_id (UUID), $2 chunk_indexes INT[], $3 cipher_sizes BIGINT[]
--
-- Bring part_chunks into line with the attempt that just won, in one statement.
--
-- A re-upload of a part KEEPS its part_id (`parts` is upserted in place with the new size and
-- etag), while these rows were only ever inserted. So both halves below are about an attempt
-- that lands on top of an earlier one:
--
--   DO UPDATE, not DO NOTHING — the earlier attempt's rows are what a bare insert collides with,
--   and leaving them meant the row described the PREVIOUS attempt's ciphertext while the disk
--   held the current one's. Invisible whenever the two encrypt to the same length (as
--   equal-length plaintexts do) and wrong for every re-upload that does not.
--
--   The DELETE — a SHORTER attempt leaves surplus rows for chunks that are no longer on disk and
--   no longer part of the object (a 9 MiB part re-uploaded at 5 MiB leaves a third row). Nothing
--   ever backs those, and `janitor_evictable_candidates` admits a part only when EVERY
--   part_chunks row has a live chunk_backend row — so one surplus row pins the part on the
--   ingest SSD permanently and keeps it in `find_underreplicated_live_chunks` forever. Reads are
--   unaffected because they plan from parts/meta, which is why the only symptom is disk that
--   never comes back.
--
-- A surplus row whose ciphertext ALREADY reached a backend is deliberately spared.
-- chunk_backend.chunk_id is ON DELETE CASCADE, so removing the row would take the
-- backend_identifier with it and strand that object on the backend with nothing left to name it
-- for the unpinner — and UnpinChainRequest is object/version-scoped, so no request can ask for a
-- single chunk back. Such a row keeps pinning the part, which is the pre-existing behaviour and
-- strictly better than an unreclaimable remote object.
--
-- The two halves do not interact: a data-modifying CTE and the outer statement share one
-- snapshot, so the DELETE cannot see the CTE's rows — and it does not need to, since it only
-- considers indexes at or beyond the new chunk count, which the INSERT never writes.
WITH upserted AS (
    INSERT INTO part_chunks (part_id, chunk_index, cipher_size_bytes)
    SELECT $1, t.chunk_index, t.cipher_size_bytes
    FROM unnest($2::int[], $3::bigint[]) AS t(chunk_index, cipher_size_bytes)
    ON CONFLICT (part_id, chunk_index) DO UPDATE SET cipher_size_bytes = EXCLUDED.cipher_size_bytes
    RETURNING 1
)
DELETE FROM part_chunks pc
WHERE pc.part_id = $1
  AND pc.chunk_index >= cardinality($2::int[])
  AND NOT EXISTS (
      SELECT 1 FROM chunk_backend cb
      WHERE cb.chunk_id = pc.id AND NOT cb.deleted
  );
