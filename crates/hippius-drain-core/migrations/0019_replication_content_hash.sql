-- B-2: record WHAT the drain committed, so a part rewritten after its commit can be detected.
--
-- THE BUG. An S3 client may `UploadPart` the same part number twice with DIFFERENT bytes before
-- `CompleteMultipartUpload` — legal S3, and also the shape of a late retry after a perceived
-- timeout. The retry reuses the same object_version, so (object_id, version, part_number) is
-- unchanged: attempt two overwrites attempt one's SSD bytes under a row that already reads
-- 'replicated'. `record_landed_part`'s conflict action only ever filled a NULL node_id, the
-- reconciler tallies a replicated part as an orphan and leaves it alone, and the api's
-- wake_version_replication only touches rows still 'pending' — so nothing re-drove it. The pool
-- kept attempt one's ciphertext while the SSD held attempt two's.
--
-- It is SILENT. The DEK is per object_version and the AEAD AAD binds (bucket_id, object_id,
-- part_number, chunk_index), all preserved by the retry, so the stale ciphertext decrypts and
-- authenticates cleanly. A reader served from the pool gets the wrong plaintext under the right
-- ETag, with no error anywhere.
--
-- content_sha256:
--   the lowercase-hex SHA-256 fold of the part's per-chunk hashes, in ascending chunk index,
--   with the chunk count and each hash length bound in (so a truncated chunk set cannot alias
--   the full one). Written by the SAME statement as status='replicated', never as a follow-up,
--   so no part is ever committed with a stale or absent digest.
--   NULL -> committed before this shipped, or not yet committed. On the re-landing check NULL
--           means "cannot compare", which is treated as diverged (fail-safe) — never as "fine".
--
-- WHY THE DIGEST IS FREE. drain_part already streams every chunk through SHA-256 to byte-verify
-- its pool copy; this stores the fold of hashes it had already computed. Deriving the digest to
-- COMPARE against costs a full SSD read, which is why it runs only on a landed announcement
-- naming an already-'replicated' part — a rewrite by construction, and rare.
--
-- COST. 64 hex chars + 1 length byte per row against ~11.4M rows is ~750 MB of heap on this
-- table. Deliberately text rather than bytea: every hash on both sides of the comparison is
-- already lowercase hex (PartPool::persist_chunk, LocalSsd::chunk_hash), and a durability
-- column that can be read straight out of psql during an incident is worth the 2x.
--
-- NO BACKFILL, and no index. Backfilling would mean re-reading every node's whole ~930 GB shard
-- to compute a digest for parts that will never be re-landed; the NULL semantics above make that
-- unnecessary. The column is only ever read by primary-key lookup (record_landed_part's
-- RETURNING) and written by mark_replicated, so no index earns its keep.

-- lock_timeout is the load-bearing line, exactly as in 0013 and 0018. ADD COLUMN without a
-- default is metadata-only and fast once it HOLDS the lock, but it still needs ACCESS EXCLUSIVE
-- to take it, and that acquisition queues behind any open reader and then blocks every statement
-- arriving after it — claim_part, mark_replicated, record_landed_part, i.e. the whole drain
-- fleet. migrate() runs in the allocator's startup path BEFORE its liveness file is first
-- touched, and that probe SIGKILLs at ~50-65s, so a long lock wait means CrashLoopBackOff with
-- each restart re-queuing the lock. Failing fast and retrying on the next start is better.
SET LOCAL lock_timeout = '5s';

ALTER TABLE cephor_replication_status
    ADD COLUMN IF NOT EXISTS content_sha256 TEXT;
