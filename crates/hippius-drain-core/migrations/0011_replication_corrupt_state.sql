-- R4: a distinct `corrupt` replication state for a live, servable object whose pool copy
-- is corrupt (a persistent ChunkMismatch on drain), so its SSD copy is the last good source.
--
-- Before this, such a part was marked `failed` — the same terminal state as an abandoned
-- upload — so the reclaim worker could not tell "safe to delete debris" from "the only good
-- copy of a live object". #235 gated the reclaim on servability as a stopgap; this makes the
-- distinction a first-class state so it is queryable, alertable, and recoverable. A `corrupt`
-- part is NEVER reclaimed and is re-driven (reset to `pending` for a fresh SSD->pool copy that
-- overwrites the corrupt pool copy) up to a bounded number of attempts, after which it is held
-- and paged. `corrupt` is therefore NOT terminal (it can transition back to `pending`), unlike
-- `replicated`/`failed`.
ALTER TABLE cephor_replication_status DROP CONSTRAINT IF EXISTS cephor_replication_status_status_check;
ALTER TABLE cephor_replication_status ADD CONSTRAINT cephor_replication_status_status_check
    CHECK (status IN ('pending', 'draining', 'replicated', 'failed', 'corrupt'));

-- Bounded re-drive counter: how many times this part has been re-driven out of `corrupt`.
-- The re-drive worker resets `corrupt` -> `pending` only while this is below the cap, then
-- holds the part `corrupt` and pages — so a persistently-unrecoverable pool copy cannot loop
-- forever. Incremented on each reset; never reset itself (a genuine recovery leaves `corrupt`
-- for `replicated`, so the counter's history is moot once the part is durable).
ALTER TABLE cephor_replication_status ADD COLUMN corrupt_attempts INTEGER NOT NULL DEFAULT 0;

-- The re-drive scan and the corrupt-backlog gauge both filter status='corrupt' per node; a
-- partial index keeps that off the full table, mirroring the pending/draining hot-path indexes.
CREATE INDEX cephor_replication_status_corrupt
    ON cephor_replication_status (node_id)
    WHERE status = 'corrupt';
