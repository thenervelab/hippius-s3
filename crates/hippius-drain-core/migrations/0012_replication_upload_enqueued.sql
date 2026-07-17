-- Decouple the Ceph-commit from the backend-upload enqueue.
--
-- Before this, drain_part enqueued the backend UploadChainRequest (which needs
-- object_versions.address) BEFORE committing 'replicated', so an in-flight MPU part
-- (address NULL until CompleteMultipartUpload) deferred and RE-COPIED to the pool on
-- every poll until the object completed. Now drain_part commits 'replicated' as soon as
-- the verified copy is durable on the pool, and a separate enqueue sweep publishes the
-- backend upload once the address lands. This column records that publish so the sweep
-- knows what is still outstanding.
--
-- upload_enqueued_at:
--   NULL      -> replicated on Ceph but the backend upload has NOT been enqueued yet
--               (an in-flight/abandoned MPU whose address is not written, or a transient
--               enqueue miss the sweep will retry).
--   timestamp -> the backend UploadChainRequest was published to the {backend}_upload_requests
--               queues; the sweep is done with this part.
--
-- Only meaningful for status='replicated'. Terminal 'failed'/'corrupt' and live
-- 'pending'/'draining' rows leave it NULL.
ALTER TABLE cephor_replication_status ADD COLUMN upload_enqueued_at TIMESTAMPTZ;

-- The enqueue sweep's worklist: this node's replicated parts still awaiting a backend
-- enqueue, oldest-committed first. A partial index keyed on the exact predicate keeps the
-- sweep's scan proportional to the small outstanding set, never the full table.
CREATE INDEX cephor_replication_unenqueued_idx
    ON cephor_replication_status (node_id, updated_at)
    WHERE status = 'replicated' AND upload_enqueued_at IS NULL;
