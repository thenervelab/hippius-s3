-- Terminal escalation for vanished SSD sources must count ONLY missing-source
-- observations: defer_attempts is shared with overdraft/not-ready deferrals, so
-- keying on it would let an unrelated backoff history fast-track a healthy part
-- to 'failed' (which is never resurrected) on its first transient NotFound.
ALTER TABLE cephor_replication_status
    ADD COLUMN missing_source_attempts INTEGER NOT NULL DEFAULT 0;
