-- Exponential defer backoff needs a per-row attempt counter: a part that keeps
-- deferring (in-progress MPU, missing address, vanished source) must back off
-- geometrically instead of re-entering the claim head every fixed interval
-- (the 2026-07-26 head-of-line starvation incident).
ALTER TABLE cephor_replication_status
    ADD COLUMN defer_attempts INTEGER NOT NULL DEFAULT 0;
