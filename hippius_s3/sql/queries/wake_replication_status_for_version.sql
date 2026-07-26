-- Wake one completed version's still-pending replication rows out of drain defer backoff.
-- Parts of an in-progress MPU cannot finish draining (upload enqueue not ready — the object
-- address is written only at complete) and accumulate exponential defer backoff, up to the
-- cap. CompleteMultipartUpload is the wake signal: clearing deferred_until lets the drain
-- claim the parts on its next poll instead of waiting out up to the full cap.
--
-- defer_attempts is reset too — deliberately unlike the drain's release_part (Rust), which
-- preserves escalation across deferrals. Completion removes the CAUSE of these deferrals
-- (the unfinalized address), so the escalation history is obsolete; preserving it would
-- re-escalate a now-healthy part's first transient hiccup straight back toward the cap.
--
-- Only 'pending' rows: 'failed' is terminal by design and 'replicated'/'draining' rows are
-- past the defer gate — resurrecting or touching them here would fight the drain's own
-- state machine. Accepted residual race: a part mid-claim that read the address as NULL
-- just before the write defers once more with its preserved near-cap attempts — bounded
-- to one extra backoff interval for at most claim-slot-count parts.
-- Parameters: $1: object_id (text), $2: object_version (bigint)
UPDATE cephor_replication_status
SET deferred_until = NULL, defer_attempts = 0, updated_at = now()
WHERE object_id = $1 AND version = $2 AND status = 'pending'
