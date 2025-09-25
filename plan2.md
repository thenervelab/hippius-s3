# Reliability Plan v2 (DLQ + SDK Publish Replacement)

## Goals

- Improve end-to-end reliability of pinning and publishing
- Add a Dead-Letter Queue (DLQ) with safe requeue tooling
- Reduce dependency on flaky SDK substrate connections with a resilient adapter or inline publish

---

## 1) Dead-Letter Queue (DLQ) + Requeue CLI

### Problem

- Transient and permanent failures are mixed in the primary queue
- Hard failures can stall progress and are difficult to triage

### Design

- DLQ storage: Redis list `upload_requests:dlq` storing JSON with:
  - full `UploadChainRequest` payload
  - object_id, upload_id, bucket_name, object_key
  - attempts, first_enqueued_at, last_attempt_at
  - last_error (string), error_type (transient|permanent|unknown)
- Worker behavior:
  - If attempts >= max OR error classified permanent → push to DLQ
  - Else → retry with exponential backoff + jitter

### Persistence of chunk bytes (filesystem-only)

- When pushing to DLQ, write chunk bytes to disk at `DLQ_DIR/{object_id}/part_{n}`
- Do not write a separate entry.json; object DB + on-disk parts are sufficient
- No encryption, no TTL; after successful requeue, MOVE directory to `DLQ_ARCHIVE/{object_id}` (manual deletion later)
- During requeue, DLQ code rehydrates Redis cache from disk just-in-time before enqueueing

### Requeue CLI

- Script: `hippius_s3/scripts/dlq_requeue.py`
  - `peek --limit N` → list DLQ entries
  - `requeue --object-id <id>` → re-enqueue matching entries to `upload_requests` (reset attempts / annotate)
  - `purge --object-id <id>` or `--all` → remove
  - `export --file dlq.json` / `import --file dlq.json`
- Safety:
  - Per-object distributed lock during requeue
  - `--force` required to requeue permanent errors
  - Rehydrate cache from `DLQ_DIR/{object_id}` just before re-enqueue
  - After requeue succeeds, MOVE `DLQ_DIR/{object_id}` → `DLQ_ARCHIVE/{object_id}` (retain for audit/manual cleanup)

### Classification heuristics (initial)

- Permanent: malformed payloads, negative sizes, missing cache bytes after N scans
- Transient: timeouts, 5xx from IPFS/gateway, substrate SDK errors
- Unknown → retry up to cap, then DLQ

### Testability and module boundaries

- Extract DLQ helpers into `hippius_s3/dlq/storage.py` (filesystem ops) and `hippius_s3/dlq/logic.py` (rehydrate + enqueue)
- CLI imports the same logic to avoid drift; tests call these helpers directly

### E2E test implementation (DLQ path)

**File**: `tests/e2e/test_DLQ_Requeue.py` (tagged `@pytest.mark.local`)

1. Create multipart upload with 2 parts (5MB each)
2. Wait for initial processing to complete
3. Monkeypatch `IPFSService.upload_file` to fail first call, succeed subsequent calls
4. Complete multipart upload (fails internally, triggers DLQ)
5. Verify object marked as "failed" in database
6. Verify DLQ entry exists in Redis with transient error type
7. Verify chunks persisted to `DLQ_DIR/{object_id}/part_{n}`
8. Clear Redis cache for the object
9. Run CLI `requeue --object-id <id>` (hydrates cache from disk, requeues)
10. Verify successful completion and data integrity
11. Verify DLQ dir moved to `DLQ_ARCHIVE/{object_id}`

### Fault injection approach

- **Chosen**: Monkeypatch `IPFSService.upload_file` to fail first call, succeed on retry/requeue
- **Benefits**: Deterministic, fast, doesn't affect other tests, easy to implement
- **Alternative options** (documented for future use):
  - Container-based: `docker compose pause ipfs` or network disconnect
  - Proxy-based: `toxiproxy` sidecar with toxic rules

---

## 2) SDK Publish Replacement (or Resilient Adapter)

### Problem

- SDK maintains a substrate connection that can break and require restart
- Publish path depends on SDK stability

### Options

1. Resilient SDK Adapter (low-risk)

- Wrap SDK publish with:
  - health check + auto-reconnect per call
  - bounded retries with jitter/timeouts
  - fallback to direct IPFS upload+pin after K failures
- Record path used (sdk|fallback) for observability

2. Inline Publish (medium-risk)

- Implement minimal publish inside service:
  - derive key/session (seed phrase + subaccount)
  - assemble manifest payload; sign and submit extrinsic
  - track inclusion/finality; on success, update object CID
- Considerations: pallet/tx shape, finality requirement, backward-compat

### Recommendation

- Phase 1: Resilient adapter now (fast win)
- Phase 2: Spike inline publish behind `HIPPIUS_INLINE_PUBLISH=1`, dark launch in staging; compare latency/reliability

---

## 3) Background Reconciliation Worker

- Periodically scan and repair drift:
  - parts with `ipfs_cid='pending'` but cache bytes exist → re-enqueue
  - all parts CIDed but no manifest → build/publish
  - manifests not recursively pinned → re-pin
  - verify pins across endpoints; re-pin missing
- Rate-limit to avoid overload; structured logs per object

---

## 4) IPFS Multi-Endpoint Failover

- Configure multiple IPFS API endpoints
- Retry across endpoints; mark health with cool-down
- Prefer primary; promote backup on sustained failures

---

## 5) Observability & Alerts

- Metrics:
  - upload_success/failure, retries, dlq_count
  - publish_success/failure, sdk_publish_fallback_count
  - pin_verify_failures, repin_success
- Logs: structured by object_id, upload_id, part_number, path(sdk|fallback)
- Alerts: DLQ growth rate, consecutive publish failures, fallback-only mode

---

## 6) Rollout Plan

- A: DLQ + CLI + metrics
- B: SDK adapter with fallback
- C: Staging chaos (drop IPFS/SDK connectivity) and verify recovery
- D: Inline publish spike behind flag; compare
- E: Enable inline publish in staging → production

---

## Open Questions

- Pallets/tx formats to mirror SDK publish
- Inclusion vs finalized finality for publish
- Per-tenant rate limits for requeue/reconciliation
