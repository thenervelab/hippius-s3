# Hippius S3 – Staged Plan

## Goals

- Reduce complexity by unifying queue payloads and worker code paths
- Make the worker a reliable uploader (chunks → IPFS + DB), with manifest build/publish as a separate, clear step
- Keep e2e tests stable and representative (no reliance on a real chain)
- Avoid user‑visible API changes during early steps

## Scope (this plan)

- Queue payload unification (call sites + worker normalization)
- Uploader (chunks) consolidation and idempotent parts upsert
- Manifest build + publish/pin (manifest CID on object row)
- Test environment alignment (host IPFS URLs)
- Deferred: directory-per-object layout, DAG root pin, envelope encryption ADR

## Non‑Goals (now)

- Changing public S3 API, bucket/object semantics, or auth
- Removing existing types immediately (we’ll deprecate after migration)
- Introducing a new on-chain workflow

---

## Step 1: Unify upload call sites (no behavior change)

- Problem: Two payload types (SimpleUploadChainRequest, MultipartUploadChainRequest) force branching everywhere.
- Plan:
  - Define a unified UploadChainRequest (conceptual) with fields:
    - object_id, bucket_name, object_key
    - upload_id (optional)
    - chunks: Chunk[] (for simple: [{ id: 0 }])
    - should_encrypt, subaccount, subaccount_seed_phrase
    - substrate_url, ipfs_node
    - request_id, attempts (retry metadata)
  - Add adapters at enqueue points:
    - Simple → Unified(chunks=[chunk])
    - Multipart → Unified(chunks=payload.chunks, upload_id=payload.multipart_upload_id)
  - Worker continues accepting both forms, but normalizes at the top into a single shape.
- Success Criteria:
  - One codepath in worker for chunk processing
  - No changes in external behavior or responses

## Step 2: Uploader consolidation (chunks only)

- Problem: Mixed responsibilities and branching complicate CID updates.
- Plan:
  - For each chunk in request.chunks:
    - Read bytes from Redis `obj:{object_id}:part:{id}` (ciphertext if private)
    - Upload + pin to IPFS → CID
    - Idempotently upsert parts(object_id, part_number, ipfs_cid, size_bytes) using UPDATE‑first or INSERT…ON CONFLICT
  - Use a per‑payload semaphore to limit concurrency
  - Do not decrypt/encrypt here (bytes in Redis/IPFS are already in final form)
- Success Criteria:
  - parts rows transition from 'pending' to concrete CIDs reliably
  - Retries do not duplicate/lose updates

## Step 3: Manifest build + publish/pin

- Problem: Manifest exposure is inconsistent and mixed into chunk logic
- Plan:
  - Build manifest JSON from DB (ordered parts with `{part_number, cid, size_bytes}`, filename, content type, appendable flag)
  - Publish/pin the manifest:
    - Prefer SDK s3_publish when account context exists (seed_phrase, subaccount_id, bucket_name), using the object key as filename
    - Otherwise, upload+pin manifest
  - Update `objects.ipfs_cid/cid_id` to manifest CID; set status='uploaded'
- Success Criteria:
  - HEAD exposes the manifest CID consistently
  - GET streams via cache/IPFS from manifest data

## Step 4: Tests & environment

- Host env for e2e:
  - `HIPPIUS_IPFS_GET_URL=http://localhost:8080`
  - `HIPPIUS_E2E_IPFS_API_URL=http://localhost:5001`
- e2e assertions:
  - Waiter observes at least one non‑pending part
  - HEAD returns manifest CID (non‑pending)
  - IPFS listing accepts either Links (SDK publish path) or non‑zero Size

## Step 5: Cleanup & deprecation

- Migrate producers to emit the unified request directly
- Deprecate the two legacy request types once all call sites are updated

## Step 6: Unify multipart and non‑multipart endpoints (schema + cache)

- Problem: MPU flows currently use different schema assumptions and cache keys (`obj:{upload_id}:part:{n}`) than simple PUT.
- Plan:
  - Canonical identity = `object_id` everywhere; `upload_id` remains an S3 session token only.
  - InitiateMultipartUpload:
    - Create (or fetch) `object_id` immediately and persist it on `multipart_uploads` (do not wait for completion).
  - UploadPart:
    - Resolve `object_id` via `upload_id`.
    - Write bytes to Redis `obj:{object_id}:part:{n}` (optionally also write a short‑TTL alias `obj:{upload_id}:part:{n}` for compatibility during transition).
    - Insert placeholder parts row keyed by `(object_id, part_number)` with `ipfs_cid='pending'`, sizes and etag set.
  - ListParts, Abort:
    - Keep S3‑compatible responses; fetch from DB using `upload_id`, but any data writes remain keyed by `object_id`.
  - CompleteMultipartUpload:
    - Validate submitted parts vs DB; compute final ETag; mark MPU completed.
    - Enqueue unified UploadChainRequest with `object_id`, `bucket_name`, `object_key`, `upload_id`, and `chunks=[{id: part_number}]` for all parts.
    - Return S3 completion XML as today.
  - Cache normalization:
    - All runtime consumers read/write `obj:{object_id}:part:{n}` only.
    - On cache miss for canonical key, optionally rehydrate from `obj:{upload_id}:part:{n}` (transition‑only).
- Success criteria:
  - One cache namespace (object_id) serves both simple and MPU flows.
  - One parts schema/path (PRIMARY KEY(object_id, part_number)); no code depends on `upload_id` to find bytes.
  - All MPU e2e tests pass (Initiate, UploadPart, ListParts, Complete) with unified behavior.

---

## Risks & Mitigations

- Redis cache miss for a chunk → treat as transient (retry), keep idempotent DB writes
- Publish path differences (SDK vs upload+pin) → e2e tests accept both listing shapes
- Concurrency races → UPDATE‑first, INSERT…ON CONFLICT upserts; semaphore controls

## Metrics to watch

- Time from PUT/APPEND to first non‑pending part CID
- Share of GETs served from cache vs pipeline
- Retry rates and causes in uploader

## Backward compatibility

- No API changes; object rows continue to expose `x-amz-ipfs-cid` via manifest CID
- Existing parts rows remain valid; manifest built from DB remains source of truth

## Later (Separate ADR)

- Directory per object: `manifest.json` + `parts/` with a DAG root pin (recursive)
- Envelope encryption (per‑object DEK, wrapped for recipients)
- Optional unpin of per‑chunk pins once root pin is authoritative

## QA and rollout discipline

- Execute changes in small steps. After each step, ensure the full e2e suite passes (cache-first, pipeline_only, range, and MPU flows) before proceeding.
