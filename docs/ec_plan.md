## Hippius EC Plan (storage_version = 4)

### Objectives

- Enable systematic Reed–Solomon EC per part with dynamic chunk sizing.
- Preserve existing fast write latency (EC async via workers and queues).
- Seamless reads; only decode when data chunks are missing.
- Migrate legacy objects to v4 EC without downtime.

### Terminology

- stripe: up to k data chunks encoded together to produce m parity chunks.
- data chunk: ciphertext chunk produced by encrypting one plaintext chunk.
- parity chunk: RS parity over equal-length ciphertext symbols.

### Policy and chunk sizing (dynamic)

- Replication below threshold: if part.size_bytes < `ec_threshold_bytes` use scheme `rep-v1` with replication factor `R`.
  - Replication produces distinct CIDs by re-encrypting each chunk with a replica-specific nonce (no cache duplication).
- EC at or above threshold: if part.size_bytes ≥ `ec_threshold_bytes` use scheme `rs-v1` with fixed k,m and dynamic shard_size.
- Choose shard_size per part (bounded by min/max). Fit into one stripe if possible; otherwise minimal stripes. EC operates on ciphertext with virtual zero-extension only during RS math; padded bytes are never stored.

Suggested config keys

- ec.enabled (bool), ec.scheme (e.g. rs-v1), ec.policy_version (int)
- ec.threshold_bytes (computed = k \* min_chunk_size_bytes), replication_factor (e.g., 2)
- ec.k (int), ec.m (int)
- ec.min_chunk_size_bytes (derived from threshold/k by default; clamp 16–4096 KiB)
- ec.max_chunk_size_bytes (e.g., 4–10 MiB)
- ec.worker.concurrency
- ec.queue_name (default: redundancy_requests)

Derivation rule

- If `ec_min_chunk_size_bytes` is not set explicitly, derive `ec_min_chunk_size_bytes = ceil(ec_threshold_bytes / k)` (clamped). This keeps EC-eligible parts near one stripe at threshold size.
- If `ec_min_chunk_size_bytes` is set explicitly, snap `ec_threshold_bytes = k * ec_min_chunk_size_bytes` to avoid incompatible settings.

### Write path (v4)

1. API receives multipart part bytes.
2. Chunk plaintext with chosen chunk_size_bytes (derived for EC-eligible parts); encrypt each chunk independently → ciphertext data chunks.
3. Store per-part meta and ciphertext chunks in Redis (unchanged keyspace for data).
4. After multipart complete:
   - enqueue upload_requests for data parts (existing behavior)
   - enqueue redundancy_requests for each part (new)

### Queues

- redundancy_requests: CPU-bound redundancy worker (EC and replication) wake-ups only.
- upload_requests: reuse existing primary queue for all uploads (data | replica | parity). No new upload queue.
- substrate_requests: reuse existing queue for pinning. No new pin queue.
- DLQ remains `upload_requests:dlq` with existing retry/backoff logic.

### Storage layout (Redis + Filesystem)

- Data (existing):
  - Redis meta: `obj:{oid}:v:{ov}:part:{pn}:meta`
  - Redis chunks: `obj:{oid}:v:{ov}:part:{pn}:chunk:{i}`
  - Filesystem store (durable): `<root>/<oid>/v<ov>/part_<pn>/{chunk_<i>.bin, meta.json}`
- EC meta (new; versioned, follows :meta pattern):
  - `obj:{oid}:v:{ov}:part:{pn}:pv:{pv}:meta`
  - JSON: `{ scheme, k, m, shard_size, stripes, policy_version: pv, enqueue_info, progress (optional) }`
  - Note: authoritative EC state lives in DB; Redis meta is for fast coordination only.
- Parity/replica staging: use filesystem, not Redis
  - Parity (rs-v1): stage files under FS store in a pv-aware path, e.g., `<root>/<oid>/v<ov>/part_<pn>/pv_<pv>/stripe_<s>.parity_<pi>.bin`
  - Replicas (rep-v1): stage files under FS store, e.g., `<root>/<oid>/v<ov>/part_<pn>/rep_<r>.chunk_<i>.bin`
  - Uploader reads staged files, uploads to IPFS, persists CIDs, and removes staged files.

### Database layout

- part_chunks (existing): data chunk CIDs.
- part_ec (new; versioned per part): params/state per policy version
  - `(part_id, policy_version, scheme, k, m, shard_size_bytes, stripes, state, manifest_cid NULL, created_at)`
  - PRIMARY KEY `(part_id, policy_version)`
- part_parity_chunks (new; versioned):
  - rs-v1: parity CIDs per stripe/index ⇒ `(part_id, policy_version, stripe_index, parity_index, cid, created_at)`
  - rep-v1: replica CIDs with `stripe_index=chunk_index` and `parity_index=replica_index` (per-chunk replicas)
  - UNIQUE `(part_id, policy_version, stripe_index, parity_index)`
  - Reader picks the highest complete `policy_version` for decode/fallback (replicas only used when primaries unavailable).
- blobs (new; unified CID work ledger in DB)
  - Represents any blob that will become a CID: data | replica | parity | manifest
  - Columns (essential):
    - `id`, `kind`, `object_id`, `object_version`, `part_id NULL`, `policy_version NULL`
    - `chunk_index NULL`, `stripe_index NULL`, `parity_index NULL`
    - `fs_path`, `size_bytes`, `cid NULL`, `status` ('staged'|'uploading'|'uploaded'|'pinning'|'pinned'|'failed')
    - `last_error NULL`, `last_error_at NULL`, `created_at`, `updated_at`
  - Uniqueness enforced per kind (e.g., parity by (part_id, policy_version, stripe_index, parity_index))
  - Status is single-field; attempts live only in queue payload (Redis)

#### Blobs DDL (ready-to-run)

```sql
-- blobs: unified CID work ledger
CREATE TABLE IF NOT EXISTS blobs (
  id               UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
  kind             TEXT        NOT NULL CHECK (kind IN ('data','replica','parity','manifest')),
  object_id        UUID        NOT NULL,
  object_version   INT         NOT NULL CHECK (object_version > 0),
  part_id          UUID        NULL REFERENCES parts(part_id) ON DELETE CASCADE,
  policy_version   INT         NULL CHECK (policy_version IS NULL OR policy_version >= 0),
  chunk_index      INT         NULL CHECK (chunk_index IS NULL OR chunk_index >= 0),
  stripe_index     INT         NULL CHECK (stripe_index IS NULL OR stripe_index >= 0),
  parity_index     INT         NULL CHECK (parity_index IS NULL OR parity_index >= 0),
  fs_path          TEXT        NOT NULL,
  size_bytes       BIGINT      NOT NULL CHECK (size_bytes >= 0),
  cid              TEXT        NULL,
  status           TEXT        NOT NULL CHECK (status IN ('staged','uploading','uploaded','pinning','pinned','failed')) DEFAULT 'staged',
  last_error       TEXT        NULL,
  last_error_at    TIMESTAMPTZ NULL,
  created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Uniqueness by kind
CREATE UNIQUE INDEX IF NOT EXISTS blobs_parity_uq
ON blobs (part_id, policy_version, kind, stripe_index, parity_index)
WHERE kind = 'parity';

CREATE UNIQUE INDEX IF NOT EXISTS blobs_replica_uq
ON blobs (part_id, kind, chunk_index, parity_index)
WHERE kind = 'replica';

CREATE UNIQUE INDEX IF NOT EXISTS blobs_data_uq
ON blobs (part_id, kind, chunk_index)
WHERE kind = 'data';

CREATE UNIQUE INDEX IF NOT EXISTS blobs_manifest_uq
ON blobs (object_id, object_version, kind)
WHERE kind = 'manifest';

-- Lookups
CREATE INDEX IF NOT EXISTS blobs_status_idx ON blobs (status);
CREATE INDEX IF NOT EXISTS blobs_part_kind_idx ON blobs (part_id, policy_version, kind);
CREATE INDEX IF NOT EXISTS blobs_object_idx ON blobs (object_id, object_version);

-- Touch updated_at
CREATE OR REPLACE FUNCTION touch_updated_at()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN NEW.updated_at = now(); RETURN NEW; END $$;

DROP TRIGGER IF EXISTS trg_touch_blobs ON blobs;
CREATE TRIGGER trg_touch_blobs
BEFORE UPDATE ON blobs
FOR EACH ROW EXECUTE FUNCTION touch_updated_at();
```

#### Repository (no ORM in this PR)

- No ORM introduction in this phase. All DB access goes through a small repository layer using plain SQL for:
  - `blobs`: insert/upsert, state transitions (CAS), lookups by keys/indices
  - `part_ec`: upsert state and completion marking
  - `part_parity_chunks`: insert/upsert CIDs
- Workers and API call repository methods; direct SQL is isolated and centralized. ORM can be added later behind the same repository interfaces.

Repository API (signatures & CAS semantics)

- blobs

  - insert_many(rows: [{kind, object_id, object_version, part_id?, policy_version?, indices?, fs_path, size_bytes}]) -> [id]
  - claim_staged_by_id(id) -> row | None
    - SQL: `UPDATE blobs SET status='uploading' WHERE id=:id AND status='staged' RETURNING *`
  - mark_uploaded(id, cid)
    - SQL: `UPDATE blobs SET cid=:cid, status='uploaded' WHERE id=:id`
  - claim_uploaded_by_id(id) -> row | None
    - SQL: `UPDATE blobs SET status='pinning' WHERE id=:id AND status='uploaded' RETURNING *`
  - mark_pinned(id)
    - SQL: `UPDATE blobs SET status='pinned' WHERE id=:id`
  - mark_failed(id, error, phase)
    - Single status model: set `status='failed'`, `last_error`, `last_error_at=now()`; producer may requeue
  - expected_counts(part_id, policy_version, kind) -> int
    - parity: stripes × m (from part_ec)
    - replica: num_chunks × replicas_per_chunk (from parts + replication factor)
  - pinned_count(part_id, policy_version, kind) -> int

- part_ec
  - upsert(part_id, policy_version, scheme, k, m, shard_size_bytes, stripes, state)
  - maybe_mark_complete(part_id, policy_version)
    - Compare expected vs pinned_count for kind in {'parity','replica'}; UPDATE state='complete' when satisfied

Queue contracts (reuse existing queues)

- upload_requests (producer → uploader)

  - Data: existing payloads
  - Replica/Parity: includes fs_path(s) and indices; repository attaches/creates blobs rows when processing
  - Queue message carries `attempts` for backoff; DB stores status only

- substrate_requests (uploader → substrate)
  - CIDs to pin (existing). On success, repository sets blobs.status='pinned' and removes fs_path

State machine & retries

- staged → uploading → uploaded → pinning → pinned
- On upload error: uploading → staged; set last_error; requeue with attempts+1 and backoff
- On pin error: pinning → uploaded; set last_error; requeue with attempts+1 and backoff

CAS examples (atomic claim)

```sql
-- claim staged for upload
UPDATE blobs
SET status = 'uploading'
WHERE id = $1 AND status = 'staged'
RETURNING *;

-- claim uploaded for pin
UPDATE blobs
SET status = 'pinning'
WHERE id = $1 AND status = 'uploaded'
RETURNING *;
```

Replica nonces

- Implementation note (Oct 2025): replicas currently use SecretBox random nonces (per-encrypt), which already yields distinct ciphertext → distinct CIDs per replica.
- Future plan: optionally add a deterministic KDF for replica nonces (without `upload_id`) under a new `policy_version` to improve reproducibility across retries.

Filesystem layout (authoritative staging)

- Data: `<root>/<oid>/v<ov>/part_<pn>/chunk_<i>.bin`, `meta.json`
- Replicas: `<root>/<oid>/v<ov>/part_<pn>/rep_<r>.chunk_<i>.bin`
- Parity: `<root>/<oid>/v<ov>/part_<pn>/pv_<pv>/stripe_<s>.parity_<pi>.bin`

Completion rules

- Replication: `pinned_count(part_id, policy_version,'replica') >= num_chunks × (R-1)` → `part_ec.state='complete'`
- Parity: `pinned_count(part_id, policy_version,'parity') == stripes × m` → `part_ec.state='complete'`

Metrics & observability

- Counters: blobs.staged/uploaded/pinned, uploader.items_uploaded_total, substrate.items_pinned_total
- Timers: upload_seconds, pin_seconds, ec.encode.seconds
- Errors: last_error sampling, DLQ volume

Rollout & migration

1. Add blobs table + repository (no ORM yet)
2. Redundancy worker: stage to FS and enqueue existing upload_requests (replica/parity). Optionally pre-insert blobs rows
3. Uploader: on redundancy kinds, use repository to attach/create blobs rows, upload, set cid & status='uploaded', enqueue substrate
4. Substrate: pin, set status='pinned', remove fs_path, call maybe_mark_complete
5. Optional later: add blob_id to part_chunks/part_parity_chunks and move read joins

Testing checklist

- Replication < threshold: replicas staged, uploaded, pinned; part_ec complete; distinct CIDs
- EC ≥ threshold (m=1): parity staged, uploaded, pinned; part_ec complete
- Retry paths: upload/pin transient errors revert status and requeue with backoff
- Janitor: cleans stale staged/failed files; no data loss on happy path

### EC worker (encode/replicate)

- Input: (object_id, object_version, part_number, policy_version)
- Steps:
  - rs-v1 (≥ threshold):
    1. Fetch ciphertext data chunks from Redis.
    2. For each stripe of up to k data chunks, zero-extend ciphertext symbols to shard_size for RS math and compute m parity chunks (m=1 placeholder XOR for now).
    3. Stage parity bytes to filesystem under pv-aware path.
    4. Insert one row per parity into `blobs` with kind='parity', indices, fs_path, status='staged'.
    5. Upsert `part_ec` (state='pending_upload').
    6. Enqueue `upload_requests` with a parity UploadRequest that references the staged files (repository will create/update `blobs` rows on the worker side if needed).
  - rep-v1 (< threshold):
    1. Stream plaintext by decrypting the primary ciphertext chunks (Redis first, IPFS fallback via existing migration streaming path).
    2. Re-encrypt each chunk with a replica-specific nonce (derived from object_id, part_number, chunk_index, replica_index).
    3. Stage replica ciphertext to filesystem (one file per (chunk, replica)).
    4. Insert one row per replica into `blobs` with kind='replica', indices, fs_path, status='staged' (optional in this phase; repository can attach rows at upload time).
    5. Upsert `part_ec` (state='pending_upload').
    6. Enqueue `upload_requests` with a replica UploadRequest that references the staged files (repository will create/update `blobs` rows on the worker side if needed).

### Uploader / Substrate (reuse existing queues)

- Uploader:
  - BRPOP `upload_requests` → request (kind='data'|'replica'|'parity'). For redundancy kinds, use repository to insert/update `blobs`, then perform upload; set `blobs.status` to 'uploaded' and persist CID. Enqueue pinning via `substrate_requests`.
- Substrate:
  - BRPOP `substrate_requests` → pin CIDs; on success set `blobs.status='pinned'`, remove staged file via repository; run completion check for EC/replicas to mark `part_ec` complete when expected rows exist.
- Janitor:
  - GC orphaned staged/failed files older than TTL; optionally unstick long-running 'uploading' back to 'staged'.

#### Uploader job payload (parity)

- LPUSH `upload_requests` with a parity UploadRequest (includes staged file paths and indices). Repository attaches/creates `blobs` rows on the worker side as needed.

#### Uploader job payload (data)

- Unchanged. Existing producers LPUSH to `upload_requests`. Over time, producers may optionally pre-insert `blobs` rows via the repository, but this is not required for the initial rollout.

### Reader behavior

- v4: prefer data chunks. Recovery using parity is supported in test-only for now; runtime read-path decode is deferred until ISA-L integration lands.

### Implementation status note

- Current EC encode path ships with an m=1 XOR placeholder via `encode_rs_systematic`; ISA-L integration is pending. Configure `ec_m=1` until the backend is wired.
- v2/v3: unchanged.

### Idempotency & retries (queue-driven)

- Workers are woken by Redis; no DB polling. Queue payloads carry `{blob_id, attempts}` only.
- Claims use CAS-style updates in DB:
  - staged→uploading for upload; uploaded→pinning for pin
  - on failure: revert to previous ready status (uploading→staged, pinning→uploaded), set `last_error`, and requeue with backoff (attempts incremented in the queue message only).

### Worker simplification and layering

- Redundancy worker produces bytes and enqueues existing `upload_requests` with `kind`='replica'|'parity'; it may insert `blobs` rows via repository but does not handle IPFS/pinning.
- Uploader and Substrate remain unchanged operationally (queues and concurrency); they rely on the repository to mutate `blobs`/`part_ec`/`part_parity_chunks` where applicable, so they do not need additional EC/replica context.
- Completion check marks `part_ec` complete when pinned rows meet expected counts.

### EC policy versioning and changing k/m

- Write EC under a versioned `policy_version`; readers select the highest complete `policy_version` for each part.
- Changing m:
  - Increase: compute and upload additional parity for the new `policy_version` (no data changes).
  - Decrease: mark surplus parity deprecated for old `policy_version`; GC later.
- Changing k:
  - Re-encode parity for the new `policy_version`; data chunks remain unchanged; stripes regroup under the new k.
- Redis and DB are keyed by `policy_version` so old parity coexists until GC.
- Readers should use DB to select the highest complete `policy_version`; Redis `...:pv:{pv}:meta` is auxiliary coordination only.

### Idempotency & retries

- Idempotency key: `(object_id, object_version, part_number, policy_version)`.
- Resume parity generation/upload from missing stripes/indices on retry.
- CAS when updating `part_ec` to avoid stale overwrites.

### Observability

- Metrics: `ec.encode.bytes_total`, `ec.encode.seconds`, `ec.jobs.*`, `ec.reads_requiring_decode_total`, `ec.repair.reconstructed_shards_total`.
- Logs correlated by `(object_id, object_version, part_number, policy_version)`.
- Alerts: EC backlog age, decode rate spikes, repair deficits.

### Security & correctness

- AEAD per-chunk: decrypt only exact ciphertext bytes; RS operates on ciphertext with zero-extension for math.
- Integrity: IPFS CIDs for content integrity; EC for availability.

### Rollout

1. Dark launch EC worker disabled.
2. Staging on a test bucket.
3. Canary on a subset of buckets.
4. Wider enablement with `storage_version_default=4`.
5. Background migration for high-value objects.

---

### Write path data flow (API, workers, queues)

```mermaid
flowchart LR
    subgraph Client
        CPUT[PUT/MPU Parts]
    end

    subgraph API
        A1[Chunk plaintext -> Encrypt per-chunk]
        A2[Write data meta+chunks to Redis]
        A3[Insert data rows into blobs]
        A4[Enqueue redundancy job]
    end

    subgraph Queues
        Q2[redundancy_requests]
        Q3[upload_requests]
        Q4[substrate_requests]
    end

    subgraph Redis+FS
        RD_meta[Redis: obj:OID:v:OV:part:PN:meta]
        RD_chunks[Redis: obj:OID:v:OV:part:PN:chunk:I]
        RD_ec_meta[Redis: obj:OID:v:OV:part:PN:pv:PV:meta]
        FS_staging[FS: <root>/OID/vOV/part_PN/(pv|rep)_... .bin]
    end

    subgraph Workers
        WEC[Redundancy Worker]
        WU[Uploader]
        WS[Substrate]
    end

    subgraph Storage
        DB[Postgres]
        IPFS[IPFS]
    end

    CPUT --> A1 --> A2
    A2 --> RD_meta
    A2 --> RD_chunks
    A2 --> A3 --> Q3
    A2 --> A4 --> Q2

    Q2 --> WEC
    RD_chunks --> WEC
    WEC --> RD_ec_meta
    WEC --> FS_staging
    WEC --> Q3

    Q3 --> WU
    RD_chunks --> WU
    FS_staging --> WU
    WU --> IPFS
    WU --> DB
    WU --> Q4

    Q4 --> WS
    WS --> IPFS
    WS --> DB
```
