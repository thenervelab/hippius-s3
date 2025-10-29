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
- ec.threshold_bytes (e.g., 1 MiB), replication_factor (e.g., 2)
- ec.k (int), ec.m (int)
- ec.min_chunk_size_bytes (derived from threshold/k by default; clamp 16–4096 KiB)
- ec.max_chunk_size_bytes (e.g., 4–10 MiB)
- ec.worker.concurrency
- ec.queue_name (default: ec_encode_requests)

Derivation rule

- If `ec_min_chunk_size_bytes` is not set explicitly, derive `ec_min_chunk_size_bytes = ceil(ec_threshold_bytes / k)` (clamped). This keeps EC-eligible parts near one stripe at threshold size.
- If `ec_min_chunk_size_bytes` is set explicitly, snap `ec_threshold_bytes = k * ec_min_chunk_size_bytes` to avoid incompatible settings.

### Write path (v4)

1. API receives multipart part bytes.
2. Chunk plaintext with chosen chunk_size_bytes (derived for EC-eligible parts); encrypt each chunk independently → ciphertext data chunks.
3. Store per-part meta and ciphertext chunks in Redis (unchanged keyspace for data).
4. After multipart complete:
   - enqueue upload_requests for data parts (existing behavior)
   - enqueue ec_encode_requests for each part (new)

### Queues

- upload_requests: I/O-bound uploader (existing). Handles both data and later, parity uploads.
- ec_encode_requests: CPU-bound EC worker.
  - rs-v1 (≥ threshold): produces parity bytes, stores them in Redis, then enqueues parity-upload jobs to upload_requests.
  - rep-v1 (< threshold): re-encrypts chunks with replica-specific nonces and streams replicas directly to IPFS (no Redis duplication), then persists replica CIDs in DB.
- Separate DLQs and retry policies for isolation.

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
  - rep-v1: replica CIDs with `stripe_index=0` and `parity_index=replica_index-1` (for replicas 1..R-1)
  - UNIQUE `(part_id, policy_version, stripe_index, parity_index)`
  - Reader picks the highest complete `policy_version` for decode/fallback (replicas only used when primaries unavailable).

### EC worker (encode/replicate)

- Input: (object_id, object_version, part_number, policy_version)
- Steps:
  - rs-v1 (≥ threshold):
    1. Fetch ciphertext data chunks from Redis.
    2. For each stripe of up to k data chunks, zero-extend ciphertext symbols to shard_size for RS math and compute m parity chunks.
    3. Write parity bytes to Redis keys `...:pv:{pv}:stripe:{s}:parity:{pi}`.
    4. Write/refresh per-part EC meta `...:pv:{pv}:meta` (parameters and enqueue info).
    5. Enqueue parity uploads to upload_requests (payload includes pv and stripe/parity indices).
  - rep-v1 (< threshold):
    1. Stream plaintext by decrypting the primary ciphertext chunks (Redis first, IPFS fallback via existing migration streaming path).
    2. Re-encrypt each chunk with a replica-specific nonce (derived from object_id, part_number, chunk_index, replica_index).
    3. Stream replica ciphertext directly to IPFS, obtain CIDs (no Redis writes for replicas).
    4. Persist replica CIDs into `part_parity_chunks` with `stripe_index=0`, `parity_index=replica_index-1`; update `part_ec` state to complete when all replicas present.

### Uploader (parity)

- rs-v1: Reads parity bytes from Redis by key, uploads to IPFS, persists CIDs in `part_parity_chunks` using `(part_id, policy_version, stripe_index, parity_index)`, updates `part_ec.state` for that `policy_version` to 'complete' when all parity present (in DB), and trims parity Redis keys (delete or short TTL). The uploader does not update Redis EC meta.
- rep-v1: Not used; replicas are uploaded directly by the EC worker to avoid Redis duplication.

#### Uploader job payload (parity)

- kind: `parity`
- object_id, object_version, part_number
- policy_version
- indices: `stripe_index`, `parity_index`
- redis_key: `obj:{oid}:v:{ov}:part:{pn}:pv:{pv}:stripe:{s}:parity:{pi}`
- Optional: scheme/k/m/shard_size for metrics/validation

#### Uploader job payload (data)

- kind: `data`
- object_id, object_version, part_number
- chunk_index
- redis_key: `obj:OID:v:OV:part:PN:chunk:I`

### Reader behavior

- v4: prefer data chunks. If a needed data chunk is missing and `part_ec` exists, reconstruct only the affected stripe using parity (Redis first if available; otherwise IPFS parity), then decrypt only the needed chunks for the byte range.
- v2/v3: unchanged.

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
        A3[Enqueue data uploads]
        A4[Enqueue redundancy job]
    end

    subgraph Queues
        Q1[upload_requests]
        Q2[redundancy_requests]
    end

    subgraph Redis+FS
        RD_meta[Redis: obj:OID:v:OV:part:PN:meta]
        RD_chunks[Redis: obj:OID:v:OV:part:PN:chunk:I]
        RD_ec_meta[Redis: obj:OID:v:OV:part:PN:pv:PV:meta]
        FS_staging[FS: <root>/OID/vOV/part_PN/(pv|rep)_... .bin]
    end

    subgraph Workers
        WEC[Redundancy Worker]
        WU[Upload Worker - data and parity]
    end

    subgraph Storage
        DB[Postgres]
        IPFS[IPFS]
    end

    CPUT --> A1 --> A2
    A2 --> RD_meta
    A2 --> RD_chunks
    A2 --> A3 --> Q1
    A2 --> A4 --> Q2

    Q2 --> WEC
    RD_chunks --> WEC
    WEC --> RD_ec_meta
    WEC --> FS_staging
    WEC --> Q1

    Q1 --> WU
    RD_chunks --> WU
    FS_staging --> WU
    WU --> IPFS
    WU --> DB
```
