# CLAUDE.md

Guidance for Claude Code (and human contributors) working in this repo. The goal of this document is to let a new dev or fresh Claude instance come up to speed in ~15 minutes. Deeper detail lives in per-subsystem `CLAUDE.md` files referenced below.

Companion files:
- [todo.md](todo.md) — known pitfalls, optimizations, and contributor tasks. Read this once you're oriented.
- [README.md](README.md) — user-facing quickstart, client examples, and operational commands.

---

## 1. What is hippius-s3

`hippius-s3` is an S3-compatible gateway in front of the Arion storage backend, with blockchain metadata publishing on the Hippius chain. Clients (AWS CLI, MinIO, boto3, s3cmd) talk to it with SigV4 auth; we authenticate, authorize, encrypt, chunk, stage on a shared filesystem cache, and asynchronously upload to Arion while recording state in Postgres + Redis queues.

What makes this stack different from a normal S3 proxy:

1. **Server-side envelope encryption with OVH KMS.** Every object chunk is AES-256-GCM encrypted with a per-object-version DEK; the DEK is wrapped by a per-bucket KEK; KEK is wrapped by an OVH KMS master key reachable only via mTLS from our API pods. Decryption therefore cannot be done on the client — every read flows through our API for decryption.
2. **Two FastAPI services: a gateway and an internal API.** The gateway does auth, ACL, rate-limit decisions, and forwards to the internal API with trusted `X-Hippius-*` headers. This is a full streaming proxy, not a redirect.
3. **Filesystem-first cache.** Chunk data lives on a shared NVMe/CephFS volume (`/var/lib/hippius/object_cache`). Redis is used for pub/sub chunk-ready notifications and for work queues — **not** for chunk storage. This is new as of 2026-04-21 (the old Redis download cache is gone — see [todo.md](todo.md)).
4. **Async backend writes.** Client PUT returns success once data hits the FS cache + DB row + Redis queue. A dedicated uploader worker drains the queue and uploads to Arion and publishes to the Hippius chain. State progresses `pending → uploading → uploaded → published`.
5. **S4 append extension.** On top of standard S3 we support atomic O(delta) appends with compare-and-swap semantics, spec at [docs/s4.md](docs/s4.md).

The pipeline is deliberately split so the user-facing path (gateway + API) is fast and bounded in memory, while slow/brittle work (Arion uploads, chain publishing, cleanup) is pushed to workers that can retry independently.

---

## 2. Repo topology

```
.
├── gateway/                 # Public-facing FastAPI on :8080 — auth, ACL, forward
│   ├── middlewares/         # Auth chain, CORS, input validation, HMAC, etc.
│   ├── services/            # auth_orchestrator, acl_service, forward_service
│   └── routers/             # /docs proxy, /acl endpoints
├── hippius_s3/              # Main package
│   ├── api/                 # Internal FastAPI on :8000
│   │   ├── middlewares/     # parse_internal_headers, fs_cache_pressure, ip_whitelist
│   │   └── s3/              # buckets/, objects/, multipart.py, extensions/append.py
│   ├── writer/              # Upload pipeline: object_writer, chunker, write_through_writer
│   ├── reader/              # Read pipeline: planner, streamer, decrypter
│   ├── cache/               # FileSystemPartsStore, RedisObjectPartsCache, ChunkNotifier
│   ├── services/            # crypto, KMS, Arion client, Hippius API, copy, audit, ACL helper
│   ├── workers/             # Core worker loops (uploader, downloader, unpinner)
│   ├── dlq/                 # Dead-letter queue implementations (upload, unpin)
│   ├── repositories/        # Database access layer
│   ├── sql/                 # Migrations (migrations/) and parameterized queries (queries/)
│   ├── scripts/             # Operational Python scripts
│   ├── config.py            # All env vars, typed dataclass
│   └── main.py              # API factory + lifespan
├── workers/                 # Worker ENTRY points (run_*_in_loop.py) — invoked by k8s
├── cacher/                  # Substrate account data cacher service
├── scripts/                 # Top-level ops scripts (dumps, smoke tests, MPU retry)
├── tests/                   # unit/, integration/, e2e/, smoke/
├── docs/                    # Architecture and spec docs (s4.md, s3-compatibility.md)
├── k8s/                     # Kustomize manifests: base/, staging/, production/, cache/, otel/
├── monitoring/              # Grafana dashboards, observability config
└── examples/                # Python and JavaScript client examples
```

Performance benchmarks live in the separate `hippius-benchmarks` repo, not here.

A **subsystem index** with links to per-directory `CLAUDE.md` files is in section 7 below.

---

## 3. Request lifecycle

### 3.1 PUT (simple object)

1. **Client → Gateway** (`https://s3.hippius.com/<bucket>/<key>`). SigV4-signed.
2. **Gateway middleware chain** ([gateway/main.py:181-197](gateway/main.py)) — registered bottom-up, runs top-down: CORS → (read-only guard) → input validation → auth router → trailing slash → account → ACL → frontend HMAC → tracing → metrics → audit log → ray_id. On the way back out: the same stack in reverse.
3. **Auth orchestrator** ([gateway/services/auth_orchestrator.py:39](gateway/services/auth_orchestrator.py)) picks one of five methods (presigned URL, bearer, access key SigV4, seed-phrase SigV4, anonymous), verifies the signature, and attaches `request.state.account_id` / `request.state.account` / etc.
4. **ACL middleware** ([gateway/middlewares/acl.py:70](gateway/middlewares/acl.py)) checks bucket ownership + permission. Master tokens bypass.
5. **Forward** ([gateway/services/forward_service.py:67](gateway/services/forward_service.py)). Gateway strips client-supplied `X-Hippius-*` headers ([forward_service.py:71-74](gateway/services/forward_service.py)), then adds trusted headers: `X-Hippius-Ray-ID`, `X-Hippius-Request-User`, `X-Hippius-Bucket-Owner`, `X-Hippius-Main-Account`, `X-Hippius-Seed` (if seed auth), `X-Hippius-Has-Credits`, `X-Hippius-Can-Upload`, `X-Hippius-Can-Delete`, `X-Hippius-Gateway-Time-Ms`. Body is **streamed** (`request.stream()`), not buffered.
6. **API middleware chain** ([hippius_s3/main.py:293-299](hippius_s3/main.py)): `metrics → tracing → parse_internal_headers → ip_whitelist → fs_cache_pressure`. `fs_cache_pressure` ([hippius_s3/api/middlewares/fs_cache_pressure.py](hippius_s3/api/middlewares/fs_cache_pressure.py)) short-circuits PUTs with 503 + Retry-After **before reading the body** if the cache disk is ≥90% full.
7. **PutObject endpoint** ([hippius_s3/api/s3/objects/put_object_endpoint.py:29](hippius_s3/api/s3/objects/put_object_endpoint.py)). Resolves bucket, decides if this is an S4 append (`x-amz-meta-append: true` → [extensions/append.py](hippius_s3/api/s3/extensions/append.py)), builds metadata.
8. **Object writer** ([hippius_s3/writer/object_writer.py:169 `put_simple_stream_full`](hippius_s3/writer/object_writer.py)):
   - **Reserve version**: `upsert_object_basic` inserts/bumps `object_versions` with placeholder size/md5 ([object_writer.py:210](hippius_s3/writer/object_writer.py)). **The DB-returned `object_id` is authoritative** — a concurrent create on the same (bucket, key) may override the client-generated candidate UUID ([object_writer.py:222-227](hippius_s3/writer/object_writer.py)).
   - **Generate DEK**, wrap it with the bucket KEK, and **immediately write the envelope to DB** ([object_writer.py:244-261](hippius_s3/writer/object_writer.py)) — a concurrent GET between the upsert and this write would otherwise hit a NULL `kek_id`/`wrapped_dek` and 500. See [analysis.md](analysis.md) for the broken-v5 bug that still produces orphan rows when the PUT aborts mid-stream.
   - **Producer/consumer pipeline**: a coroutine drains `body_iter`, encrypts each chunk under the global `chunk_index` (required for AEAD nonce determinism), and enqueues onto an asyncio queue (maxsize 16). A consumer reads the queue and writes to `fs_store.set_chunk` ([object_writer.py:308-333](hippius_s3/writer/object_writer.py)). FS writes are fatal; Redis batched `set_chunks` is best-effort ([object_writer.py:285-306](hippius_s3/writer/object_writer.py)).
   - **Write FS meta** last ([object_writer.py:420](hippius_s3/writer/object_writer.py)) — `meta.json` is the "part complete" signal, so it must land after every chunk.
   - **Update object_versions** with final size/md5 ([object_writer.py:442](hippius_s3/writer/object_writer.py)). Until this runs, the download query skips the version — it's reserved but not serveable.
   - **Return**. Client sees 200 OK.
9. **Enqueue upload** to `arion_upload_requests` Redis queue ([put_object_endpoint.py:162](hippius_s3/api/s3/objects/put_object_endpoint.py)).
10. **Arion uploader worker** ([workers/run_arion_uploader_in_loop.py](workers/run_arion_uploader_in_loop.py)) picks up the request, reads chunks from FS, uploads to Arion, records `chunk_backend` rows, publishes to the Hippius chain via [hippius_s3/services/hippius_api_service.py](hippius_s3/services/hippius_api_service.py).

### 3.2 GET (full object or Range)

1. Gateway middleware chain (same as PUT, minus write-only middlewares).
2. **GetObject endpoint** → [hippius_s3/services/object_reader.py `build_stream_context`](hippius_s3/services/object_reader.py):
   - Read parts list from DB.
   - Build chunk plan ([hippius_s3/reader/planner.py](hippius_s3/reader/planner.py)) — maps Range bytes to (part_number, chunk_index, slice_start, slice_end).
   - **Batch-check** every needed chunk on FS in one pass ([object_reader.py:67](hippius_s3/services/object_reader.py) via `chunks_exist_batch`).
   - If all present → `source="cache"`; stream directly.
   - If any missing → `source="pipeline"`:
     - **Coalesce**: per (object, version, part), try `SET NX EX 120` on `download_in_progress:{object_id}:v:{ov}:part:{pn}` ([object_reader.py:87-104](hippius_s3/services/object_reader.py)). If you lose the race, skip the enqueue; another streamer is already fetching and you'll wait on pub/sub.
     - If you won, build a `DownloadChainRequest` with optional per-chunk CIDs and enqueue to `arion_download_requests` ([object_reader.py:146-165](hippius_s3/services/object_reader.py)).
   - **Unwrap DEK** from DB (`kek_id`, `wrapped_dek`) via [hippius_s3/services/envelope_service.py](hippius_s3/services/envelope_service.py). If the current version is mid-write (envelope missing), fall back to version-1 ([object_reader.py:177-220](hippius_s3/services/object_reader.py)).
3. **Stream plan** ([hippius_s3/reader/streamer.py:18](hippius_s3/reader/streamer.py)):
   - Configurable prefetch depth (default 0 for correctness) overlaps FS fetch with decrypt+IO.
   - For each chunk: `obj_cache.wait_for_chunk` → fast path reads from FS; slow path subscribes to `notify:{chunk_key}` pub/sub and re-reads on notification ([hippius_s3/cache/notifier.py:61](hippius_s3/cache/notifier.py)).
   - Decrypt ([reader/decrypter.py](hippius_s3/reader/decrypter.py)), optionally slice for Range, yield.
4. **Downloader worker** ([hippius_s3/workers/downloader.py:94](hippius_s3/workers/downloader.py)) handles `DownloadChainRequest`:
   - Writes `meta.json` **eagerly** from DB parts rows ([downloader.py:49-91](hippius_s3/workers/downloader.py)) so partial-range fills are readable per-chunk as they land.
   - For each chunk: check FS (maybe another worker filled it) → look up `backend_identifier` in `chunk_backend` → fetch from Arion → `fs_store.set_chunk` → `obj_cache.notify_chunk`.
   - Releases the coalesce lock on part completion ([downloader.py:286-292](hippius_s3/workers/downloader.py)) — key format **must match** the streamer's exactly.

### 3.3 Range request specifics

The planner ([reader/planner.py](hippius_s3/reader/planner.py)) only includes chunks that intersect the requested range, and sets `slice_start`/`slice_end_excl` on the first and last to trim plaintext. The downloader, when invoked for a Range miss, fetches **full chunks** (not byte ranges) from Arion — there is no `BackendClient.download_range` yet. See [todo.md](todo.md) for this optimization.

---

## 4. Crypto / KMS cheat sheet

```
OVH KMS master key  (HSM; reachable only via mTLS from API pods)
   │ wraps
   ▼
Bucket KEK           (per-bucket; stored wrapped in the keystore DB)
   │ wraps
   ▼
Object DEK           (per object_version; AES-256; wrapped_dek stored on object_versions)
   │ encrypts
   ▼
Chunk ciphertext     (AES-256-GCM per chunk; AAD binds bucket_id:object_id:version)
```

- Suite id: `hip-enc/aes256gcm` (stored per-version in `object_versions.enc_suite_id`).
- AAD is `f"hippius-dek:{bucket_id}:{object_id}:{object_version}"` — copy operations must rewrap the DEK under the destination AAD ([hippius_s3/services/copy_service_v5.py](hippius_s3/services/copy_service_v5.py)).
- Chunk index is the AAD for AEAD per-chunk, which is why chunks must be encrypted with their **global** index (not per-part) — see [object_writer.py:349-360](hippius_s3/writer/object_writer.py).
- KMS code: [hippius_s3/services/kek_service.py](hippius_s3/services/kek_service.py), [hippius_s3/services/ovh_kms_client.py](hippius_s3/services/ovh_kms_client.py). `HIPPIUS_KMS_MODE=disabled` (dev) substitutes [hippius_s3/services/local_kek_wrapper.py](hippius_s3/services/local_kek_wrapper.py) so tests don't need mTLS.
- The broken-v5 incident ([analysis.md](analysis.md)) concerns rows with `storage_version>=5` but `kek_id IS NULL OR wrapped_dek IS NULL` — caused by client disconnect between the `upsert_object_basic` and the envelope UPDATE. Fix now writes the envelope immediately after reserve ([object_writer.py:244-261](hippius_s3/writer/object_writer.py)) but legacy orphan rows still exist in prod.

---

## 5. Storage: FS cache + Redis + Postgres

### 5.1 Filesystem cache

`/var/lib/hippius/object_cache` (configurable: `HIPPIUS_OBJECT_CACHE_DIR`).

```
/var/lib/hippius/object_cache/
└── <object_id_uuid>/
    └── v<version>/
        └── part_<number>/
            ├── chunk_0.bin
            ├── chunk_1.bin
            ├── ...
            ├── meta.json        # Presence = "part has been seen"
            └── <f>.tmp.<uuid>   # In-flight atomic write (rare; janitor cleans if stale)
```

- **Atomic writes**: each worker writes to a unique `.tmp.<uuid4>` file and `os.replace`s onto the final path ([hippius_s3/cache/fs_store.py:92](hippius_s3/cache/fs_store.py), [fs_store.py:123-131](hippius_s3/cache/fs_store.py)). Concurrent writers of the same chunk are safe — content is deterministic per (object_id, version, part, chunk_index), so last rename wins is harmless.
- **Meta is the readiness signal**: `get_chunk` returns `None` if `meta.json` is missing ([fs_store.py:168](hippius_s3/cache/fs_store.py)) — even if the chunk file exists. Uploaders write meta **last** (after all chunks); downloaders write meta **first** (so per-chunk visibility works as chunks land).
- **Hot retention via `os.utime`**: reads touch both chunk and meta atime/mtime ([fs_store.py:183-186](hippius_s3/cache/fs_store.py)). Janitor uses this to keep hot parts on NVMe for `HIPPIUS_FS_CACHE_HOT_RETENTION_SECONDS` (default 3h).
- **UUID coercion**: asyncpg may hand back `UUID` objects OR strings. `_safe_object_id` handles both ([fs_store.py:48-62](hippius_s3/cache/fs_store.py)) and rejects anything else to prevent path traversal.

### 5.2 Janitor (FS cache GC)

[workers/run_janitor_in_loop.py](workers/run_janitor_in_loop.py) — critical safety invariants documented at the top of the file ([janitor.py:1-22](workers/run_janitor_in_loop.py)):

- **Replication is an absolute gate.** A chunk that has NOT been replicated to every required backend (`HIPPIUS_UPLOAD_BACKENDS` ∪ `HIPPIUS_BACKUP_BACKENDS`) is **never** deleted — under any condition, including disk-full.
- **Hot retention.** Parts whose atime is within `fs_cache_hot_retention_seconds` are kept, regardless of age.
- **DLQ protection.** `get_all_dlq_object_ids` ([janitor.py:220](workers/run_janitor_in_loop.py)) scans all upload + unpin DLQs to avoid deleting data for still-in-flight operations.
- **Disk-pressure modes** ([janitor.py:125-146](workers/run_janitor_in_loop.py)):
  - Normal (<85%): honor hot retention.
  - Elevated (85-95%): halve the hot-retention window.
  - Critical (≥95%): disable hot retention. But **still** replication-gated — if nothing is deletable under critical pressure, log ERROR and refuse to free space. Deadlock detection, not silent data loss.
- **Orphan cleanup**: `.tmp.*` files older than 1h ([janitor.py:76](workers/run_janitor_in_loop.py)) get deleted.
- **Metrics**: `fs_store_parts_on_disk`, `fs_store_oldest_age_seconds`, `fs_cache_disk_used_bytes`, `fs_cache_hot_parts`, `fs_cache_pressure_mode`, `fs_cache_age_bucket_parts`.

### 5.3 Redis instances

Six separate services for blast-radius isolation:

| Service | Port | Purpose | Persistence |
|---|---|---|---|
| `redis` | 6379 | General cache / short-lived state | Ephemeral |
| `redis-accounts` | 6380 | Account credit cache | Persistent (AOF) |
| `redis-chain` | 6381 | Blockchain operation cache | Persistent (AOF) |
| `redis-queues` | 6382 | Work queues + chunk pub/sub notifications + drain coordination (`cephor:*`) | Persistent, 2GB, **noeviction** (holds queue + coordination data — must not evict; a full instance fails writes loudly) |
| `redis-rate-limiting` | 6383 | Rate limit counters | Ephemeral, 1GB |
| `redis-acl` | 6384 | ACL cache | Ephemeral, 2GB, LRU |

**Not in the table any more**: the old 32GB `redis-download-cache` (6385). Decommissioned 2026-04-21 with the FS-cache migration. If you spot a reference to `REDIS_DOWNLOAD_CACHE_URL`, it's stale.

The `ChunkNotifier` pub/sub ([hippius_s3/cache/notifier.py:46-49](hippius_s3/cache/notifier.py)) publishes to `notify:{chunk_key}` on `redis-queues`. Streamers subscribe + re-check on each notification.

### 5.4 Postgres schema (high level)

- **`objects`** — logical objects, current version pointer.
- **`object_versions`** — one row per PUT/overwrite. Holds `storage_version`, `size_bytes`, `md5_hash`, `kek_id`, `wrapped_dek`, `enc_suite_id`, `enc_chunk_size_bytes`.
- **`parts`** — per-object-version part rows with `size_bytes`, `chunk_size_bytes` (may vary per part), `etag`.
- **`part_chunks`** — per-chunk rows with optional `cid` and `cipher_size_bytes`.
- **`chunk_backend`** — `(chunk_id, backend, backend_identifier, deleted, deleted_at)` — maps a chunk to its location on each backend. Soft delete only.
- **`buckets`**, **`bucket_acls`**, **`object_acls`** — standard S3 metadata.
- **`multipart_uploads`** — in-flight MPU state.

Separate keystore DB for encryption keys (`HIPPIUS_KEYSTORE_DATABASE_URL`, falls back to `DATABASE_URL`). Migrations live in [hippius_s3/sql/migrations/](hippius_s3/sql/migrations/), queries in [hippius_s3/sql/queries/](hippius_s3/sql/queries/). Run via `python -m hippius_s3.scripts.migrate`.

---

## 6. Authentication (five methods)

Orchestrated by [gateway/services/auth_orchestrator.py:39 `authenticate_request`](gateway/services/auth_orchestrator.py). Priority order:

1. **Presigned URL** — query params `X-Amz-Algorithm=AWS4-HMAC-SHA256`, `X-Amz-Credential`, `X-Amz-Signature`. Credential starts with `hip_`. Verified by [gateway/middlewares/access_key_auth.py:139](gateway/middlewares/access_key_auth.py).
2. **Bearer token** — `Authorization: Bearer hip_...`. Verified against the Hippius API via [gateway/services/auth_cache.py](gateway/services/auth_cache.py) (Redis-cached).
3. **Access key header** — SigV4 Authorization with credential starting `hip_`. Same verifier as presigned URL ([access_key_auth.py:35](gateway/middlewares/access_key_auth.py)).
4. **Seed phrase SigV4** — base64-encoded 12-word seed as access key ID, raw seed as secret. Verified by [gateway/middlewares/sigv4.py `SigV4Verifier`](gateway/middlewares/sigv4.py).
5. **Anonymous** — GET/HEAD on public buckets with no Authorization header.

Canonicalization uses `request.scope["raw_path"]` (bytes) rather than `request.url.path` to preserve exact percent-encoding. `hmac.compare_digest` is used for signature comparison (constant-time).

**Token types**: Arion returns `master` or `sub` for access keys. Master tokens bypass ACL entirely ([gateway/middlewares/acl.py:126-130](gateway/middlewares/acl.py)) — authorization is enforced upstream by Arion. Sub-token scope evaluation is partially implemented ([gateway/services/sub_token_scope.py](gateway/services/sub_token_scope.py)) but **not wired** and currently imports a nonexistent `TokenAcl` — see [todo.md](todo.md).

---

## 7. Subsystem index

### Gateway
- [gateway/CLAUDE.md](gateway/CLAUDE.md) — entry, middleware order, ForwardService.
- [gateway/middlewares/CLAUDE.md](gateway/middlewares/CLAUDE.md) — per-middleware behavior.
- [gateway/services/CLAUDE.md](gateway/services/CLAUDE.md) — auth_orchestrator, ACLService, ForwardService, sub_token_scope (dormant).
- Entry: [gateway/main.py](gateway/main.py) — `factory()` at line 35.

### Internal API
- [hippius_s3/api/CLAUDE.md](hippius_s3/api/CLAUDE.md) — router structure, lifespan, middleware chain.
- [hippius_s3/api/middlewares/CLAUDE.md](hippius_s3/api/middlewares/CLAUDE.md) — `fs_cache_pressure`, `parse_internal_headers`, `ip_whitelist`.
- [hippius_s3/api/s3/objects/CLAUDE.md](hippius_s3/api/s3/objects/CLAUDE.md) — PUT/GET/HEAD/DELETE/COPY endpoints.
- Entry: [hippius_s3/main.py](hippius_s3/main.py) — `factory()` at line 237, `lifespan` at 85.

### Upload pipeline
- [hippius_s3/writer/CLAUDE.md](hippius_s3/writer/CLAUDE.md) — `ObjectWriter`, `WriteThroughPartsWriter`, chunker, DB.

### Download / streaming pipeline
- [hippius_s3/reader/CLAUDE.md](hippius_s3/reader/CLAUDE.md) — planner, streamer, decrypter.
- [hippius_s3/services/object_reader.py `build_stream_context`](hippius_s3/services/object_reader.py) — cache-vs-pipeline decision + download coalescing.

### Cache & pub/sub
- [hippius_s3/cache/CLAUDE.md](hippius_s3/cache/CLAUDE.md) — `FileSystemPartsStore`, `RedisObjectPartsCache`, `ChunkNotifier`, `DualFileSystemPartsStore`.

### Workers
- [hippius_s3/workers/CLAUDE.md](hippius_s3/workers/CLAUDE.md) — core logic (uploader, downloader, unpinner).
- [workers/CLAUDE.md](workers/CLAUDE.md) — entry-point loops and janitor.
- Entry scripts: [workers/run_arion_uploader_in_loop.py](workers/run_arion_uploader_in_loop.py), [workers/run_arion_downloader_in_loop.py](workers/run_arion_downloader_in_loop.py), [workers/run_arion_unpinner_in_loop.py](workers/run_arion_unpinner_in_loop.py), [workers/run_janitor_in_loop.py](workers/run_janitor_in_loop.py), [workers/run_orphan_checker_in_loop.py](workers/run_orphan_checker_in_loop.py), [workers/run_account_cacher_in_loop.py](workers/run_account_cacher_in_loop.py), [workers/run_migrator_once.py](workers/run_migrator_once.py), [workers/cachet_health_check.py](workers/cachet_health_check.py).

### Business services
- [hippius_s3/services/CLAUDE.md](hippius_s3/services/CLAUDE.md) — all service modules.
- Key files: [arion_service.py](hippius_s3/services/arion_service.py), [hippius_api_service.py](hippius_s3/services/hippius_api_service.py), [crypto_service.py](hippius_s3/services/crypto_service.py), [envelope_service.py](hippius_s3/services/envelope_service.py), [kek_service.py](hippius_s3/services/kek_service.py), [ovh_kms_client.py](hippius_s3/services/ovh_kms_client.py), [copy_service_v5.py](hippius_s3/services/copy_service_v5.py), [parts_service.py](hippius_s3/services/parts_service.py), [parts_catalog.py](hippius_s3/services/parts_catalog.py).

### DLQ
- [hippius_s3/dlq/CLAUDE.md](hippius_s3/dlq/CLAUDE.md) — base DLQ with Redis LPUSH layout, upload/unpin subclasses.

### Database
- [hippius_s3/sql/CLAUDE.md](hippius_s3/sql/CLAUDE.md), [migrations](hippius_s3/sql/migrations/), [queries](hippius_s3/sql/queries/). Migrations run via `python -m hippius_s3.scripts.migrate`.
- [hippius_s3/repositories/](hippius_s3/repositories/) — data access wrappers.

### Scripts
- [hippius_s3/scripts/CLAUDE.md](hippius_s3/scripts/CLAUDE.md) — operational and migration scripts.
- [scripts/CLAUDE.md](scripts/CLAUDE.md) — top-level ops scripts (smoke tests, dump generators).

### Tests
- [tests/unit/CLAUDE.md](tests/unit/CLAUDE.md), [tests/integration/CLAUDE.md](tests/integration/CLAUDE.md), [tests/e2e/CLAUDE.md](tests/e2e/CLAUDE.md).

---

## 8. Environment & configuration

Config is a typed dataclass: [hippius_s3/config.py](hippius_s3/config.py). Values are loaded from `.env.defaults` → `.env` (or `.env.test-local` / `.env.test-docker` for tests).

### Essential env vars

| Variable | Default | Notes |
|---|---|---|
| `DATABASE_URL` | — | Postgres connection string (required). |
| `HIPPIUS_KEYSTORE_DATABASE_URL` | `DATABASE_URL` | Separate keystore DB, falls back. |
| `REDIS_URL` | — | Main Redis (:6379). |
| `REDIS_QUEUES_URL` | `:6382` | Queue + pub/sub Redis. Persistent. |
| `REDIS_ACCOUNTS_URL` | `:6380` | Account cache. Persistent. |
| `REDIS_CHAIN_URL` | `:6381` | Chain cache. Persistent. |
| `REDIS_RATE_LIMITING_URL` | `:6383` | Rate limit counters. |
| `REDIS_ACL_URL` | `:6384` | ACL cache. |
| `HIPPIUS_ARION_BASE_URL` | `https://arion.hippius.com/` | Arion endpoint. |
| `ARION_SERVICE_KEY`, `ARION_BEARER_TOKEN` | — | Arion auth. |
| `HIPPIUS_KMS_MODE` | — | `required` (prod) or `disabled` (dev). |
| `HIPPIUS_SERVICE_KEY`, `HIPPIUS_AUTH_ENCRYPTION_KEY` | — | 64-hex each. |
| `FRONTEND_HMAC_SECRET` | — | For frontend-signed requests. |

### Cache & performance knobs

| Variable | Default | Notes |
|---|---|---|
| `HIPPIUS_OBJECT_CACHE_DIR` | `/var/lib/hippius/object_cache` | FS cache root. |
| `HIPPIUS_OBJECT_CACHE_FALLBACK_DIR` | — | If set, wraps FS store in `DualFileSystemPartsStore` for read-only migration fallback. |
| `HIPPIUS_CHUNK_SIZE_BYTES` | `4194304` (4 MiB) | Must be consistent across upload/download code paths. |
| `HIPPIUS_CACHE_TTL` | `3600` | Pub/sub wait timeout. |
| `HIPPIUS_FS_CACHE_HOT_RETENTION_SECONDS` | `10800` (3h) | Janitor keeps recently-read parts. |
| `DOWNLOAD_COALESCE_LOCK_TTL` | `120` | Lock expiry guards downloader crashes. |
| `DOWNLOADER_SEMAPHORE` | `20` | Concurrent chunk fetches per DCR. |
| `DOWNLOADER_MAX_INFLIGHT` | `10` | Concurrent `DownloadChainRequest`s per pod. |
| `DOWNLOADER_CHUNK_RETRIES` | `3` | Per-chunk retry attempts. |

### Backend routing

| Variable | Default | Notes |
|---|---|---|
| `HIPPIUS_UPLOAD_BACKENDS` | `arion` | Which backends the uploader writes to. |
| `HIPPIUS_DOWNLOAD_BACKENDS` | `arion` | Download fallback order. |
| `HIPPIUS_DELETE_BACKENDS` | `arion` | Unpin fan-out. |
| `HIPPIUS_BACKUP_BACKENDS` | `` | Extra backends that must replicate a chunk before the janitor may evict it — unioned with upload backends for the replication gate ([config.py:163](hippius_s3/config.py)). |

### Feature flags

| Variable | Default | Notes |
|---|---|---|
| `ENABLE_AUDIT_LOGGING` | `true` | Gateway audit log middleware. |
| `ENABLE_BANHAMMER` | `true` | Code exists but middleware registration is currently commented out; see [gateway/main.py:94](gateway/main.py). |
| `HIPPIUS_BYPASS_CREDIT_CHECK` | `false` | Test-only. |
| `HIPPIUS_READ_ONLY_MODE` | `false` | Blocks all writes at gateway. |
| `ENABLE_REQUEST_PROFILING` | `false` | Speedscope profiler middleware. |

Full list: [hippius_s3/config.py](hippius_s3/config.py).

---

## 9. Development

### 9.1 Prerequisites

- Python 3.10+ and `uv` for package management.
- Docker + `docker compose` (v2 syntax — **not** `docker-compose`).
- The project virtualenv at `.venv` (always `source .venv/bin/activate` before running Python).
- On macOS with Homebrew, use the system install at `/usr/local/Homebrew/bin/brew`.

### 9.2 Common commands

```bash
# Tests
pytest tests/unit -v
pytest tests/integration -v
pytest tests/e2e -v

# Targeted tests relevant to recent changes
pytest tests/unit/test_download_coalescing.py -xvs
pytest tests/unit/test_janitor_hot_retention.py -xvs
pytest tests/unit/cache -xvs
pytest tests/e2e/test_GetObject_Range.py -xvs

# Code quality
ruff check . --fix
ruff format .
mypy hippius_s3
pre-commit run --all-files

# Run stack
docker compose up -d
docker compose logs -f api
docker compose restart api

# E2E with mocked backends (toxiproxy + mock-arion + mock-kms + mock-hippius-api)
COMPOSE_PROJECT_NAME=hippius-e2e docker compose -f docker-compose.yml -f docker-compose.e2e.yml up -d --wait
pytest tests/e2e -v
COMPOSE_PROJECT_NAME=hippius-e2e docker compose -f docker-compose.yml -f docker-compose.e2e.yml down -v

# Rebuild base image (rare)
COMPOSE_PROFILES=build-base docker compose build --no-cache base

# Run DB migrations
python -m hippius_s3.scripts.migrate
```

Note on dev loop: python code changes **auto-restart the container** (uvicorn `reload`). No need to bounce docker between edits.

### 9.3 Repo conventions

- Line length 120.
- ruff+ mypy (strict). Single-line imports (isort `force-single-line = true`).
- **Avoid `try/except` unless absolutely necessary.** Let errors bubble up and kill the request; otherwise debugging is painful. Only swallow in narrow, well-justified spots (resource cleanup, best-effort Redis).
- Keep inline comments minimal; write them only where the **why** is non-obvious. Never explain what code does, only why it deviates from the obvious.
- Never add docstrings at the top of modules. Per-function/class is fine when behavior is non-trivial.
- No new `""" ... """` comment blocks for TODOs — use a single `# TODO:` line or add to [todo.md](todo.md).

---

## 10. Deployment

Kustomize-based. Namespaces: `hippius-s3-staging` and `hippius-s3-prod`.

- [k8s/base/](k8s/base/) — shared resources.
- [k8s/staging/](k8s/staging/), [k8s/production/](k8s/production/) — per-env patches.
- [k8s/cache/](k8s/cache/) — regional cache stack (gateway + api + downloader + Redis per region).
- [k8s/otel/](k8s/otel/) — LGTM observability stack (Alloy, Loki, Prometheus, Tempo, Grafana).

### 10.1 CI/CD

Two deploy workflows, one per environment:

- [.github/workflows/staging-deploy.yaml](.github/workflows/staging-deploy.yaml) — triggers on pushes to the `staging` branch. Job: `deploy-staging`.
- [.github/workflows/production-deploy.yaml](.github/workflows/production-deploy.yaml) — triggers on pushes to the `k8s-production` branch. Jobs: `deploy-production`, `deploy-cache-production` (**currently DISABLED** via `if: false` — regional cache rollout for `us-0` / `eu-0`, to be re-enabled once the FS cache NVMe PVC story is finalized per region).

Both share the same `build-base` and `build-images` jobs (duplicated, since GitHub Actions can't share jobs across workflows without `workflow_call`).

### 10.2 Monitoring

OTel instrumentation across FastAPI, asyncpg, httpx, redis. Standard span attributes: `hippius.ray_id`, `hippius.account.main`. Metrics exported via Prometheus on `/metrics`; dashboards in [monitoring/grafana/](monitoring/grafana/) and [k8s/base/grafana-dashboards.yaml](k8s/base/grafana-dashboards.yaml).

Key dashboards: Hippius S3 Overview (request rates, latencies, error rates, queue depths), S3 Workers (backend latency, retry rates), FS cache (age buckets, pressure mode, hot parts).

### 10.3 Querying Loki on prod

Loki is the LGTM stack's log store. It runs in the `monitoring` namespace as `loki-0` and is exposed via the `loki` service on port 3100. Grafana is at `monitoring/grafana` (NodePort 31337). Current kubectl context: `hippius`.

**Connect (port-forward):**
```bash
kubectl -n monitoring port-forward svc/loki 3100:3100 >/tmp/loki-pf.log 2>&1 &
sleep 2
curl -s http://localhost:3100/loki/api/v1/labels   # sanity check
```
The pod's container has no `wget`/`curl`, so always port-forward and curl from your host.

**Useful labels** (from `/loki/api/v1/labels`): `namespace`, `app`, `pod`, `node`, `container`, `level`.

**Namespaces of interest:** `hippius-s3-prod`, `hippius-s3-staging`, `hippius-arion`, `hippius-arion-staging`, `hippius-indexer`.

**`app` values in `hippius-s3-prod`:** `gateway`, `api`, `arion-uploader`, `arion-downloader`, `arion-unpinner`, `janitor`, `account-cacher`, `cachet-health-checker`, 1`redis-queues`, `redis-accounts`, `otel-collector`.

**Run a query (LogQL):**
```bash
END=$(date +%s)000000000
START=$(( $(date +%s) - 3600 ))000000000   # 1h window
curl -sG "http://localhost:3100/loki/api/v1/query_range" \
  --data-urlencode 'query={namespace="hippius-s3-prod",app="gateway"} |= "S3_OPERATION_SUCCESS"' \
  --data-urlencode "start=$START" --data-urlencode "end=$END" --data-urlencode "limit=5000"
```
Response shape: `{"status":"success","data":{"resultType":"streams","result":[{"stream":{...labels...},"values":[[ts_ns,line], ...]}, ...]}}`. Pipe into `python3 -c '...'` or `jq` to parse.

**LogQL filters:** `|= "literal"` (substring), `|~ "regex"` (regex), `!= "literal"`, `!~ "regex"`. Stack them: `{namespace="hippius-s3-prod",app="gateway"} |= "S3_OPERATION_SUCCESS" |~ "/affine-datasets/" != "list-type"`.

**Audit log shape (gateway):** lines start with `S3_OPERATION_SUCCESS:` followed by JSON with `ray_id`, `account_id`, `client_ip`, `method`, `path`, `query_params`, `status_code`, `processing_time_ms`, `content_length`. Use these for per-account and latency analysis.

**Gotchas:**
- `query_range` caps at `limit=5000` per call. For wider windows, narrow with extra filters first or split the time range.
- Long ranges (>3-4 days) on broad queries can time out — paginate by hour.
- Audit success lines log `processing_time_ms` and response `content_length`. Throughput = `content_length / processing_time_ms`. Streaming responses with chunked transfer may report `content_length` of 0 — filter those out.
- The gateway is the canonical place to query for *user-visible* request behavior (account_id, ACL decisions, processing time). The internal `api` app logs the per-stream details (chunk planning, cache vs pipeline).

---

## 11. Operational runbooks (quick pointers)

- **DLQ requeue**: [hippius_s3/scripts/dlq_requeue.py](hippius_s3/scripts/dlq_requeue.py).
- **Failed pin resubmit**: [hippius_s3/scripts/resubmit_failed_pins.py](hippius_s3/scripts/resubmit_failed_pins.py).
- **Arion identifier migration** (new): [hippius_s3/scripts/migrate_arion_identifiers.py](hippius_s3/scripts/migrate_arion_identifiers.py) + [k8s/migrate-arion-identifiers-job.yaml](k8s/migrate-arion-identifiers-job.yaml). Fixes legacy chunk_backend rows that stored `arion_hash` instead of `path_hash`.
- **Clean prod DB dump for testing**: [scripts/gen_clean_dump.py](scripts/gen_clean_dump.py).
- **MPU retry**: [scripts/retryable-mpu.py](scripts/retryable-mpu.py) with usage notes in [retryable-mpu.md](retryable-mpu.md) (if present).
- **Dangerous scripts** (flagged for a reason):
  - [hippius_s3/scripts/nuke_user.py](hippius_s3/scripts/nuke_user.py) — deletes a user and all their data.
  - [hippius_s3/scripts/purge_buckets.py](hippius_s3/scripts/purge_buckets.py), [purge_source_versions.py](hippius_s3/scripts/purge_source_versions.py).
  - [hippius_s3/scripts/delete_legacy_object_versions.py](hippius_s3/scripts/delete_legacy_object_versions.py).

---

## 12. Testing strategy

- **Unit** ([tests/unit/](tests/unit/)) — pure; mocks DB/Redis/services. Fast, run on every save.
- **Integration** ([tests/integration/](tests/integration/)) — real DB and Redis via docker-compose; mocks external services (Arion, Hippius API, KMS).
- **E2E** ([tests/e2e/](tests/e2e/)) — real API stack with [docker-compose.e2e.yml](docker-compose.e2e.yml): `mock-arion`, `mock-kms`, `mock-hippius-api`, `toxiproxy` (fault injection for resilience tests).
- **Smoke** ([tests/smoke/](tests/smoke/)) — post-deploy verification, run as part of the k8s pipeline.

Test env flags: `HIPPIUS_BYPASS_CREDIT_CHECK=true` is enforced in test env at [config.py:282](hippius_s3/config.py). `RUN_REAL_AWS=1` (or `AWS=1`) routes e2e tests against real AWS for parity checks.

---

## 13. User & contributor preferences

- **PR reviews**: pull the diff into `/tmp`, read files, check if the PR does what it says, flag glaring issues. Propose tests for critical changes. Don't nitpick.
- **PR title + description**: download the diff, read it, produce a short (<70 char) title + body with bulleted changes biggest-to-smallest and a brief conclusion.
- **Commits**: never commit to GitHub unless explicitly asked.
- **Summaries of changes**: when asked, diff the checkout and return a 6-7 word description + 2-3 bullets of 6-7 words each if needed.

---

## 14. Where to go next

Once you've read this file, read [todo.md](todo.md) for:
- The 2026-04-21 postmortem findings and proposed retry hardening.
- The known P1 issues (double FS writes on upload, broken-v5 rows, `copy_service_v5` latent risk).
- Download path optimizations (range-aware backend fetch, prefetch, keep-alive).
- Cache invalidation proposals.
- Dead code candidates.
- Getting-started-as-contributor checklist.
