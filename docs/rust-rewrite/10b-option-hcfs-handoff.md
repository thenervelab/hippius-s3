# 10b — Architecture Option: "Hand off byte storage to `hcfs-server`"

**Status:** scoping doc for ONE greenfield-rewrite option. Companion to the nine subsystem
specs ([00-index](00-index.md)) and the strategy doc (`../rust-rewrite-assessment.md`).
**Date:** 2026-09-15.

> **Scope note.** This is a *fresh build* on a new branch of `hippius-s3`, with its own
> Postgres DB + optimized schema + separate pods; existing data is re-encrypted/migrated later,
> so there is **no in-place bit-compat constraint**. Full vanilla-S3 parity is required. The new
> S3 service performs its **own envelope encryption** and hands **ciphertext** to whatever stores
> the bytes. This doc evaluates making that byte-store the **existing `hcfs-server`
> `StorageBackend`** (Arion / S3 / dual-write) at `/Users/camden/Source/hcfs`.

## The option in one paragraph

The Rust S3 service becomes **protocol + metadata + crypto only**: it terminates SigV4/SigV2,
owns buckets/objects/versions/parts/ACL/multipart state in its own Postgres, encrypts each chunk
with AES-256-GCM, and then, instead of writing bytes to a cache tier + drain + Arion itself, it
**calls `hcfs-server` over HTTP** to persist and retrieve ciphertext blobs. Because the S3 service
encrypts before the hand-off, `hcfs-server` only ever sees ciphertext — consistent with hcfs's
"server never sees plaintext" principle. The central question: **can hcfs's `StorageBackend`
actually serve as a ciphertext-blob store for a full S3 product, and what would hcfs need to
ADD?** Short answer: the *streaming/range/backend* machinery is genuinely reusable and battle-
tested, but hcfs is **not a content-addressed blob store** — it is a *path-keyed, signed-manifest,
per-account-billed file store with no blob refcounting* — so a faithful hand-off needs a new
raw-blob API surface and a refcount table on the hcfs side, or the S3 service must contort itself
into hcfs's FileRecord model.

---

## 1. The hand-off interface

### 1.1 There is no in-process crate seam today — the boundary is HTTP

`StorageBackend` is a concrete `struct` inside the `hcfs-server` binary crate
(`hcfs-server/src/storage/backend.rs:396-402`), not a published library:

```rust
// hcfs-server/src/storage/backend.rs:395-402
#[derive(Clone)]
pub struct StorageBackend {
    arion: Option<Arc<ArionBackend>>,
    s3: Option<S3Backend>,
}
```

Its methods take `hcfs-server`-internal types (`multer::Field<'static>`, `crate::metrics::Metrics`,
`crate::state::AppState`), so it cannot be lifted into a shared crate without first severing those
dependencies. **Extracting a shared `hcfs-storage` crate is possible but is real work** (see §3);
absent that, the S3 service talks to hcfs over the existing HTTP surface.

### 1.2 The HTTP surface the S3 service would drive

Routes are defined in `hcfs-server/src/http/router.rs`. The relevant ones:

| Purpose | Method + path | Handler | Notes |
|---|---|---|---|
| Single-blob upload (S3 dialect) | `POST /upload` (first multipart field `account_ss58`, then `file`) | `upload` → `handle_s3_upload` (`upload.rs:424-636`) | **≤ 16 MiB per object** (`S3_FILE_FIELD_CAP`, `upload.rs:49`); server buffers + BLAKE3-hashes the part itself; keyed by `blake3(filename)` as `path_hash` (`upload.rs:1086-1088`) |
| Native upload | `POST /upload` (first field `manifest`, then `ciphertext`) | `handle_hcfs_upload` (`upload.rs:213-415`) | Requires a **signed `Manifest`** (Ed25519 ToS signature verified, `session.rs:475`); Arion-only streams, S3/`both` buffer whole field |
| Chunked session — create | `POST /upload/session` | `create_upload_session` (`session.rs:451-587`) | JSON body with a full signed `Manifest`, `chunk_count` (≤100 000, `session.rs:193`), `chunk_size`, `ciphertext_size` |
| Chunked session — put chunk | `PUT /upload/session/{id}/chunk/{index}` | `upload_session_chunk` (`session.rs:794-958`) | Raw body, **≤ 16 MiB per chunk** (`CHUNK_BODY_CAP_BYTES`, `session.rs:92`; route limit `router.rs:110`); **server re-hashes the chunk with BLAKE3** (`session.rs:1013`) — caller does not choose the key |
| Chunked session — status / finalize / delete | `GET …/status`, `POST …/finalize`, `DELETE …/{id}` | `session.rs:1234 / 1275 / 1527` | Finalize folds chunk hashes into the FileRecord's `chunk_hashes[]` array |
| Download (range-capable) | `GET /download/{ss58}/{folder_hash}/{file_id}` and `GET /download/{ss58}/{file_id}` | `download` / `download_no_folder` (`file.rs:942 / 926`) | `file_id` = hex `path_hash`; honours `Range:` (`file.rs:993-996`); returns `Content-Length`(ciphertext), `X-Size-Bytes`(plaintext), `X-Revision-*` |
| Delete | `DELETE /delete/{ss58}/{folder_hash}/{file_id}` and `…/{ss58}/{file_id}` | `delete_file` / `delete_file_no_folder` (`file.rs:1055 / 1040`) | **Unconditional blob cleanup, no refcount** (see §2c) |
| Batch delete (S3 `DeleteObjects` dialect) | `POST /delete_files` | `batch_delete_files` (`file.rs:1165`) | 200-on-partial-failure, per-item results, quiet mode |
| **Raw content-hash read (no FileRecord, no auth)** | `GET /public/download/{hash}` | `public_download` (`public.rs:23`) | Streams straight from storage by BLAKE3 hash: `storage.download(&hash, Some(&hash), …, None)` (`public.rs:43-46`). **No `Range` (passes `None`)**, no auth gate |
| Admin S3→Arion repair by hash | `POST /admin/arion-repair/{s3_hash}` | `repair_from_s3` (`router.rs:185`) | Admin bearer only; `stream_from_s3` (`backend.rs:870-890`), no range |

**The public S3-style gateway** (`hcfs/docs/public/api/s3-gateway.md`) is exactly three of these:
`POST /upload` (≤16 MiB, filename-keyed), `GET /download/{ss58}/{file_id}` (Range supported),
`DELETE /delete/{ss58}/{file_id}`. It is single-blob only; **it has no multipart-assembly notion**.

### 1.3 The real `StorageBackend` signatures (what a shared crate would expose)

Quoted verbatim from `backend.rs`:

```rust
// backend.rs:533-538 — native upload (streams for Arion-only; buffers for S3/both)
pub async fn upload(&self, field: multer::Field<'static>, file_id: &str,
                    metrics: &crate::metrics::Metrics) -> Result<StorageUploadResult, StorageError>

// backend.rs:603-609 — store an already-buffered, already-hashed blob (the S3-gateway path)
pub async fn upload_prepared(&self, buf: Vec<u8>, hash: &str, file_id: &str,
                    metrics: &crate::metrics::Metrics) -> Result<StorageUploadResult, StorageError>

// backend.rs:792 — raw bytes → S3 by hash key (no-op if S3 unconfigured); used by chunk PUT
pub async fn upload_chunk_to_s3(&self, data: &[u8], hash: &str) -> Result<(), StorageError>

// backend.rs:705-711 — download single blob, S3-first with Arion fallback, optional Range
pub async fn download(&self, arion_hash: &str, s3_hash: Option<&str>,
                    metrics: &crate::metrics::Metrics, range: Option<&ByteRange>)
                    -> Result<StorageDownload, StorageError>

// backend.rs:808-812 — download one chunk by hash (no range; a chunk IS a whole object)
pub async fn download_chunk(&self, hash: &str, metrics: &crate::metrics::Metrics)
                    -> Result<StorageDownload, StorageError>

// backend.rs:870-874 — stream from S3 by hash, NO Arion fallback (repair primitive)
pub async fn stream_from_s3(&self, hash: &str, metrics: &crate::metrics::Metrics)
                    -> Result<StorageDownload, StorageError>

// backend.rs:774 — best-effort delete from all configured backends (no refcount)
pub async fn delete(&self, arion_hash: &str, s3_hash: Option<&str>)
```

Return/plumbing types: `StorageDownload { stream: Pin<Box<dyn Stream<Item=Result<Bytes,io::Error>>+Send>>,
content_length, source, total_size, is_partial }` (`backend.rs:166-175`), and `ByteRange { start, end }`
with `ByteRange::parse` for `bytes=start-end` / `bytes=start-` (suffix ranges **unsupported**,
`backend.rs:140-154`). Object keys are `{prefix}/{h0..2}/{h2..4}/{hash}` (`hcfs-shared/src/storage.rs`,
`file_s3_key`) — content-addressed at the *physical* S3/Arion layer.

> **Keying mismatch to note up front:** on the streaming native/S3 paths hcfs **computes the
> BLAKE3 hash itself** (`buffer_field_with_blake3`, `backend.rs:1031-1063`; chunk PUT
> `blake3::hash(&body)`, `session.rs:1013`). Only `upload_prepared` and `upload_chunk_to_s3`
> accept a caller-supplied hash. So a hash-addressed hand-off must go through those two entry
> points (or new ones), not through `upload()`.

---

## 2. Fitness assessment

| Capability | Verdict | Evidence |
|---|---|---|
| (a) Store arbitrary ciphertext **chunks by hash** for large/multipart objects | **Partial** | see below |
| (b) Serve **Range** reads | **Yes** | `backend.rs:705-769`, `1533-1580`; `file.rs:556-619` |
| (c) **DELETE with refcounting** for dedup'd blobs | **No** | `store/files.rs:490-528`, `storage/cleanup.rs:1-13,104-136` |
| (d) **Stream** without buffering multi-GB | **Partial** | download yes; S3/`both` single-PUT buffers whole object |
| (e) Keep S3 object metadata **out of hcfs** | **Partial / No (as-is)** | every normal write requires a signed `Manifest` + FileRecord |

### (a) Chunk-native storage by hash — **Partial**

hcfs genuinely has a chunk-native model: `FileRecord.chunk_hashes: Option<Vec<String>>` +
`chunk_sizes: Option<Vec<i64>>` (`store/types.rs:36-38`), populated by the chunked-session
finalize (`session.rs:1295-1327`). The chunk PUT (`PUT /upload/session/{id}/chunk/{i}`) writes each
chunk to the content-addressed S3 key and background-pushes to Arion (`session.rs:1006-1132`), and
finalize assembles them (`session.rs:1275-1380`). Ranged download reassembles across chunks
(`file.rs:556-619`).

**But it is not a free blob-by-hash store.** Three frictions:

1. **Every chunk lives under a session, and every session under a signed FileRecord.** You cannot
   PUT a chunk without first creating a session (`POST /upload/session`) carrying a full
   Ed25519-signed `Manifest` (`session.rs:475`, `build_session_row` `session.rs:699`). The chunk
   is staged in `upload_session_chunks` (FK-cascade to `upload_sessions`, migration
   `20260324000000_upload_sessions.up.sql`) and only becomes durable when finalize folds it into
   one `file_records` row keyed `(user_id, path_hash)`.
2. **The server chooses the chunk key, not the caller.** `store_chunk_body` re-hashes with BLAKE3
   (`session.rs:1013`). The S3 service's own content-addressing (or per-part digest) is ignored;
   it must read back the hash hcfs assigns.
3. **Hard caps:** ≤ 16 MiB per chunk (`session.rs:92`), ≤ 100 000 chunks/session (`session.rs:193`).
   16 MiB × 100 000 ≈ 1.5 TiB max object — fine for S3's 5 TiB ceiling only if the chunk cap is
   raised. The single-shot S3-gateway path is ≤ 16 MiB *per object* (`upload.rs:49`), so large
   objects **must** use the session protocol or be split by the S3 service.

Net: the plumbing exists, but it is wrapped in session + manifest + FileRecord ceremony that a
blob store should not impose. For multipart specifically, hcfs's "chunk" ≠ S3's "part": hcfs
chunks are a fixed-size internal envelope detail, whereas S3 parts are client-chosen (5 MiB–5 GiB)
and each needs its own ETag. The mapping is not 1:1 (see §5, ETag).

> **This is already half-live.** The current hippius-s3 gateway path **already posts one 4 MiB
> AES-GCM cipher chunk per `POST /upload` request to hcfs** (`hcfs/docs/public/api/s3-gateway.md:36`;
> hippius default `HIPPIUS_CHUNK_SIZE_BYTES` = 4 MiB, doc 03 §6) — one hippius chunk ⇄ one hcfs
> `file_id`. So a degenerate form of "encrypt in the S3 layer, hand a ciphertext blob to hcfs,
> keyed by hcfs's content hash" runs in production today. The rewrite would *formalize and
> generalize* that mapping (multipart, versioning, refcounting, larger chunks), not invent it — a
> real de-risker for the write path. It also means the live product already inherits hcfs's
> no-cache-tier read behavior for anything hcfs serves (§5.2).

### (b) Range reads — **Yes**

Solid and well-tested. Single-blob: `download()` issues a native S3 Range GET whose HTTP status is
known before any body byte (`download_s3_range`, `backend.rs:1533-1580`; `get_object_range_stream`
via `Command::GetObjectRange`, `backend.rs:1476-1485`), with Arion Range fallback
(`download_from_arion_gateway` forwards the `Range` header, `backend.rs:1342-1344`). Chunk-native:
`stream_chunked_download` computes the start chunk + intra-chunk skip and slices
(`file.rs:566-619`, `take_usable_bytes` `file.rs:386-409`), emitting `206` + `Content-Range`
(`file.rs:785-791`). Caveat: only `bytes=start-` / `bytes=start-end`; **suffix ranges
(`bytes=-N`) are not supported** (`backend.rs:140-154`) — the S3 service would translate suffix
ranges itself (it knows the object size from its own DB) before calling hcfs.

### (c) Delete with refcounting — **No** (biggest gap)

There is **no content-hash-keyed blob table and no refcount anywhere** in hcfs
(exhaustive table inventory + `grep refcount|ref_count|reference_count` returns nothing). A blob's
lifetime is tied 1:1 to a single `file_records` row (PK `(user_id, path_hash)`,
`migrations/20260313000000_initial_schema.up.sql:14`). On delete, the store returns the row's blob
hashes (`delete_file` `store/files.rs:490-528`) and the handler **unconditionally** fire-and-forget
deletes every named blob (`file.rs:1127` → `cleanup::delete_record_blobs`), with the cleanup module
explicitly documenting "an orphaned blob is already tolerated" (`storage/cleanup.rs:1-13`). For a
chunk-native file, cleanup deletes **each chunk hash** as both an Arion CID and an S3 key
(`cleanup.rs` `record_targets`). `delete_session` likewise deletes chunk blobs directly
(`session.rs:1560-1572`).

**Consequence for an S3 product:** S3 CopyObject fast-paths, versioning, and cross-object dedup all
create the situation where two logical objects reference one ciphertext blob. Under hcfs today,
deleting either one drops the shared bytes out from under the other. A dedup/version-safe S3
service **cannot** use hcfs's delete as-is; it must either (i) never dedup across hcfs objects
(store a full copy per logical object, forfeiting dedup), or (ii) hcfs must gain a refcounted blob
table (see §3).

### (d) Stream without buffering multi-GB — **Partial**

- **Downloads: yes.** Zero-copy streaming end-to-end (`Body::from_stream`, `file.rs:694/614`);
  chunk-native uses a bounded 16-slot mpsc with single-chunk prefetch so peak memory is ~2 chunks
  (`file.rs:599`, `pump_chunks` `file.rs:303-367`).
- **Uploads: depends on backend.** Arion-only `upload()` streams the multipart field straight to
  the gateway (`backend.rs:540-543`). But **S3-only and `both` buffer the entire object in memory**
  before the S3 `put_object` (`buffer_field_with_blake3`, `backend.rs:575`, `1147-1156`) — a
  multi-GB single PUT to an S3/`both` deployment would blow the pod's memory. The native path caps
  the body at what `size_bytes` can encrypt to (`upload.rs:128-133`) but does not stream it to S3.
  The only constant-memory large-file path is `upload_from_file_for_migration` (8 MiB multipart
  from a tempfile, `backend.rs:644-701`) — explicitly labelled temporary ("delete when migrations
  are complete", `backend.rs:643`). The production answer for large objects is the **chunked
  session** (≤16 MiB buffered per chunk) — which streams in the aggregate but re-imposes the
  session ceremony.

There are also **memory budgets** in the data path worth knowing: background Arion pushes are gated
by a 2 GiB byte-budget semaphore (`ARION_BG_BLOB_BUDGET`, `backend.rs:49`; `ARION_BG_CHUNK_BUDGET`,
`session.rs:75`) with skip-to-retry-worker on exhaustion. These are hcfs's own OOM guards from a
prod incident (`backend.rs:34-60`) — an S3 workload would inherit that behavior, not bypass it.

### (e) Keep S3 metadata out of hcfs — **Partial / No as-is**

hcfs is **not** a pure blob store; it *is* a metadata store. Every normal write creates a
`file_records` row with hcfs's own model: `user_id` (= ss58 `[_folderhash]`, `utils.rs:101-107`),
`path_hash`, `salted_hash`, `size_bytes` (plaintext), `revision_seq`/`revision_id` (optimistic
concurrency), `encrypted_path`, `file_name`, `relative_path`, `uploaded_by_ss58`
(`store/types.rs:18-54`). It enforces a **signed manifest** on native + session writes, runs its
own **optimistic-concurrency** (`base_revision_id`/409, `upload.rs:1016-1083`), and does its own
**per-account billing** (§below). None of that is what an S3 service — which owns
buckets/objects/versions/parts/ETags/user-metadata/object-lock in *its own* Postgres — wants
duplicated.

You *can* keep the interesting S3 metadata (ETag, content-type, `x-amz-meta-*`, ACLs, version-id,
object-lock) entirely in the S3 service's DB — hcfs neither needs nor stores those. **But you
cannot avoid hcfs storing a parallel, redundant FileRecord per blob** unless you use the
metadata-light read path (`/public/download/{hash}`) plus new raw-blob write/delete endpoints.
As-is, the honest verdict is: S3-specific metadata stays out, but a *shadow* per-object row
(path_hash, size, revision, billing attribution) is forced into hcfs on every write.

---

## 3. What hcfs must ADD to be a solid S3 backend

Sized as S (≤ few days), M (~1–2 weeks), L (multi-week), per item.

| # | Gap | What to add | Size |
|---|---|---|---|
| 1 | **Blob refcounting** (the load-bearing gap) | A content-hash-keyed `blobs(hash PK, refcount, size, backends, created_at)` table; increment on reference, decrement on delete, physical delete only at refcount 0. Touches `store/files.rs` delete paths and `storage/cleanup.rs` (both delete unconditionally today). | **L** |
| 2 | **A raw-blob API** decoupled from FileRecord/Manifest | `PUT /blob/{hash}` (caller-supplied hash, ciphertext body, streamed to S3 *and* Arion), `GET /blob/{hash}` **with Range**, `HEAD /blob/{hash}`, `DELETE /blob/{hash}` (refcount-aware). Today the only hash-addressed read is `/public/download/{hash}` (no auth, no range, `public.rs:23`). This is the clean seam the S3 service actually wants. | **L** |
| 3 | **Streamed large single-PUT to S3/`both`** | Replace the buffer-whole-object path (`backend.rs:575`, `1147-1156`) with the tempfile/multipart streaming already proven in `upload_from_file_for_migration` (`backend.rs:644-701`), promoted from "temporary" to a first-class path. Without this, S3/`both` deployments OOM on multi-GB PUTs. | **M** |
| 4 | **A clean service-to-service auth path** | Today there is **no account-agnostic *attributed* service credential**. A user token resolves to one ss58 via the upstream `HCFS_AUTH_VERIFY_URL` (`gates.rs:200-272`) and may only touch its own namespace; the single global admin bearer (`HCFS_ADMIN_BEARER_TOKEN`, "S3 gateway access" `gates.rs:78`, `main.rs:120`) is the *only* cross-account credential but writes **unattributed** (`caller_ss58 = None`) and is one high-value secret. Add a real service identity (mTLS or scoped service token) that can act on behalf of many end-user ss58s **with attribution and per-tenant isolation**. | **M** |
| 4a | Raise/parameterize caps | ≤16 MiB chunk (`session.rs:92`), ≤16 MiB S3-gateway object (`upload.rs:49`), ≤100 000 chunks (`session.rs:193`) — make these config so 5 GiB parts / 5 TiB objects fit. | **S** |
| 5 | **Usage attribution for S3 accounts** | hcfs billing keys everything on the ss58 and derives an S3-vs-Drive "rail" from the summary key, not the route (`HCFS-BILLING.md` §4.0). If the S3 service uses per-end-user ss58s, hcfs's `user_summaries` + chain-reporter already attribute correctly (bare-row = S3 rail, `HCFS-BILLING.md` §6.2, `BillingSubject::s3_gateway` `upload.rs:487`). If it uses one service ss58 (admin/exempt), **all tenants collapse into one usage row** — you lose per-tenant accounting and must attribute in the S3 service's own DB. Decide the identity model (item 4) first; this follows from it. | **M** |
| 6 | *(optional)* Extract a `hcfs-storage` crate | Sever `StorageBackend` from `multer::Field`, `crate::metrics`, `crate::state` so it can be a library the S3 service links directly (kills the network hop for co-located pods). Meaningful refactor. | **L** |

The **critical path is items 1 + 2 + 4**: without a refcounted raw-blob API and a proper service
credential, hcfs is being used against its grain (path-keyed, signed-manifest, per-account, no
dedup safety) and the S3 service inherits correctness hazards (double-delete of shared blobs) plus
a single-secret blast radius.

---

## 4. What the S3 service AVOIDS building by choosing this

This is the real upside. hcfs already operates the entire "get ciphertext durable on Arion + S3,
and keep it durable" pipeline. The greenfield S3 service would **not** build:

| Subsystem the rewrite would otherwise build (see [03](03-data-plane-cache-streaming.md)/[04](04-queues-and-workers.md)) | Covered by hcfs? | How |
|---|---|---|
| **Backend write** to Arion + S3 (incl. dual-write) | **Yes** | `StorageBackend` arion/s3/both arms (`backend.rs:499-523`); dual-write `upload_to_both` (`backend.rs:1147`) |
| **The drain / uploader** (SSD→backend replication, `arion-uploader`, `run_arion_uploader_in_loop.py`) | **Mostly** | Not needed at all in the naive model — hcfs writes S3 synchronously and pushes Arion in-band; the *retry* worker covers the async tail (below) |
| **Retry of failed Arion uploads** | **Yes** | `hcfs-retry-worker`: scans `failed_uploads` oldest-first (`worker.rs:184-199`), streams the object back from the S3 backup bucket, re-POSTs to a round-robin Arion gateway, handles chunk-native rows (`worker.rs:587-674`), dead-letters after 50 attempts / 7 days. This is hcfs's "drain" equivalent. |
| **Unpinner** (delete-from-backend on object delete) | **Partial** | hcfs `delete()` best-effort deletes from Arion + S3 concurrently (`backend.rs:774-788`), backgrounded (`cleanup.rs`). But **no refcount** (§2c) and no durable unpin queue/DLQ like hippius-s3's `arion-unpinner` (`unpinner.py`) — a failed delete just orphans (tolerated by design). |
| **Chain / usage reporting to substrate** | **Yes** | `hcfs-chain-reporter`: ~6s ticks, scans dirty `user_summaries`, submits `pallet_marketplace::update_users_file_usage` + `pallet_arion::update_multiple_user_file_sizes` (`main.rs:47-52`, `loop_.rs:183-193`). Replaces hippius-s3's chain/accounting concerns ([07](07-chain-and-accounting.md)). |
| **Billing/quota gate** | **Yes (if per-ss58)** | `validate_billing` plan+credit gate per ss58 (`HCFS-BILLING.md` §4); S3 rail already modelled (`BillingSubject::s3_gateway`). |
| **Round-robin across Arion gateways, egress metrics, shadow-benchmark reads** | **Yes** | `ArionBackend` gateway pool (`backend.rs:380-385`), OTel egress metrics throughout, Arion shadow-read on every S3-served download (`backend.rs:720-726`). |

**What is NOT covered — and this is the whole point of hippius-s3's data plane:**

| hippius-s3 subsystem | Covered by hcfs? |
|---|---|
| **Multi-tier read cache: node-local NVMe → peer NVMe → CephFS pool → backend** (doc 03 §4, `dual_fs_store.py`, tiers at 03:391-393) | **No.** hcfs reads go **straight to S3, then Arion fallback** (`backend.rs:714-760`). No SSD tier, no peer fetch, no pool. |
| **Promotion / residency / pressure bands** (`_promote_chunk`, 03:524-547) | **No.** |
| **Redis durable work queues + retry ZSETs + DLQ** (5 Redis instances, doc 04 §1) | **No** (hcfs uses a Postgres `failed_uploads` table + a polling worker instead — simpler, coarser). |
| **~13 purpose workers** (purger, mpu-reaper, janitor, orphan-checker, account-cacher, plans-cacher, doc 04) | **No** — most are hippius-s3-specific concerns (MPU lifecycle, cache janitoring) that either move into the S3 service or disappear. |

So the hand-off eliminates the *backend-durability + retry + chain + billing* half of the data
plane, but it **does not give you a performance cache tier** — see §5.

---

## 5. Tradeoffs (candid)

1. **Extra network hop + latency.** Every GET/PUT becomes an intra-cluster HTTP round trip S3-svc
   → hcfs-svc → S3/Arion, instead of S3-svc → local NVMe (a cache hit in hippius-s3 today is a
   local file read). On the write path hcfs *buffers then hashes* for S3/`both`
   (`backend.rs:1147-1156`), adding a full-object memory copy + serialized S3 PUT before ACK. This
   is strictly slower than hippius-s3's "write to SSD, ACK, drain async" model
   (doc 03 §1: "the response body does ZERO DB work"). Item 6 (in-process crate) removes the hop
   but not the buffering.
2. **Loss of the SSD→Ceph performance tier — the biggest architectural regression.** hippius-s3's
   read latency comes from the NVMe/peer/pool cache (doc 03 §4); a cold read that falls through to
   Arion is the slow exception there. Under this option **every** read is that cold path — hcfs
   goes straight to S3/Arion with no local flash (`backend.rs:714-760`). And a **1-byte Range on a
   cold chunk still transfers the whole chunk from Arion** (doc 03:456-461, and hcfs's
   `download_chunk` fetches a whole chunk object, `backend.rs:808-863`) — so range-heavy workloads
   (media seeking, `mmap`-style clients) get no help. Recovering parity means the S3 service builds
   its *own* cache in front of hcfs — at which point much of hippius-s3's data plane comes back and
   the "protocol + metadata + crypto only" framing weakens.
3. **Availability coupling.** The S3 service's data plane is only as available as hcfs-server. An
   hcfs deploy, restart, or the 2 GiB Arion-bg budget saturating (`backend.rs:49`) now shows up as
   S3 5xx/latency. Two services must be released and on-called together.
4. **Internal-API versioning.** The `POST /upload` multipart dialect, the session state machine,
   the `X-*` download headers, and the `file_s3_key` layout become a **cross-service contract**.
   hcfs evolves them for its Drive/desktop clients on its own cadence; the S3 service pins to them.
   The chunk-key-is-server-chosen behavior (§1.3) and the "server re-hashes" invariant are exactly
   the kind of implicit contract that breaks silently.
5. **ETag / content-addressing impedance.** S3 ETags are **MD5-based** — `"<md5>"` for simple
   PUT, `"<md5>-<parts>"` for multipart (doc 06:166-217, 195-200), and Content-MD5 verification is
   a conformance requirement (doc 06 §3). hcfs is **BLAKE3** content-addressed and exposes no MD5.
   So the S3 service must compute + store MD5/ETag in its own DB regardless (fine — reinforces
   "S3 metadata stays out of hcfs"), but it means hcfs's content hash is *not* reusable as the
   ETag, and multipart ETag assembly (per-part MD5 → composite) is entirely the S3 service's job.
6. **Model impedance overall.** hcfs's signed-manifest + optimistic-concurrency + path-hash model
   is designed for a *sync* client, not an *object-store* client. Every write the S3 service makes
   is squeezed through machinery (Ed25519 manifest verify, `base_revision_id` 409s, plaintext-size
   envelope checks) it does not want. Bypassing that (raw-blob API, item 2) is precisely the work
   that makes hcfs *stop being hcfs* for this caller.

---

## 6. Net assessment

**Total build size.**

- **S3 service (this option):** it still builds the entire S3 front half — SigV4/SigV2, the ~60
  actions + XML + error catalog (doc 06), buckets/objects/**versions**/multipart/ACL/object-lock in
  its own Postgres (doc 02), AES-256-GCM envelope + KMS (doc 01), and — to not regress on latency —
  most likely its **own read cache**. What it *saves* is the backend-durability tail: no drain, no
  uploader, no Arion retry, no chain reporter, no billing gate (if per-ss58). Rough shape: the
  hand-off removes maybe the "storage engine backend + queues/workers + chain" third of the effort
  ([03]/[04]/[07]) **but only if you accept the cache-tier regression**; keep parity on latency and
  you claw much of it back.
- **hcfs side:** the must-do list (§3 items 1, 2, 4) is **two L's and an M** — a refcounted blob
  table, a raw-blob Range-capable API, and a proper attributed service credential — plus streamed
  large PUTs (M) and cap parameterization (S). Realistically **4–8 weeks** of hcfs work before hcfs
  is a *correct* general blob backend rather than a path-keyed file store used off-label.

**Biggest risks (ranked):**

1. **Blob refcounting (§2c).** Using hcfs's unconditional, orphan-tolerant delete under an S3
   product with versioning/copy/dedup **will** drop live bytes. This is a correctness bug, not a
   perf one, and it is the single hardest thing to bolt on because it changes hcfs's data model.
2. **Loss of the SSD/Ceph cache tier (§5.2).** The performance profile of the product regresses to
   "cold Arion read every time" unless the S3 service rebuilds a cache — which undermines the
   option's premise.
3. **Service credential / multi-tenant attribution (§3 item 4/5).** The only cross-account hcfs
   credential today is one global unattributed admin bearer. Getting per-tenant isolation +
   attribution + billing right is a prerequisite, not a detail.
4. **Availability + versioning coupling (§5.3/5.4)** turning two independently-evolving services
   into one release/on-call unit joined by an implicit multipart-HTTP contract.

**Bottom line.** hcfs's `StorageBackend` is a *good, proven* Arion+S3 dual-write engine with
first-class streaming, real Range support, a retry worker, and chain/billing already wired — and
reusing it deletes a genuine chunk of backend work. But hcfs is **not, today, a content-addressed
refcounted blob store**, and it carries **no local cache tier**. Made fit for purpose it needs a
new raw-blob API + a refcount table + a service identity (weeks of hcfs work), and even then the S3
product either accepts a latency regression or rebuilds a cache in front — at which point the clean
"just hand bytes to hcfs" story is muddier than it first appears. The write path is meaningfully
de-risked by the fact that hippius-s3 **already** hands 4 MiB cipher chunks to the hcfs gateway in
production (a degenerate one-chunk-per-file version of this option). Reasonable as a **Phase-1
accelerator** (ship on hcfs's backend, defer the cache tier), risky as the **permanent** data-plane
architecture.

---

## Open questions

- **Refcount migration:** if hcfs adds a blob table, how do existing `file_records` (millions of
  rows, blob hashes inline) get back-filled into refcounts without a stop-the-world scan? (Mirrors
  the [02] "who owns migrations" question.)
- **Chunk vs part:** should the S3 service map one S3 *part* to one hcfs *session chunk* (needs the
  16 MiB cap raised to 5 GiB, item 4a) or shard each part into 16 MiB hcfs chunks and track the
  mapping itself? The former simplifies ETag-per-part; the latter fits current caps.
- **`/public/download/{hash}` semantics:** it has no auth and no Range (`public.rs:23-46`). Is a
  hardened, Range-capable, service-authed variant acceptable to hcfs owners, or does the S3 service
  always go through per-object `/download/{ss58}/{file_id}` (and thus always carry a FileRecord)?
- **Does hcfs's `both` backend's in-band Arion push (2 GiB budget, skip-to-retry) give acceptable
  Arion durability latency** for an S3 SLA, or does the retry-worker tail (default 300s interval,
  prod ~5s) leave too long a window where Arion lacks the blob (`backend.rs:44-48` notes the
  optimistic hash "points at a blob Arion does not hold yet")?
- **Attribution model:** confirm whether the product wants per-end-user ss58 identities (clean hcfs
  billing/chain attribution, needs a token the upstream auth service can mint per tenant) or a
  single service account (simple, but collapses all tenants into one usage row — item 5).
- **Suffix Range + cold-range fetch:** the S3 service must translate `bytes=-N` itself; and is a
  range-aware Arion fetch (avoid pulling a whole chunk for a 1-byte range) in scope for hcfs, or
  accepted as a known inefficiency (it is a standing open question on the hippius-s3 side too,
  doc 03 Open questions)?

---

*Cross-repo references: `hcfs` = `/Users/camden/Source/hcfs`; `hippius-s3` = this repo. All
`backend.rs`/`session.rs`/`upload.rs`/`file.rs`/`public.rs` paths are under
`hcfs/hcfs-server/src/`. Worker refs under `hcfs/hcfs-retry-worker/src/` and
`hcfs/hcfs-chain-reporter/src/`. Billing refs: `hcfs/docs/HCFS-BILLING.md`.*
