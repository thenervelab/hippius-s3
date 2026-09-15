# 10a — Option "Arion-direct": the Rust S3 service owns the full data plane

**Status:** scoping / effort estimate for ONE architecture option. Not a decision.

**The option in one line.** The greenfield Rust `hippius-s3` service reproduces the current
Python topology: the API lands ciphertext chunks on **node-local SSD**, a **drain** subsystem
verifies + hashes each part and hands it off, and a **node-local uploader** pushes the chunks to
**Arion** over HTTP (Arion owns the on-chain pin). The service does its **own** envelope
encryption and never hands plaintext to Arion. The central question this doc answers: **how much of
the already-Rust `crates/hippius-drain-{core,agent,allocator}` can this new service reuse, what must
be refactored or rebuilt, and what else must be built.**

This is a greenfield build on a new branch with its **own** Postgres DB, an optimized schema, and
its own pods; existing data is re-encrypted/migrated in later, so there is **no in-place
bit-compat constraint** on stored bytes. But note the cross-service wire/table contracts below
still bind if the new service shares an Arion backend, a redis-queues instance, or the same drain
tables with anything else during migration.

> **Sources.** Read against `docs/rust-rewrite/03-data-plane-cache-streaming.md` (write/read/cache
> pipeline), `docs/rust-rewrite/04-queues-and-workers.md` (the 5-Redis + workers), `07-chain-and-accounting.md`
> (CID + chain), `02-storage-engine-schema.md` §2.18 (drain-owned tables), the three drain crates,
> `hippius_s3/services/arion_service.py`, `workers/uploader.py`, `workers/unpinner.py`, the
> `hippius_s3/cache/*` modules, and the HCFS API docs (`/Users/camden/Source/hcfs/docs/public/api/`).

---

## 0. Premise correction (read first)

The task framing — *"SSD landing → drain replicates to CephFS → uploader pushes chunks to Arion"* —
is **stale**. The CephFS byte-copy step no longer exists in the drain code:

- `crates/hippius-drain-agent/src/localfs.rs:6` — *"There is no pool side any more."*
- `crates/hippius-drain-core/src/partdrain.rs:4-6` — *"Draining a part no longer copies it anywhere."*

The **current** data plane is:

1. API encrypts and writes each part to node-local SSD in the layout
   `<root>/<object_id>/v<version>/part_<n>/chunk_<i>.bin` + `meta.json` (`fs_store.py`;
   drain mirror `localfs.rs`).
2. **`drain_part`** (`partdrain.rs`) does **no copy** — it runs completeness gate → SHA-256 each
   chunk → `LPUSH` an `UploadChainRequest` to Redis → record residency → commit `uploading`.
3. The **node-local uploader** (`workers/uploader.py`) reads the *same* SSD tree and `POST`s each
   ciphertext chunk to Arion, writes `chunk_backend` rows, then flips the drain row to `replicated`.
4. The SSD copy is **retained** as the warm read tier (local NVMe → peer node → CephFS pool →
   backend). CephFS survives only as the optional **pool** cache tier (`HIPPIUS_OBJECT_CACHE_FALLBACK_DIR`),
   and even that is marked "pool era, TODO delete" in several places (`enqueue.rs` `pool()`,
   doc 03 §Open-questions #4).

So "Arion-direct" is really **SSD-landing + warm-cache + async uploader → Arion**, not a Ceph
replication pipeline. This changes the operational footprint (§4) and what the allocator is even
for (§2).

---

## 1. Arion API contract the service would call

The "Arion" backend is reached through the HCFS S3-gateway HTTP surface (`arion.hippius.com`).
Authoritative client: `hippius_s3/services/arion_service.py` (`ArionClient`); server side:
`/Users/camden/Source/hcfs/docs/public/api/{upload,download,delete,can-upload,s3-gateway}.md`.

### 1.1 Transport & auth

| Aspect | Value | Source |
|---|---|---|
| Base URL | `HIPPIUS_ARION_BASE_URL` (default `https://arion.hippius.com/`) | `arion_service.py:226,302` |
| Client | one process-wide `httpx.AsyncClient`, timeout 60s (connect 10s), `follow_redirects=True`, TLS verify per `HIPPIUS_ARION_VERIFY_SSL` | `arion_service.py:304-312` |
| Auth headers (every call) | `X-API-Key: {ARION_SERVICE_KEY}`, `Authorization: Bearer {ARION_BEARER_TOKEN}` | `arion_service.py:326-339` |
| Optional | `X-Hippius-Bypass-Rate-Limiting: {ARION_RATE_LIMITING_PROXY_BYPASS_KEY}` | `arion_service.py:337-338` |
| Optional (uploader only) | `X-Billing-Bypass: {ARION_BILLING_BYPASS_KEY}` when `payload.bypass_billing` set | `uploader.py:202-210` |

### 1.2 Endpoints

| Op | Method + path | Request | Response | Notes / traps |
|---|---|---|---|---|
| **Upload** (one *ciphertext chunk*) | `POST /upload` | `multipart/form-data`: field `account_ss58` (first, text) + field `file` (`(file_name, bytes, "application/octet-stream", {"Content-Length": len})`). `file_name = str(chunk_id)` where chunk_id = `part_chunks.id` | `{upload_id, timestamp, size_bytes, file_id, arion_hash?}` (`UploadResponse`) | **One POST per chunk**, not per part or object. Max 16 MiB/part (live sends ≤4 MiB+28B). First field must be `account_ss58` (routes to the S3-gateway handler vs the native-manifest handler). `arion_service.py:471-522`, s3-gateway.md |
| **Download** (one chunk) | `GET /download/{account_ss58}/{file_id}` | streaming; optional `Range: bytes=…` | `200`/`206` raw ciphertext; headers `Content-Length` (cipher), `X-Size-Bytes` (plain), `Accept-Ranges`, `Content-Range` | **Range IS supported** (`206`) — see §5 "Range-aware fetch". `arion_service.py:435-469`, download.md/s3-gateway.md |
| **Download-multi** | *does not exist* | — | — | No batch/multi-download endpoint in the client or HCFS router (`hcfs-server/src/http/router.rs`). Cold reads fetch chunks **one GET each**; concurrency comes from the reader's prefetch window (§3.5 of doc 03), not a batch endpoint. Flagged as an Open question. |
| **Delete** (single) | `DELETE /delete/{account_ss58}/{file_id}` | — | `{Success:{status,file_id,user_id}}` | **404 == success** (idempotent) — must be reproduced or the unpinner zombie-loops (`arion_service.py:370-378`). |
| **Delete** (batch) | `POST /delete_files` | `{ss58_address, folder_hash, file_ids:[≤1000], quiet:false}` | `{Success:{deleted:[{file_id,status}], errors:[{file_id,code,message}], files_deleted}}` (HTTP 200 even on partial) | Hard cap **1000** file_ids. **404 → `BatchEndpointUnavailable`** (a non-`HippiusAPIError` so it skips the retry decorator) → caller falls back to per-file. A file_id absent from *both* `deleted` and `errors` = transient failure, never a silent soft-delete. `arion_service.py:384-433` |
| **Billing gate** | `POST /can_upload` | `{user_id, size_bytes}` | `{result:bool, error?}` | Runs on the request/admission path; short timeout `CAN_UPLOAD_TIMEOUT_SECONDS` (3s). `arion_service.py:530-567` |

### 1.3 Retry discipline (do not double-retry)

`retry_on_error` (`arion_service.py:194-271`): 3 retries / 5s fixed backoff on
`HTTPStatusError`/`HippiusAPIError` only. **Not** retried: 401/403 (→ auth error), 404, 507, and all
transport errors (`ConnectError` etc.). Transport failures are left to the **worker's Redis retry
ZSET** (exponential + jitter, survives restarts). Reproducing both layers multiplies into ~24
requests at a failing backend — keep the split.

---

## 2. Drain reuse table

Read of `crates/hippius-drain-core` (20 modules, 21 migrations), `crates/hippius-drain-agent`
(11 modules), `crates/hippius-drain-allocator`.

**Shape of the crates.** `drain-core` is a clean split: a **pure default-feature core** (types +
algorithms, no I/O, no schema) and two **feature-gated I/O shells** — `pg` (`store.rs`, all the
schema coupling) and `coord` (`coordination.rs`/`tick.rs`, all the Redis coupling). `drain-agent`
is the tokio/nix/redis daemon that implements the core's traits; `drain-allocator` is the
Ceph-write-budget singleton. The pure core carries **no** `NODE_NAME`/DaemonSet/hostname hardcoding
— node identity is a `NodeId` **value** you pass in (`ids.rs`). All cephor/Redis/Ceph coupling is
in the feature-gated shells and the agent/allocator binaries.

### 2.1 `hippius-drain-core`

| Module | Verdict | Reason |
|---|---|---|
| `units.rs`, `state.rs`, `ids.rs`, `clock.rs`, `error.rs` | **Reusable as-is** | Pure value types / time seam. `state.rs` enums (`ReplicationState`, `PressureZone`, `CephCeiling`) are vocabulary; `CephCeiling` is just Open/Throttle (rename optional). `ids.rs::NodeId` is a value, not K8s-bound. |
| `apipart.rs` | **Reusable as-is IF you adopt the same part/chunk dir layout** | The FS-layout contract: `PartKey`, `PartMeta`, `META_FILE_NAME`, `chunk_file_name`, `relative_dir`, `parse_part_dir` → `<object_id>/v<version>/part_<n>/chunk_<i>.bin` + `meta.json`. Treats chunks as **opaque files** — ciphertext fits unchanged. Assumes canonical-UUID object ids (`apipart.rs:40-58`). Strongest single reuse candidate. |
| `alloc.rs` | **Reusable as-is** | Pure integer mClock write-budget allocator, overflow-guarded, proptested. Generic — only needed if you keep a global write-budget controller (§4). |
| `enforce.rs` | **Reusable as-is** | Pure circuit-breaker → token-bucket → concurrency-limiter over injected `Instant`. Generic local rate valve. |
| `redrive.rs` | **Reusable as-is** (light refactor) | The `part_digest` fold + reland verdicts that detect a silently-swapped ciphertext (raced `UploadPart`). **Directly valuable to a service doing its own envelope encryption** — this is the safety net for the per-version-DEK/AEAD hole. Byte-for-byte golden-pinned with the Python `part_digest.py`. |
| `snapshot.rs` | **Reusable as-is** (trivial) | `AtomicU64` counters + diffing; metric names are drain-flavored but adaptable. |
| `partdrain.rs` | **Reusable with refactor** | The crash-safe per-part drain state machine + seams `PartSource`/`PartReplicationStore`/`UploadEnqueuer`. Pure orchestration, but encodes Hippius's exact lifecycle (verify whole → hash → hand off to a *separate* uploader → claim residency → commit `uploading` → confirm later via `chunk_backend` coverage). Reusable only if the new service adopts that same hand-off lifecycle; you implement the traits. |
| `reconcile.rs` | **Reusable with refactor** | Reconciler backstop (scan SSD, record `pending` for any complete part with no row). Encodes the residency-vs-replication-row ownership + meta.json completeness gate. |
| `ssd_evict.rs` | **Reusable with refactor** | LRU read-tier evictor keeping an NVMe free floor; only unlinks `replicated`/row-less parts. Tied to the residency-table model. |
| `ssd_reclaim.rs` | **Reusable with refactor** | Debris backstop for aborted/abandoned/deleted-orphan SSD parts, gated on unservable version. Tied to failed/corrupt/servable model. |
| `gc.rs` | **Reusable with refactor** | `gc_object` reclaims a terminal file's folder; `CephFs`/`SsdCache` traits are generic ("remove a folder"). `gc-cephfs` feature gates the pool half (ships off). Production `GcClaim` comes from `store.rs` (coupled). |
| `mgr.rs` | **Not reusable** (unless you gate on Ceph) | Parses Ceph-mgr Prometheus metrics (`OSD_NEARFULL`, `ceph_pool_percent_used`) into a ceiling. Pure, but wholly Ceph-fullness domain. |
| `coordination.rs` (`coord`) | **Reusable with refactor** | Redis leader lease + heartbeats + per-node allocations, epoch-fenced via Lua. Key prefix hardcoded `cephor:*`, `cephor:promote_floor:*` (`coordination.rs:36-41`). Generic if you rename the prefix; only needed with the allocator/multi-node fleet design. |
| `tick.rs` (`coord`) | **Reusable with refactor** | Allocator control loop; Ceph enters only via the pluggable `CephCeilingSource` (a `StaticCeiling` is provided). |
| **`store.rs` (`pg`)** | **NOT reusable — the hard blocker** | ~4768 lines of **inline runtime SQL** maximally coupled to (a) the `cephor_*` schema it owns, (b) the hippius-s3 API's Postgres schema it JOINs, and (c) the Arion multi-backend model. See below. |

**`store.rs` coupling detail.** Owns `cephor_replication_status`, `cephor_ssd_residency`,
`cephor_gc_state`, `cephor_claim_seq` (migrations `0001-0021`, applied via `sqlx::migrate!` at
`store.rs:386`). **Reads/JOINs foreign API tables** `parts`, `part_chunks`, `chunk_backend`,
`object_versions.address`, `objects.object_key`, `buckets.bucket_name`, `multipart_uploads`
(`load_upload_context` `store.rs:1209-1243`). Encodes backend-coverage confirm with string literals
`"arion"`/`"ovh"` (`store.rs:4304-4360`). **The reuse contract here is the trait, not the impl:** a
greenfield service writes its own store against its own schema, implementing the pure traits
`PartReplicationStore`/`PartLandingLog`/`ResidentLog`/`ReclaimLog` (defined in
partdrain/reconcile/ssd_evict/ssd_reclaim). Because the new build has its **own** DB, it can adopt
the `drain-core` migrations wholesale as its starting schema — but it still must supply an S3
metadata schema for the tables `store.rs` currently JOINs.

### 2.2 `hippius-drain-agent`

| Module | Verdict | Reason |
|---|---|---|
| `localfs.rs` | **Reusable as-is IF same on-disk layout** | `LocalSsd` = plain `struct { root }`, **no node id, no Redis, no Postgres, no daemon**. Implements `PartSource`/`PartScan`/`PartRemover`/`FreeSpaceProbe` and encodes the whole FS contract: dir layout, `MetaJson` fail-closed parse (`localfs.rs:468`), write-temp/staged naming, and the **publish-rename `flock` interlock** (`remove_part_dir_exclusive` `localfs.rs:272-311`). The one cleanly-liftable substantial module. (Carries SSD-cache/eviction GC helpers a non-caching service just won't call.) |
| `disk.rs` | **Reusable as-is** | `statvfs` free-space probe via `nix`; pure over a `Path`. |
| `readiness.rs` | **Reusable as-is** | Pure stall/progress verdict for a K8s readiness file. |
| `supervisor.rs` | **Reusable as-is** | Generic `CancellationToken` task supervisor; not drain-specific (small, easy to replace). |
| `landed.rs` | **Reusable with refactor** | `LandedQueue`: RPOP `cephor:landed:<node>` (`landed.rs:44`). Clean but the per-node key + API-publisher wire contract are baked in. |
| `enqueue.rs` | **Reusable with refactor** | `RedisEnqueuer`: LPUSH `UploadChainRequest` to `{backend}_upload_requests:<node>`. Tightly coupled to the Python uploader wire shape (golden fixture `enqueue.rs:217`), node-scoped naming, backends pinned `["arion"]`. |
| `metrics.rs` (`otel`) | **Not reusable** (pattern only) | All instruments are `drain_*` and read the runtime snapshot type. |
| `config.rs` | **Reusable with refactor** | Clean env-parsing helpers, but every knob is drain-domain and the required `CEPHOR_NODE_ID` + `CEPHOR_SSD_ROOT` bake in the DaemonSet model. |
| `worker.rs` | **Reusable with refactor** | The per-tick drain unit (claim → enforce → `drain_part` → record). Reusable only if you adopt the whole core state machine + a `Store`. |
| `runtime.rs` | **Not reusable** | The orchestrator — spawns ~11 node-scoped supervised workers (drain, reconcile, ssd_reclaim, redrive, ssd_evict, failed_reclaim, landed, enqueue_sweep, upload_sweep, heartbeat, allocation). It *is* the drain topology. |
| `main.rs` | **Not reusable** | The daemon bootstrap; hard-scopes `Store::with_node_id`, `RedisEnqueuer::node_scoped`, `LandedQueue`. |

### 2.3 `hippius-drain-allocator`

**Not reusable unless you keep a shared Ceph write-budget.** It is a leader-elected singleton that
probes the **Ceph mgr** (`CEPHOR_CEPH_MGR_METRICS_URL`, `CEPHOR_CEPH_POOLS`, ceiling/near-full BPS)
and allocates a global Ceph write budget across the fleet via the Redis coordinator
(`config.rs` env dump confirms the Ceph coupling). With the CephFS copy step gone (§0), its reason
to exist is questionable for this option — see §4. The *algorithm* (`alloc.rs`) and the coordinator
(`coordination.rs`/`tick.rs`) are in `drain-core` and reusable there; the allocator binary itself is
a Ceph-budget controller.

### 2.4 Does this force a DaemonSet on SSD ingest nodes?

**Yes, if the new service owns the ingest/drain data plane — which is the whole point of this
option.** The drain daemon's identity is "one process owning one SSD node": node-scoping threads
through `Store::with_node_id`, `RedisEnqueuer::node_scoped`, `LandedQueue` (`cephor:landed:<node>`),
the heartbeat/allocation coordinator, and the evictor promote-floor (`cephor:promote_floor:<node>`),
all keyed on `CEPHOR_NODE_ID`. The drain only claims parts physically on its local SSD
(`main.rs:55-56`). A central/stateless service cannot drive `runtime.rs`/`worker.rs`/`enqueue.rs`/
`landed.rs` — it has no local `CEPHOR_SSD_ROOT` and would claim parts whose bytes live on another
node.

Practically the new service splits into **two deployables** (mirroring today): a scalable **API
Deployment** and a **per-SSD-node ingest/drain DaemonSet** (which can host the drain loop + the
uploader + the peer-serve endpoint). The genuinely deployment-agnostic reuse is `localfs.rs`,
`disk.rs`, `readiness.rs`, `supervisor.rs`, and the pure `drain-core` core — all node-identity-free,
with the caveat that reuse pulls `drain-core` in as a dependency for its `PartKey`/trait types.

---

## 3. Build list

Everything the option must build (S=~days, M=~1-2wk, L=~2-4wk, XL=~1mo+, per component,
integration/test on top). "Reuse" column notes what §2 already gives you.

| # | Component | Effort | Reuse leverage |
|---|---|---|---|
| 1 | **Encrypt → SSD-landing writer + `meta.json` protocol** — the PUT/UploadPart/append pipeline (bounded encrypt fan-out in `chunk_index` order → single FS consumer → atomic `chunk_<i>.bin` → `meta.json` written last + parent-dir fsync), staged-vs-published discipline, the reserve/tail DB txns, envelope write. | **L** | `apipart.rs` (paths/meta types) + `localfs.rs` (writer/flock) reusable **if** you adopt the same layout. The encryption + DB txns are new. Doc 03 §1-2 is the spec. |
| 2 | **`ArionClient` HTTP surface** — upload/download/delete/delete_files/can_upload + auth headers + the retry split (§1). Process-wide client. | **M** | New; port `arion_service.py` semantics. Foundation for 3-5. |
| 3 | **Queue layer** — per-backend work list + node-scoped names + retry ZSET + the **Lua CAS** retry-mover + DLQ (drop-newest cap) matching `queue.py`/`enqueue.rs` wire formats and the golden fixture. | **M** | `enqueue.rs` is the producer reference; must match byte-for-byte (doc 04 §2, §5.3). Redis patterns mirror the drain. |
| 4 | **`part_digest` fold** — SHA-256 fold with tag `hippius-drain/part-digest/v1\n`, used as the hand-off content fence. | **S** | `drain-core::redrive::part_digest` reusable as-is; golden-pinned to the Python. |
| 5 | **Uploader worker** — the hand-off state machine: await `draining→uploading`, per-chunk `POST /upload`, `insert_chunk_backend` (ON CONFLICT **revive** deleted), **digest-fenced** confirm flip to `replicated`, `_required_backends` union logic, billing-abort on first 402, stale-part drop, per-pod Arion concurrency semaphore vs per-request chunk semaphore, node-scoped retry/DLQ routing. | **XL** | Hardest piece. `redrive.rs` fence + `partdrain.rs` lifecycle inform it; the worker itself is new. Doc 04 §3.1 + `uploader.py`. |
| 6 | **Unpinner worker** — fetch identifiers (with the **WORM/object-lock + never-current-version guards baked into the SQL**), concurrent DELETE, **A9-gated** soft-delete (never ack a partial), 404-idempotent, no-rows-retry-6×, optional batch `/delete_files` grouped by `(address, folder_hash)` with per-file fallback. | **L** | New; `unpinner.py` + doc 04 §3.2. The retention guard lives in `get_chunk_backend_identifiers.sql`, not the API. |
| 7 | **Multi-tier read cache + prefetch streamer** — local NVMe → peer → (optional CephFS pool) → backend; range→chunk planner; prefetch window (=cold backend parallelism); first-chunk 503 gate + per-chunk timeout; **backend bytes cached nowhere, peer bytes promote**; AEAD-failure single-reload; DEK unwrap up front, zero DB in the body. | **L** | Doc 03 §3-4 is the spec. New code; `ssd_evict.rs` handles the evictor side. |
| 8 | **Peer tier** — serve `GET /internal/parts/{oid}/{ver}/{part}/chunks/{i}` (auth-before-parse, 200/404/503 only, local tier only, exact-length ciphertext) + client (owner resolution SQL unioning residency + fresh-part fallback, memoized, exact-length verify, shed-not-queue), peer registry `hippius:peer:<node>`, fresh-part hint `hippius:fresh-part:...`. | **L** | New; `peers.py` + doc 03 §4.3. Cross-node contract — get status codes + length check exact. |
| 9 | **Residency + recency** — `cephor_ssd_residency` claim-before-write (accumulate on promote, overwrite on drain, release-on-failure) + `last_read_at` sampling; `fs_cache_inventory.last_access_at` batched flush for the janitor. | **M** | Schema from `drain-core` migrations; `ssd_evict.rs`/`ssd_reclaim.rs` consume it. `residency.py`/`read_recency.py`/`access_tracker.py`. |
| 10 | **The drain subsystem itself** — verify+hash+enqueue+state machine (`partdrain`/`reconcile`/`worker`/`runtime` behavior) against your own `Store`. | **L (mostly reuse)** | Reuse `partdrain.rs`+`reconcile.rs`+`worker.rs` logic by implementing the traits with a new `store.rs`. The **new `store.rs` is the real work** (see §2.1). |
| 11 | **CID handling** — the **two-identifier** model: `file_id` (HCFS path hash → `chunk_backend.backend_identifier`, addresses download/delete) vs `arion_hash` (BLAKE3 of ciphertext → `chunk_backend.arion_hash`). Legacy `cids`/`cid_id` read-only for old data. | **S** | Part of the uploader (#5); `uploader.py:251-266` `_arion_hash_of`. Chain pin is Arion's side effect — **no substrate call here** (doc 07 §0). |
| 12 | **Credit-read for billing** — the request-path gate: `POST /can_upload` on writes, plus the **account-cacher** (read-only Substrate credit scrape → redis-accounts) and **plans-cacher** (HTTP plan roll → redis-accounts) the gate consumes. | **M** (gate) **+ M** (each cacher) | Doc 05 §5, doc 07 §3. Substrate read client patterns from `hcfs-chain-reporter`. The only direct chain contact in the whole product is this read-only scrape. |
| 13 | **Janitor** — sharded FS-cache GC + hard-delete of soft-deleted rows + `fs_cache:pressure` publisher + the single queue-depth sampler; **absolute replication gate** (never evict an unbacked chunk) + fail-closed **DLQ protection**. Strict singleton. | **L** | New; doc 04 §3.3. `ssd_reclaim.rs`/`ssd_evict.rs` cover part of the FS side; the SQL-discovery + census is new. |
| 14 | **mpu-reaper** — mark abandoned/leaked `cephor_replication_status` terminal + drop MPU headers; DLQ-protected; client+server statement timeouts. Singleton. | **M** | New; doc 04 §3.4, `mpu_cleanup.py`. |
| 15 | **orphan-checker** — per-account chain `list_files`, enqueue unpin for on-chain CIDs absent locally. Singleton. | **M** | New; doc 04 §3.7. |
| 16 | **usage-rollup** — fold `storage_delta_ledger` → `bucket_storage_usage` (DELETE…RETURNING exactly-once) + periodic reconcile. Singleton on PRIMARY. | **M** | New; doc 04 §3.6. |
| 17 | **purger** (account purge) — `purge_jobs` SKIP-LOCKED claim, fan out unpins with the **high-water backpressure gate** (a mass purge once queued 1.29M unpins). | **M** | Likely control-plane / possibly out of data-plane scope; doc 04 §3.5. |
| 18 | **Worker supervision harness** — one binary per worker, SIGTERM→`CancellationToken`, bounded drain-on-shutdown, bounded-dispatch loop, close pools in drop. | **S-M** | `supervisor.rs` reusable; doc 04 §5.2. |

**Not needed for this option:** the S3 protocol/signing/XML front-half (SigV4, policy, conditional
headers, error catalog) is common to *both* storage options and is scoped separately in doc 08 —
out of scope here. No download worker exists today (doc 04 open-Q).

---

## 4. Operational footprint this option forces

| Requirement | Forced? | Detail |
|---|---|---|
| **Node-local SSD landing dir** | **Yes** | `CEPHOR_SSD_ROOT` / `HIPPIUS_OBJECT_CACHE_DIR` (default `/var/lib/hippius/object_cache`). The whole design writes ciphertext here first. |
| **DaemonSet on ingest SSD nodes** | **Yes** | The drain + uploader + peer-serve must run *on the node that holds the bytes* (§2.4). API stays a scalable Deployment; ingest/drain is a per-node DaemonSet keyed on `CEPHOR_NODE_ID`. |
| **CephFS** | **Optional / shrinking** | Only as the **pool** read-cache tier (`HIPPIUS_OBJECT_CACHE_FALLBACK_DIR`); unset ⇒ single-tier NVMe + peer + backend, no pool. The drain no longer copies to Ceph (§0). A two-tier (NVMe+backend, +peer) target is viable and simpler. |
| **Redis instances** | **Yes (3 for data plane)** | Of the 5 (doc 04 §1): **cache** (reads/metrics/pressure), **accounts** (credit/plan roll), **queues** (`noeviction`, 2 GB, CephFS-backed — carries upload/unpin lists, retry ZSETs, DLQs, **plus** the drain's `cephor:*` lease/epoch + `notify:*`). Rate-limit + ACL instances are API-only. The `noeviction` queues instance is the fragile one — every producer must respect DLQ caps + purger high-water. |
| **Postgres `cephor_*` tables** | **Yes** | `cephor_replication_status`, `cephor_ssd_residency` (+ `cephor_gc_state`, `cephor_claim_seq`), plus `fs_cache_inventory`. In this greenfield they live in the service's own DB, provisioned from the `drain-core` migrations. |
| **Allocator singleton** | **Only if you keep a shared write-budget** | It exists to allocate a **Ceph** write budget by probing Ceph-mgr. With no Ceph copy step, its value is unclear; you *may* still want the leader-elected coordinator (`coordination.rs`) to publish per-node promote-floors / rate budgets, but the Ceph-probe allocator binary is likely droppable. **Open question.** |
| **Peer auth secret + pod-network** | **Yes (for the peer tier)** | Shared secret (`peer_auth`), `PEER_PORT=8000`, literal pod-IP registration in Redis. |
| **Deploy ordering** | **Hard cutover** | Drain first, then the API that stops enqueuing (doc 03 §5.3). Backends pinned in code (`STORAGE_BACKENDS`), not env. |

Net: **the option stands up two pod topologies (API + ingest DaemonSet), three Redis roles, a
Postgres with the cephor schema, an SSD per ingest node, and optionally CephFS + a coordinator/
allocator.** That is the current Python operational surface, minus the Ceph copy.

---

## 5. Net assessment

**Total build size.** Large. Roughly **one XL (uploader), five L (SSD-landing writer, unpinner,
read-cache/streamer, peer tier, drain-with-new-store, janitor), and ~seven M + a few S** on top of
the shared S3-protocol front-half (scoped elsewhere). The reusable Rust already carries real
weight: the entire **pure `drain-core`** (types, `apipart`, `alloc`, `enforce`, `redrive`,
`snapshot`) and **`localfs.rs`/`disk.rs`/`readiness.rs`/`supervisor.rs`** lift with little change,
and `partdrain`/`reconcile`/`ssd_evict`/`ssd_reclaim`/`gc` reuse by implementing their traits. **The
single largest new write is the drain `store.rs`** (its own schema + SQL) plus the uploader
hand-off state machine. Envelope encryption changes nothing in the reusable core — it treats chunks
as opaque, and `redrive.rs` actively exists to catch the silent-ciphertext-swap hole a per-version
DEK creates.

**Biggest risks.**

1. **The uploader/drain hand-off is a distributed content-fence state machine, not "POST chunks."**
   The `content_sha256`/`part_digest` fence, the `draining→uploading→replicated` transitions, and
   the ON-CONFLICT-revive are the safety properties that stop a raced `UploadPart` from making the
   backend hold non-acknowledged bytes while the evictor frees the only good copy. All three of
   drain, uploader, and any Rust reimpl must fold the digest **identically** (golden-pinned).
2. **Cross-service table + wire contracts.** `cephor_replication_status`/`cephor_ssd_residency`
   (object_id as **TEXT**, specific status enum), the `UploadChainRequest` golden, the queue-name
   node-scoping, the peer 200/404/503 + exact-length rule, and the "404 = idempotent success" /
   revive-on-conflict behaviors all break **silently** if wrong.
3. **Operational complexity.** A DaemonSet-per-SSD-node topology, a `noeviction` queues instance
   that a mass purge can fill (broke prod once), residency/eviction/promotion bands that must nest
   correctly, and the flock interlock between publish and evict. This is inherent to owning the data
   plane.
4. **Silent scope in the SQL.** WORM/object-lock retention and never-delete-current-version guards
   live inside `get_chunk_backend_identifiers.sql`, not the API — easy to miss in a rewrite and a
   durability/compliance hazard if dropped.
5. **Dead/shrinking surfaces to not resurrect** (doc 03 open-Qs): `cache_writer.py`, the pool-era
   enqueue path, the standalone Ceph allocator. Decide the pool tier's fate before building §7/§8.

**What Arion-direct gives you that handing storage to hcfs would not.** HCFS streams objects
**straight to Arion or S3** (`hcfs-server/src/storage/backend.rs`: backend = `arion`|`s3`|`both`,
dual-write) with **no node-local SSD landing, no warm read cache, no peer tier, and no drain/
eviction machinery.** So handing storage to hcfs is dramatically less to build — but it **forfeits
the SSD → warm-cache performance tier**: sub-ms local-NVMe ingest acknowledgement, the local→peer→
pool read hierarchy that serves hot/recently-written objects without a backend round-trip, and the
mid-stream promotion of peer-served bytes. Arion-direct is the *only* option that keeps that tier —
that is the entire trade: you rebuild (and reuse ~half of) the drain/cache/uploader complexity in
exchange for the ingest/read latency profile the SSD tier provides. If that latency tier is not a
product requirement, hcfs-handoff is the smaller build; if it is, Arion-direct is the path and the
existing Rust drain crates de-risk a meaningful fraction of it.

---

## Open questions

1. **Pool (CephFS) tier lifetime.** Two-tier (NVMe + backend, +peer) vs three-tier (add CephFS
   pool). Removing the pool changes the invalidation/promotion gates and the invalidation
   `durable_elsewhere` check. Doc 03 open-Q #4.
2. **Allocator's fate.** With no Ceph copy, is the leader-elected coordinator still wanted (to
   publish per-node promote-floors / drain rate budgets), or does the whole allocator binary go? The
   `alloc.rs`/`coordination.rs` primitives survive either way.
3. **Range-aware backend fetch.** HCFS `/download` **does** honor `Range` (§1.2), which resolves
   doc 03's open-Q #1 assumption — the rewrite *can* add range-aware backend fetch (AEAD chunk
   boundaries still force ≥whole-chunk granularity). Worth doing since it removes the "1-byte Range
   pulls a full 4 MiB chunk" cost.
4. **Reuse-by-dependency vs vendoring.** Lifting `localfs.rs` + the pure core means depending on
   `hippius-drain-core` (for `PartKey`/trait types) from the new repo. Is a cross-repo crate
   dependency acceptable, or should the reusable core be vendored/extracted into a shared crate?
5. **Multi-backend fan-out.** Only `arion` is live, but keys/`_required_backends` are generic. Build
   arion-only with the generic key scheme, or wire multi-backend on day one?
6. **`download-multi`.** No such endpoint exists (§1.2). Confirm the reader keeps one-GET-per-chunk
   with a prefetch window, or whether a batch download endpoint is worth adding to Arion/HCFS.
7. **Two recency tables.** `fs_cache_inventory` (janitor age-based) and `cephor_ssd_residency`
   (drain LRU) both exist with separate evictors. The greenfield could unify them — a drain/
   data-plane co-design question. Doc 03 open-Q #6.
