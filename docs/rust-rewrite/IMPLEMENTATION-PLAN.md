# Rust S3 — Unified Implementation Plan

**Status:** consolidated plan, ready to execute; the one remaining gate is a Phase-0 code review of the committing-AEAD wrapper · **Date:** 2026-09-15

This is a **self-contained** plan: reading it is enough to understand what we are building and how to build it. It needs no other document.

**What we're building.** A from-scratch Rust reimplementation of our object-storage product — a **very tight AWS S3 emulation** whose durable bytes live in **HCFS** (an existing internal service that fronts Arion and S3 and handles dual-write, retry, and per-tenant usage→chain billing). It is **greenfield**: a new branch, its own database, its own optimized schema, and a separate deployment. Existing customer data is moved over later by a one-time re-encrypting migration, so the new system is not constrained by the old on-disk formats.

---

## 1. Executive summary

The service behaves like AWS S3 — the objective bar is a green run of the standard S3 conformance suites (Ceph **s3-tests** and MinIO **mint**) against our endpoint. It stores every object as **client-invisibly encrypted, content-addressed blobs in HCFS**, which we treat as an **unchanged backend** (no HCFS code changes are required to ship). The service owns the S3 protocol, all metadata, envelope encryption, object chunking, and blob deduplication/refcounting; HCFS provides durable storage plus per-tenant billing, which we inherit.

Writes land on a node-local SSD for a fast acknowledgement and drain asynchronously to HCFS; reads go straight to HCFS. Encryption is a **committing AEAD** built and verified for this project, enabling O(1) server-side copies and per-owner dedup while staying confidentiality-first.

The big-bang destination is reached **incrementally** — correctness proven against the conformance suites, writes shadowed, cutover done per-bucket — so it never behaves like a flag-day. **One thing gates the start:** a cryptographer's code review of the Phase-0 committing-AEAD wrapper (the construction is already specified, frozen, PoC-verified and covered by golden vectors in `ctx-poc/` — the review is of the implementation once written, not the spec).

---

## 2. Architecture

```
                         ┌──────────────── kube namespace: hippius-s3r ────────────────┐
   S3 clients            │                                                             │
 (aws-sdk, s3cmd, …) ────┼──▶  API tier  (stateless Deployment, autoscaled)            │
     SigV4 / XML         │      · SigV4 & presigned auth · XML · policy/ACL · quota     │
                         │      · reads: byte-range → frames → one HCFS ranged GET      │
                         │      · WORM-bucket writes: sync-to-HCFS-before-ack           │
                         │                                                             │
                         │  Ingest tier  (DaemonSet, one per SSD node)                  │
                         │      · encrypt → land frames on local NVMe + meta.json →     │
                         │        fast ack; forwarder POSTs blobs to HCFS → mark        │
                         │        replicated → clean SSD; serves not-yet-drained reads  │
                         │                                                             │
                         │  Workers  (Postgres-queued: forward, cleanup, reap, refcnt) │
                         │  own Postgres (metadata)  ──────────────────────────────────┘
                         │
                         └──▶  HCFS  (separate service, UNCHANGED)
                                   store / ranged-get / delete by (account, file-id)
                                   durable dual-write to Arion & S3 · retry · usage→chain
```

Three roles from one binary (selected by a role env var, like our existing Rust drain daemon):

- **API tier** — stateless, horizontally scaled. Terminates the S3 protocol, owns metadata/crypto/policy, and serves reads from HCFS.
- **Ingest tier** — a DaemonSet on SSD nodes. This is the SSD write-staging reservoir plus the forwarder that drains blobs to HCFS. We own the ingest hot path; HCFS is the durability tier. There is no separate read cache and no Ceph tier.
- **Workers** — Postgres-queued background daemons (see §8).

HCFS is the single storage backend, used through its existing HTTP API.

### Request flows

- **PUT (normal bucket):** authenticate → quota-gate → encrypt into 256 KiB frames packed into 4 MiB blobs → write blobs to local SSD with a `meta.json` completion marker → **return 200** → the forwarder drains blobs to HCFS → increment blob refcounts and mark replicated → delete the local SSD copy.
- **PUT (object-lock / WORM bucket):** identical, except the client is **acknowledged only after HCFS confirms durability** — no local-only window.
- **GET / Range:** resolve the serveable version → map the byte range to the covering frames → one ranged GET per blob from HCFS → verify each frame's commitment and decrypt → stream. A GET of an object still staged on SSD (not yet drained) is served from that node's SSD via **node-sticky routing** (the router resolves not-yet-`durable` blobs through `staged_blobs.node_id` and forwards to that ingest node), to preserve read-after-write consistency. **⚠ This is a per-object SPOF for the pre-drain window:** on a fast-ack bucket, until a blob is `durable` in HCFS it exists only on one node's SSD, so that node dying before it drains = data loss + an unreadable-object window (the prod node6-cache SPOF shape). Bounded by keeping drain lag low (the durability-window SLO, §13), the crash reconciler (§8), and — for buckets that can't tolerate it — WORM/sync-before-ack. See §15.
- **CopyObject:** metadata only — the new version references the same blobs, refcounts increment, and the data-key is re-wrapped under the (same) owner key. No re-encryption, no byte movement.

---

## 3. Key decisions (all resolved)

| Area | Decision |
|------|----------|
| Product | Very tight AWS S3 emulation; **full vanilla-S3 parity, no incomplete ship**; conformance measured by s3-tests + mint |
| Storage backend | **HCFS, unchanged** — store/ranged-get/delete via existing endpoints; blob refcount owned by us |
| Write path | **SSD staging** + async forwarder to HCFS; **per-bucket acknowledgement policy** (fast-ack + serve-pending by default; WORM buckets sync-before-ack) |
| Encryption | Committing AEAD, `hip-enc/aes256gcm-ctx-frames-v1` (§6) — verified; enables O(1) copy + per-owner dedup |
| Identity / billing | Keep remote credential assertion; the account's chain address (SS58) is the tenant identity, the billing key, and the credit key; per-tenant billing inherited from HCFS |
| Object lock | AWS-verbatim semantics; the proprietary append verb kept but made lock-safe |
| Metadata store | Own optimized Postgres (via sqlx); version-native; the "serveable version" rule defined once |
| Migration | Re-encrypt existing data through the live write path; per-bucket; verified on a plaintext hash |
| Security posture | Reject cross-tenant convergent dedup; block key-destruction as a deletion shortcut; no direct blockchain writes (delegated) |

---

## 4. Component / crate architecture

One Cargo workspace on the new branch, reusing the pure logic and local-filesystem module from our existing Rust drain daemon.

| Crate | Responsibility |
|-------|----------------|
| `s3-protocol` | The AWS wire layer: SigV4/SigV2 verification, XML (de)serialization, the S3 error catalog + status/XML mapping, IAM-style bucket-policy evaluation, aws-chunked decoding, conditional-header evaluation, and request dispatch. Decoupled from storage via small traits (`CredentialStore`, `MetadataStore`, `ObjectBackend`). Roughly a third of this can be lifted from an earlier internal S3 module (chiefly the SigV4 code) and adapted. |
| `s3-crypto` | The encryption envelope (§6): frame seal/open, the key hierarchy, the KMS client, **and** a read-only decrypt path for the *old* format (needed by migration). |
| `s3-metadata` | The Postgres schema and access layer (§5): version-native model, the serveable-version view, blob dedup/refcount, staging state, and all S3 metadata (multipart, versioning, lock, tags, ACL). |
| `s3-hcfs-client` | The HCFS integration (§7): store/ranged-get/delete via existing endpoints, service credential + per-tenant attribution, idempotent retries. |
| `s3-ingest` | SSD landing (reusing the drain's local-filesystem contract) + the forwarder daemon + staging state + post-confirm cleanup. Runs as the DaemonSet. |
| `s3-api` | The axum service: middleware chain, the four auth methods, policy/ACL/quota gating, the per-bucket ack policy, serve-pending reads; wires everything together. |
| `s3-workers` | The background daemons (§8). |
| `s3-migration` | The one-time re-encrypting backfill job (§12). |

---

## 5. Data model

Own Postgres, redesigned for throughput, with the old system's known defects fixed by construction — e.g. a constraint forbids objects with missing encryption metadata, and the "is this version serveable?" rule is a single database view rather than logic duplicated across queries.

Core tables: `accounts` (keyed by the chain SS58 address) · `buckets` (with versioning state, object-lock config, policy, CORS, lifecycle, tagging, default-encryption) · `objects` · `object_versions` (retention/legal-hold fields; the serveable-version inputs) · `parts` · `chunks` · **`blobs`** · `chunk_blobs` (the reference edge that carries refcounts) · `multipart_uploads` + `upload_parts` · `object_tags` · bucket/object ACL grant tables · `sub_token_scopes` · `staged_blobs` (SSD landing/replication state, which doubles as the Postgres work queue) · `bucket_usage` · and the private, owner-scoped plaintext→blob dedup map.

**A blob has three distinct identities — never conflate them:**

- **`blob_id`** — an opaque, server-assigned, key-independent value, known at seal time and bound into the encryption. (It must *not* be the ciphertext hash — that isn't known until after sealing — and must *not* be the plaintext hash — exposing that would create a "confirm whether a known file is stored" oracle.)
- **`content_hash`** = BLAKE3 of the ciphertext — the address used to store and fetch the blob in HCFS.
- The private **owner-scoped BLAKE3 of the plaintext** — the deduplication key, held only in our database and never exposed.

The data-key lives with the **blob**, wrapped under the owner's key-encryption-key (per-owner dedup means one wrapping). `blobs.refcount` governs lifecycle: we issue the HCFS delete only when a blob's refcount reaches zero. (HCFS itself has no refcount and deletes unconditionally — confirmed by reading its code — which is precisely why we own this.)

**The refcount has a resurrection-race guard — this is load-bearing, not optional.** A blob must **not** be HCFS-deleted the instant its refcount hits zero: a concurrent CopyObject or dedup-PUT can take a new reference in the same window (decrement→GC-delete→resurrect = a dangling reference / data loss). So the 1→0 transition records `blobs.zero_refcount_at` (cleared on any 0→1), and the GC worker deletes only after a grace window **and** a re-check of `refcount = 0` under a row lock (see §8). Without this, a builder following the naive "delete at zero" rule ships the race.

**The blob write algorithm (per 4 MiB chunk, owner = bucket owner):** `pt_hash = blake3(plaintext)` → look up `(owner, pt_hash)` in the dedup map → **hit:** add a `chunk_blobs` edge to the existing blob (refcount++, clear `zero_refcount_at`), no seal/store; **miss:** assign a fresh opaque `blob_id`, generate a random per-blob DEK (wrap under the owner KEK), seal the frames, compute `content_hash = blake3(ciphertext)`, insert the `blobs` row, store on SSD→HCFS, record the dedup-map row, add the edge. **Concurrency:** two same-owner PUTs of identical plaintext both miss → both seal (different random DEKs ⇒ different `content_hash`) → race the dedup-map PK; the loser re-looks-up the winner's blob, references *that*, and drops its own (which grace-GCs). The dedup-map row is the single serialization point.

**Scale.** `object_versions` is already ~177M rows in prod and grows; a 5 TiB object is ~1.28M `chunks`/`blobs` rows. Commit to **hash-by-`object_id` partitioning** for the unbounded tables (`object_versions`/`parts`/`chunks`/`chunk_blobs`) at schema-creation time — retrofitting a partition key later is a table rewrite (an explicit open call, doc 12 OQ-4). Size the owned Postgres cluster for that row count + write concurrency.

Migrations are sqlx-managed and owned by this service; because it's a separate database, there is no coexistence concern with any other migrator.

---

## 6. Encryption — `hip-enc/aes256gcm-ctx-frames-v1` (verified)

The construction was checked against the primary cryptographic literature, implemented as a Rust proof-of-concept that passes round-trip and every tamper test, and pinned with golden test vectors (in `ctx-poc/`). **The construction is frozen; the one remaining gate is a cryptographer's code review of the Phase-0 wrapper implementation against that PoC.**

- **Key hierarchy:** OVH KMS master key (mutual-TLS) → per-bucket key-encryption-key → **per-blob data-key** → frames. (This mirrors the AWS envelope pattern and matches the old system's KMS setup, which the migration needs.)
- **Layout:** an object splits into 4 MiB plaintext chunks; each chunk is one blob; each blob is **16 × 256 KiB frames** (tunable per bucket down to 64 KiB for range-heavy workloads).
- **Per-frame wire format:** `nonce(12) ‖ ciphertext ‖ GCM-tag(16) ‖ commitment(32)` — 60 bytes of overhead per frame (~0.023% at 256 KiB).
- **Committing AEAD (CTX construction), giving full key-commitment:** the commitment is `BLAKE3` over a length-prefixed, domain-separated encoding of (data-key, nonce, AAD, GCM-tag). On decrypt, the constant-time commitment check runs **first**, then GCM verification, then the final-frame flag. This closes the "one ciphertext opens under two keys" class of attack, which matters because blobs are looked up and opened under a shared data-key.
- **Nonce (STREAM scheme):** `random-prefix(7) ‖ frame-index(4, big-endian) ‖ final-flag(1)`. Because every blob has a fresh random data-key and is encrypted exactly once, no (key, nonce) pair can repeat; frame counts (16–64 per blob) are far below all safety limits.
- **AAD:** `blob_id ‖ frame-index ‖ suite-id` — binds a frame to its blob identity and position but **not** to the logical object, which is what makes O(1) copy and dedup possible.
- **Deduplication:** per-owner, copy-oriented only. Cross-tenant convergent (content-derived-key) dedup is **rejected** — it is a known confidentiality downgrade and buys nothing our copy model doesn't already give.
- **Crates:** `aes-gcm`, `aead`, `sha2`/`blake3`, `subtle`, plus an ~120-line hand-written committing wrapper (no off-the-shelf committing-AEAD crate exists).
- **Old-format decrypt:** the previous envelope (plain AES-GCM, per-version key, prepended random nonce, object-bound AAD, non-committing) is retained in a `legacy` module used only by the migration; a per-version suite id distinguishes old from new blobs.

---

## 7. Storage integration with HCFS (zero HCFS changes)

Verified achievable against HCFS as it is today:

- **Store:** POST an encrypted blob (≤4 MiB, well under HCFS's 16 MiB cap) under the tenant's SS58; persist the returned identifier.
- **Get + Range:** fetch by that identifier with native HTTP Range support.
- **Delete:** by identifier; a "not found" is treated as success (safe for our refcount-gated deletes).
- **Billing:** per-tenant credit-gating (a 402 when out of credit) **and** usage→chain reporting come for free, provided each tenant's bytes are stored under their real SS58 and that account is **not** on the storage-exemption list. HCFS attributes usage by where bytes land, and its existing reporter pushes it on-chain.
- **Deduplication/refcount:** owned by us — exactly one HCFS object per unique ciphertext, deleted only when our refcount hits zero.

**Security note (one recommended, optional HCFS change).** The zero-change path uses HCFS's global administrative bearer token, mitigated by always deriving the SS58 we send from *our* authentication result (never a client-supplied field) and by locking egress to HCFS at the network level. A leaked token is still all-powerful, so a **scoped service token** (a small, well-precedented change to HCFS that binds a token to a supplied account header) is recommended to contain blast radius. This is a security-vs-simplicity decision, not a functional blocker.

---

## 8. Background workers (Postgres-queued)

We do **not** build the old system's uploader, unpinner, cache-hydrator, janitor, Ceph allocator, or chain-reporter — HCFS owns those concerns. Our set is eight, using Postgres (`SELECT … FOR UPDATE SKIP LOCKED`) rather than Redis:

| Worker | Role | Placement |
|--------|------|-----------|
| Forwarder | drain SSD blobs to HCFS, mark replicated | per node (ingest) |
| SSD cleanup | delete confirmed local blobs | per node |
| Staging reconciler | crash-recover SSD blobs staged but never confirmed | per node |
| Multipart reaper | abort abandoned multipart uploads | singleton |
| Lifecycle / version reaper | lifecycle expiry + version GC (lock-aware) | singleton |
| Blob-refcount GC | issue HCFS delete when a blob reaches refcount zero — **only after the `zero_refcount_at` grace window and a re-check of `refcount = 0` under a row lock** (resurrection-race guard, §5); a re-reference during the window cancels the delete | singleton, single-flight |
| Usage reconciler | reconcile our usage against HCFS summaries | singleton |
| Consistency checker | periodic orphan/consistency sweep | singleton, optional |

The queue pattern is a scan over partial indexes plus the `staged_blobs` durable queue, with SKIP LOCKED claims, backoff, a failed-state dead-letter equivalent, and a visibility-timeout reaper for stuck claims.

---

## 9. S3 protocol surface & conformance

**In scope for v1 (must pass the suites):** object create/read/head/delete, CopyObject, GetObjectAttributes; all seven multipart operations with real ListMultipartUploads pagination; versioning, delete markers, and ListObjectVersions including the *Suspended* state; object and bucket tagging; ACLs; bucket policy (the public-read subset in v1, the rest as a fast follow); real CORS; lifecycle configuration round-trip; object lock; **AWS's 2024 conditional writes** (`If-Match` on writes plus conditional reads — the single largest fidelity gap versus current AWS); and **server-side-encryption headers** (advertise SSE-S3 as the always-on default, map `aws:kms` requests onto our key hierarchy, reject external KMS ARNs and customer-provided keys with clear errors; the ETag remains the MD5 of the plaintext regardless of encryption).

**Out of scope for v1 (return an explicit "not implemented", never a fake success):** Select, torrent, static-website, replication, analytics/inventory/metrics, transfer acceleration, requester-pays, access logging, event notifications, intelligent-tiering, ownership controls / public-access-block, directory buckets, and any IAM/STS surface.

**The bar** is two CI lanes — Ceph s3-tests (a protocol oracle) and MinIO mint (an SDK-interop oracle) — run against our own endpoint, with a reviewed skip-file whose every entry is justified; growth of that skip-file is the regression metric.

**Fidelity details most clones get wrong, which we must nail:** ETag correctness across single-part/multipart/encrypted paths; conditional writes done inside the write transaction (no check-then-act race); never silently ignoring `If-*` headers; list pagination and `encoding-type`; supporting both path-style and virtual-hosted addressing; exact error XML and codes; the element ordering strict SDKs require; presigned-URL corner cases; and Range semantics (an inverted range returns the whole object; an unsatisfiable one returns 416).

**The proprietary append verb** is kept: it remains an in-place, O(delta) append, but with a "refuse if the current version is protected" check enforced *inside* the same compare-and-swap transaction — closing the write-once hole the old system had.

### Non-S3 surfaces that are also in v1 scope

The product is more than the S3 protocol. These non-S3 endpoints must be served or the console/admin tooling breaks the moment the old system is retired:

- **Admin API** (`/admin/*`, admin-HMAC): suspend, reactivate, status, async **purge** (the account-deletion / GDPR / cost-reclaim path — cannot be dropped), and purge-job status. Carries a "refuse service-account" guard.
- **Console read model** (`/user/*`, frontend-HMAC): list-buckets, list-objects, get-bucket-location, recent-uploads. These return **JSON shapes the console renders from — not S3 XML** — so if they're absent the console goes blank even though S3 itself works. Load-bearing.
- **Sub-token scope API** (`/user/sub-tokens/*`, frontend-HMAC): get/put/delete scope. Required *because* we enforce scopes fail-closed — without the write API, sub-tokens default-deny, which is an outage, not a degrade.
- **Anonymous public read** (`/public/{bucket}/{key}`): a distinct prefix, implemented by the S3 layer.
- **Health** (`/health`) and `robots.txt`.

**Obsolete under our design — do not build:** peer / internal-parts serving (we have no read cache or peer tier), an HTTP `/metrics` route (telemetry is OTLP push), and the Redis-banhammer `unban`. Service-account handling is config + guard logic, not an API.

**One deferred ops decision:** whether the ATS edge/caching tier (and its auth-probe, cache-control, and purge middleware) is retained — if kept it needs its re-authorization probe; if the edge is redesigned it's obsolete. Also: existing console/backend request signatures (path + HMAC byte-fidelity) must keep verifying, so those wire contracts are frozen inputs.

---

## 10. Authentication, identity & billing

- **Four auth methods:** access-key SigV4, presigned SigV4, bearer token, and anonymous. (The old seed-phrase method stays retired.)
- **Identity is asserted remotely:** each key/token is verified against the existing account service (with a ~60-second cache), and the resolved **SS58 address is simultaneously the tenant identity, the billing key, and the credit key.** A secret-free account row is kept locally for durability.
- **Sub-tokens** carry a scope (a permission tier × a bucket list, the R2-style model) that is **enforced fail-closed** — a scoped token is never accepted with its scope ignored. There is no general IAM policy engine; one optional name-prefix scope axis is reserved for later.
- **The billing gate** performs the per-tenant credit check before storing bytes; usage is recorded under the tenant SS58, which is what makes HCFS's reporting attribute correctly.

---

## 11. Object lock (AWS-verbatim)

Compliance mode is absolute (nobody, including the account owner, can shorten or remove it; only account termination lifts it). Governance mode is bypassable only by the bucket owner's master token together with the explicit bypass header. Legal hold is independent of retention and never bypassable. Versioning is a one-way prerequisite (attempting to suspend it returns a 409). Default bucket retention is not retroactive and is overridden by explicit request headers. Lifecycle expiration cannot delete a locked version, and delete markers are never themselves protected. Maximum retention is 36,500 days; the default cap is ~7 years; a "year" is 365 days.

Enforcement lives at **two layers** — the API (to return the right error) and the database/worker paths (for the actual byte protection, since destructive operations can bypass the API). The compliance-vs-GDPR tension is handled contractually, and **destroying an encryption key is explicitly disallowed as a deletion shortcut** (crypto-shredding is not treated as a lawful erasure path).

---

## 11a. Security posture (consolidated)

The security story is otherwise spread across §3/§6/§7/§10; consolidated here so it reads as one thing.

- **Confidentiality-first crypto (§6):** committing AEAD (CMT-4), per-blob random DEK, cross-tenant convergent/MLE dedup **rejected**. The dedup confirmation-oracle is **per-owner only** — an owner can tell they already store a given plaintext, never across tenants.
- **Cross-owner isolation is defense-in-depth, by design:** blob references are gated at the metadata layer, but even a metadata bug that let owner A reference owner B's blob **cannot leak plaintext** — the DEK is wrapped under B's per-bucket KEK, which A cannot unwrap. Confidentiality survives a metadata-layer error; state and preserve this property.
- **Identity is server-derived, never client-steered:** the SS58 we send to HCFS always comes from *our* authenticated result, never a client field (the compensating control for the shared admin bearer — §7). Egress to HCFS is network-locked; the scoped HCFS service token (§7 / B3) lands before prod cutover to contain blast radius.
- **Input validation is load-bearing:** reserved-name / path-traversal / double-decode defenses on bucket+key, judged on the *collapsed* routing path (the 2026-08-03 prod traversal incident: `/docs/../bucket/key`). Every auth-exempt path segment must be unusable as a bucket name.
- **Object-lock / WORM (§11):** COMPLIANCE is absolute; crypto-shredding (KEK destruction) is explicitly **not** a lawful erasure path and is blocked for lock-enabled buckets.
- **Known, deliberate v1 gaps (state them, don't hide them):** **rate-limiting is parked** (G2 — no request-path rate limit in v1; revisit worker-side); presigned uploads default to `UNSIGNED-PAYLOAD` (AWS-compatible, but the body is effectively unauthenticated — a documented footgun, not a bug). COMPLIANCE-as-regulator-grade needs **legal sign-off** before it is offered (no Cohasset assessment; GDPR-erasure-vs-WORM is contractual — doc 18 Q7).

---

## 12. Migration & cutover

- **Migrator:** enumerates the old buckets, objects, and versions; decrypts each using the retained old-format code; and **re-encrypts by feeding the plaintext through the live new-write path**, so migrated objects are byte-for-byte identical to native uploads. It is resumable and parallelised via a dedicated schema and SKIP LOCKED.
- **Verification:** anchored on the plaintext BLAKE3 (old recorded hash == recomputed == new), with per-bucket count/byte/config equality, and covering versions, delete markers, multipart composite ETags, object lock, tags, and ACLs — lock and ACL treated as hard gates.
- **Dual-run per bucket:** snapshot → catch-up on changes → a short write-freeze for the final delta. No dual-write and no split brain. In-progress multipart uploads are frozen and abandoned. **⚠ Prereq (verified 2026-09-15):** the old Python side has **no per-bucket write-freeze** — only fleet-wide read-only and per-*account* suspension. So either add a small `buckets.write_frozen` flag on the old side, or **migrate at account granularity** using the existing per-account `read_only` suspension (zero old-side change; the default if per-account freeze windows are acceptable). Decide before Phase 4 (doc 20 §3.3).
- **Cutover:** shadow-and-diff → shift traffic per bucket (freeze the old, enable the new) → aged decommission (drop old rows and GC old HCFS blobs; WORM data waits out its retention). Rollback is a repoint-to-old **before the soak period ends** — but the **soak length and the forward-commit point** (the boundary past which rollback means reverse-replay of new-only writes, not a clean repoint) are **still open** (OQ-8) and must be set before Phase 5. Storage temporarily doubles and usage can double-count during the window — both expected (defer new-service usage reporting until cutover).
- **What the migration cannot carry:** ~167k prod objects are unreadable today (~141k NULL-envelope v5 + ~25.9k v4-stuck — python-side-findings #2/#14); they can't be decrypted, so they land in skip-and-report/quarantine. The Python team must repair or consciously abandon them before decommission.

---

## 13. Deployment, operations & observability

- **A dedicated namespace, separate from the current product's environments.** One container image, its role selected by env var (following our drain daemon's shape: minimal init, injectable config).
- **Topology:** the API Deployment (with autoscaling), the ingest DaemonSet (per SSD node, with local NVMe volumes and node affinity), the worker Deployments/singletons, an owned Postgres cluster, a schema-migration job, and a metrics collector.
- **Config & secrets:** own database URL, the HCFS base URL and service credential, the KMS credentials, the signing material, and metrics settings. The HCFS credential is injected from a secret manager and its egress is network-locked to HCFS only.
- **Observability:** OTLP metrics (mirroring the drain daemon), request traces and latency histograms, and the **`S3rForwardBacklogAge`** ("oldest un-drained blob age") metric as the durability-window health signal — **warn > 30 s, page > 120 s** (well above the p99 store latency, so it fires when the forwarder is actually falling behind, not on normal jitter). The API exposes an HTTP health endpoint; the daemons use file-freshness liveness probes.
- **CI:** a two-stage image build; the s3-tests and mint conformance lanes run against a real local HCFS; and a fault-injection test on the HCFS network hop that proves writes survive an HCFS outage by staying on SSD and draining on recovery.
- **SSD sizing (measured from prod, 2026-09-15):** peak per-node PUT ingest is **~160–190 MB/s** burst (~72 MB/s sustained; burst ≈ 2.3×), and a single 4 MiB blob stores to HCFS at **p50 630 ms / p95 1.37 s / p99 2.36 s**. → a **~64 GB/node reservoir** rides a ~5-min HCFS stall at peak burst (96–128 GB buys 10–15 min) — **7–14× smaller** than the old ~930 GB/node read-cache reservoir, because we stage writes only. **Forwarder concurrency ~32–64/node** (draining 190 MB/s at p95 1.37 s/blob needs ~65 concurrent POSTs; below that the backlog grows and the reservoir fills). Enforce SSD watermarks: near-full → warn + prioritize forwarding; critical → fail ingest readiness and shed writes to the sync-to-HCFS path / 503, per node. The `max_forward_lag_window` (defaulted to 5 min) should be re-picked from HCFS's actual worst sustained outage.

### 13a. Performance targets (what "fast" must mean — Phase-1/2 gates measure against these)

- **Write throughput:** match or beat the current **~190 MB/s/node** burst ingest (per SSD node); the SSD reservoir absorbs bursts above HCFS's drain rate.
- **PUT-ack latency:** fast-ack buckets ≈ one local-SSD fsync (sub-10 ms goal, ≪ HCFS round-trip); WORM buckets ≈ the HCFS store round-trip (~p95 1.4 s today — the sync-before-ack cost is the accepted durability price).
- **GET first-byte latency:** one covering-frame fetch + decrypt (≈ HCFS ranged-GET RTT + a 256 KiB AES-GCM open), not a whole-blob fetch.
- **Copy latency:** O(1) — metadata-only, independent of object size.
- These are the objective targets behind Phase 1's "read-after-write holds / bytes durable" gate and Phase 2's parity gate; without them "fast" is unfalsifiable.

---

## 14. Build plan — phases with exit gates

The order reaches the big-bang destination through independently verifiable steps. The critical path is `s3-crypto` → `s3-metadata` → the first vertical slice → full parity. The HCFS client, the protocol crate, and the conformance harness can be built in parallel from day one.

**Phase 0 — Foundations.**
- Workspace + CI (mirroring the drain daemon's toolchain, lint, and lockfile discipline).
- `s3-crypto`: implement the verified encryption envelope from the proof-of-concept, the key hierarchy and KMS client, and the old-format decrypt path; lock in the golden vectors.
- `s3-metadata`: the schema as migrations — core tables with the three-way blob identity, per-blob data-key, the serveable-version view, refcounts, and staging state.
- `s3-hcfs-client`: store/ranged-get/delete against a real HCFS, with integration tests.
- `s3-protocol`: harvest and adapt the SigV4 and XML/error layers behind the storage traits; pass SigV4 conformance vectors.
- Stand up the s3-tests + mint harness (initially mostly skipped).
- *Gate:* an encrypt → store → ranged-read → decrypt round-trip against real HCFS is byte-identical; the schema migrates; SigV4 vectors pass. **This phase completes only after the cryptographer's code review of the `s3-crypto` wrapper.**

**Phase 1 — Vertical slice (core object path).**
- `s3-ingest`: SSD landing + completion marker + forwarder to HCFS + staging state + cleanup, on the fast-ack path.
- `s3-api` (minimal): access-key SigV4; bucket create/delete/head/list; put/get(range)/head/delete object; list objects; the credit gate and usage attribution; and the per-bucket ack-policy scaffold.
- *Gate:* the AWS CLI performs bucket and object CRUD and listing against us; bytes are durable in HCFS; usage is attributed; read-after-write holds via serve-pending + sticky routing.

**Phase 2 — Full vanilla-S3 parity.**
- Multipart (staging tree, promote-on-complete, data-key re-wrap); versioning and version operations; CopyObject/UploadPartCopy as metadata-only; conditional requests including the 2024 conditional writes; Content-MD5 and modern checksums; tagging, ACLs, real per-prefix bucket policy, CORS, lifecycle, GetObjectAttributes, PostObject; all four auth methods; the server-side-encryption header emulation; and complete error/XML fidelity.
- *Gate:* the full conformance checklist is green — the "no incomplete ship" bar.

**Phase 3 — Object lock & the append verb.**
- Retention, legal hold, and modes enforced at both the API and database/worker layers; WORM buckets on the sync-before-ack path; the append verb made lock-safe.
- *Gate:* object-lock enforcement tests pass and write-once guarantees demonstrably hold.

**Phase 4 — Data migration.**
- The re-encrypting backfill with its per-bucket state machine and reconciliation.
- *Gate:* a pilot bucket migrates with plaintext-identical verification and serves from the new system.

**Phase 5 — Cutover.**
- Shadow/dual-run → per-bucket traffic shift → decommission.
- *Gate:* production traffic runs on the new system and the old one is scaled to zero.

---

## 15. Risks & mitigations

| Risk | Mitigation |
|------|-----------|
| Encryption-format correctness | Already verified against the literature, with a passing proof-of-concept and golden vectors (`ctx-poc/`); a cryptographer's code review of the Phase-0 wrapper gates Phase 0 completion |
| Durability window from fast-ack | Per-bucket policy makes WORM buckets sync-before-ack; SSD writes are fsync'd; a reconciler recovers un-drained blobs after a crash |
| Coupling to HCFS availability | The SSD reservoir lets writes succeed during an HCFS blip and drain on recovery; a fault-injection CI test proves it |
| Administrative-token blast radius | Never let a client steer the account we send; network-lock egress; recommend the scoped service token |
| Cold-range read amplification | Sub-object framing (256/64 KiB) turns a range into a single ranged fetch, not a whole-blob fetch |
| Parity drift / shipping incomplete | The conformance suites are the gate; the justified skip-file is the regression metric |
| Migration data loss or double-count | Plaintext-hash verification; per-bucket freeze; documented window costs and rollback |
| **Blob-refcount resurrection race** (top-tier, alongside crypto/cutover) | Never delete a blob the instant refcount hits zero: record `zero_refcount_at`, honor a grace window, and re-check `refcount = 0` under a row lock in the single-flight GC worker; any re-reference clears the timestamp and cancels the delete (§5/§8) |
| **Pre-drain per-object SPOF** (fast-ack buckets) | Until a blob is `durable` in HCFS it lives on one node's SSD; that node dying loses it (the node6-SPOF shape). Bound by low drain lag (`S3rForwardBacklogAge` SLO), the crash reconciler, and WORM/sync-before-ack for buckets that can't tolerate the window (§2/§13) |
| **Old-side per-bucket write-freeze doesn't exist** (migration) | Verified: only fleet-wide read-only + per-account suspension. Migrate at account granularity via existing per-account `read_only`, or add a small `buckets.write_frozen` flag — decide before Phase 4 (§12) |
| Inheriting old defects | Fixed by construction (encryption-metadata constraint, single serveable rule, lock-safe append); the old product's separate issues are handled by that team, not here |

---

## 16. Open sign-offs

**Exactly one thing gates the START (Phase 0); a handful of later-phase decisions are listed after it. Everything else is resolved (see `decisions-register.md` resolution log).**

1. **THE PHASE-0 GATE — a code review of the committing-AEAD wrapper.** The construction itself is already frozen and verified (§6; source-checked against the literature, a reference PoC + golden vectors live in `ctx-poc/`). What still needs a cryptographer is the **~120-line Rust wrapper once it's written in Phase 0** (seal/open, the injective commitment preimage, the constant-time-check-first OPEN order) plus reproducing the golden vectors — not a review of the spec. This can't happen until Phase 0 writes the code.
2. **RESOLVED — scoped HCFS service token (B3):** recommended, NOT a blocker. Ship zero-change (admin bearer + always-derive-ss58 + network-locked egress + mTLS) through Phase 1; land the scoped token before production cutover.
3. **RESOLVED — migration scope (E1/E2/E4):** prod counted — only v4 (212,498) + v5 exist, no v1/v2/v3, and all v4 GETs error today, so pre-v5 is **skip-and-report** (`s3-crypto::legacy` v5-only); the v5 copy fast-path was never enabled, so the **undecryptable-copy class is empty**; the window's storage/billing double-up is accepted (defer new-service usage reporting until cutover, migrate WORM buckets last).

The former "routine confirmations" are settled too: credit-cache **fail-open** (HCFS's 402 is the backstop), multipart **freeze-and-abandon**, sqlx-owned migrations (own DB), a **custom in-flight/RPS** autoscaling metric, namespace `hippius-s3r` (placeholder), and seed-phrase-stays-removed / no-direct-chain-writes.

**Not Phase-0 gates, but genuinely-open decisions to make before their phase (not blockers to starting):**
- **Migration write-freeze approach** (before Phase 4): account-granularity via existing per-account `read_only` (default) vs. a new old-side `buckets.write_frozen` flag (§12).
- **Soak length + forward-commit point** (before Phase 5): the rollback-clean boundary (OQ-8, §12).
- **Migration version-history scope**: full chain + delete markers vs. current-serveable-only (product/compliance).
- **Postgres partitioning** commit (schema-creation time — retrofit is a rewrite, §5).
- **COMPLIANCE-mode legal sign-off** before offering regulator-grade object lock (no Cohasset assessment; GDPR-vs-WORM is contractual — §11a / doc 18 Q7); plus object-lock Q3/Q4/Q8.
- Parked by choice: **rate-limiting** (G2), **ATS edge retention** (H9).

---

*Deeper working notes — the per-subsystem research, the encryption verification with its proof-of-concept, and the decision history — live alongside this file in the same directory, but are not required to build from this plan.*
