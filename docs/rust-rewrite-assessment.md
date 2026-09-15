# Rust Rewrite of hippius-s3 — Assessment & Big-Bang Strategy

**Status:** Draft for discussion · **Date:** 2026-09-15 · **Author:** research pass (Camden + Claude)
**Decision this doc supports:** a full Rust reimplementation of the hippius-s3 object-storage product, harvesting the hcfs `service`-branch S3 gateway as the protocol front-end.

> **⚠️ SUPERSEDED by [`rust-rewrite/IMPLEMENTATION-PLAN.md`](./rust-rewrite/IMPLEMENTATION-PLAN.md) + [`rust-rewrite/decisions-register.md`](./rust-rewrite/decisions-register.md) (2026-09-15).** This early assessment is kept for history; where it disagrees with those two, they win. Key reframes:
> - **Greenfield build** (new branch, separate fresh Postgres + optimized schema, separate pods, re-encrypt migration) — so **no bit-compat constraint**; only the OVH **KMS setup** must match Python.
> - **Storage-backend decision IS made:** hand off to **HCFS, unchanged** (zero hcfs changes, doc 13) — NOT Arion-direct. So §3's `s3-storage` (Ceph/peer/pool tiers, peer-serving), `s3-queue` (Redis), and `s3-chain` (credit/chain we build) crates are **wrong** — there is no Ceph/read-cache/peer tier, workers are **Postgres-queued (8, not ~13)**, and HCFS owns usage→chain. Use IMPLEMENTATION-PLAN §4/§8 for the real crate/worker set.
> - **Crypto is frozen** (doc 25): **per-blob** DEK (not the per-version `s3-crypto` line in §3), opaque `blob_id` AAD, CTX frames.
> - **Auth = FOUR methods** (seed-phrase removed) — any "5 auth methods" below (e.g. §6) is stale.
> - §4/§5's "read-compat before write" phasing and the risk-#1 ciphertext-bit-compat framing are superseded by the re-encrypt migration; the v5 copy-fast-path hazard is **resolved (empty class)**.
> - §7's "open decisions" are settled — see the register's resolution log.
>
> The subsystem *contracts* in [`rust-rewrite/`](./rust-rewrite/) remain valid as **semantic references** (what each feature must do), not as byte/wire layouts for the new build.

> **Bottom line up front.** The hcfs `service`-branch S3 module is a strong, near-complete **S3 protocol front-end** (SigV4, XML, error mapping, routing, bucket/object/multipart semantics) — but it is stale, uncompilable dead code written for an architecture that no longer exists, and it is only ~30% of what "replace hippius-s3" means. The other ~70% — a version-native storage engine, envelope encryption, the multi-tier cache, the worker fleet, credit/plan accounting, KMS, and the S4 append verb — is a near-from-scratch Rust build. (Nine deep, code-anchored subsystem specs back this doc: `docs/rust-rewrite/00-index.md`.) The dominant cost and risk of a big-bang is **not** writing Rust; it is **bit-for-bit data compatibility with live encrypted customer data** and a **flag-day cutover of a shipping product**. This doc scopes all of that and proposes how to de-risk a big-bang so it doesn't behave like one.

---

## 1. The three artifacts

### 1.1 `hippius-s3` — the product being replaced (the target)

Not a thin gateway; a full distributed object-storage system.

- **Code size:** ~41k LOC Python (`hippius_s3/`) + **~28k LOC Rust already in production** (`crates/hippius-drain-*`).
- **S3 surface:** 41 AWS S3 actions marked supported (`docs/s3-compatibility.md`, 41/108) — PutObject/GetObject/HeadObject/DeleteObject, CopyObject + UploadPartCopy, ListObjects v1/v2, ListObjectVersions, DeleteObjects batch, object & bucket tagging, ACLs (canned + get/put), bucket policy (get), lifecycle (get; **XML parsed then discarded** per `todo.md`), versioning toggle, object-lock config, GetBucketLocation, range + conditional requests, delete markers — plus a **proprietary `S4` atomic O(delta) append verb** (`hippius_s3/api/s3/extensions/append.py`, `docs/s4.md`).
- **Storage engine:** version-native from day one — every Put/Complete/Copy allocates an `object_versions` row and bumps `objects.current_object_version`; `?versionId=` served from history. Schema spans **86 SQL migrations** across **two Postgres DBs** (main data + a separate keystore).
- **Encryption:** envelope — per-object-version DEK (`os.urandom(32)`) wrapped by a bucket KEK, KEK wrapped by **OVH KMS** (mTLS) or a local AES-GCM wrap when KMS is disabled; wrapped DEK in the main DB (`object_versions.wrapped_dek`), KEK in a **separate keystore DB** (`bucket_keks`). Chunk cipher is **AES-256-GCM, v5 only**, wire `nonce(12)‖ct‖tag(16)` with a **random per-chunk nonce** (read back on decrypt — *not* derived), and a chunk AAD binding `bucket_id‖object_id‖part_number‖chunk_index` (per-part index). *(Crypto is the crux of data compat — see §5 and `docs/rust-rewrite/01-crypto-envelope.md`.)*
- **Data plane / cache:** producer/consumer streaming; multi-tier cache — api-local **NVMe/SSD → peer nodes → pool → Ceph → Arion backend** — with an FS `meta.json` part-completion protocol and prefetch/overlap streaming.
- **Async work system:** **5 Redis instances** (general cache, accounts/credit cache, durable work queues + DLQ, rate-limiting, ACL cache) and **~13 background workers** (Arion uploader, unpinner, janitor/FS-GC, MPU reaper, purger, usage-rollup, migrator, orphan-checker, account-cacher, plans-cacher, …).
- **Integrations:** Arion backend (which owns object pinning/on-chain storage — hippius-s3 does **not** submit extrinsics itself), a **read-only** Substrate credit query cached in redis-accounts, HTTP-scraped plan cache, IPFS CID addressing (CIDs produced by Arion over the ciphertext), OpenTelemetry/LGTM/Sentry. *(The subxt write pattern exists only in sibling `hcfs-chain-reporter`; giving the rewrite a direct chain-write path would be a product change, not a port — see `docs/rust-rewrite/07-chain-and-accounting.md`.)*
- **Auth/authorization:** **four** live auth methods (access-key SigV4 header, presigned SigV4, bearer token, anonymous — **seed-phrase SigV4 was removed**). Identity is **not locally verified**: every key/bearer request is validated against a remote `api.hippius.com` token-auth service and cached ~60s in Redis (hippius never stores the secret). Plus ACLs (canned + grants), sub-token R2-style scopes (enforced), HMAC admin/frontend, suspension, plan/quota gating.
- **Maturity:** actively developed (2,606 commits; shipping the day of this writing). Object-lock **delete** enforcement *is* now wired (403 on deleting a locked version), but the **S4 `append` verb bypasses the lock entirely** (mutates a version in place without an `is_version_locked` check) — a live WORM hole. The config surface exists; the enforcement story is incomplete (`object-lock.md`, `specs/s3-object-lock*.md`).

### 1.2 hcfs `service`-branch S3 module — the hoped-for asset

`hcfs-server/src/s3/`, branch `service` @ `778ea38`, **14,141 LOC** across 32 files.

- **Protocol coverage is broad and well-built:** SigV4 (header + query presigned) and legacy SigV2 (`auth/signing.rs`), buckets CRUD + the full sub-resource matrix, objects put/get/head/delete/copy/list v1+v2/rename/restore, DeleteObjects, PostObject form upload, GetObjectAttributes, multipart (create/upload-part/upload-part-copy/complete/abort/list, 1,619 LOC), versioning (758), lifecycle (1,000, with expiration loop), tagging, CORS, bucket policy with IAM-style eval (832), public-access-block, SSE, aws-chunked decoding. **~100 typed `S3Error` variants** with correct XML/status mapping. Clean routing/dispatch in `server.rs`.
- **But it is dead code, on the wrong architecture:**
  - Not declared anywhere (`no mod s3;`); required Cargo deps (quick-xml, hmac, sha1, md-5, crc32c, …) aren't even present. **It does not compile.**
  - Written for the **old sled + single-Arion** hcfs: **157 `sled` references across 22 files**, its own **19 sled trees** for metadata, hard-coded Arion HTTP (`/upload`, `/download/multi`, `/blobs`). It **does not use** today's `StorageBackend`.
  - Calls old free functions (`database::get_user_summary(&sled::Db)`, some *synchronously*) that are now async methods on `HcfsStore`; uses a now-private `middleware::generate_request_id`; destructures `ArionUploadResponse` fields that no longer exist.
  - **6 months / 738 commits stale** (diverged 2026-03-18); the sled→Postgres migration happened underneath it.
  - **~zero tests** for the module (`tests/verify_s3_files.rs` is unrelated backup-blob verification).
- **Architecturally it is a thin protocol translator** (S3 → one Arion, metadata in sled blobs). It has **none** of hippius-s3's envelope crypto, version-native engine, cache tiers, worker fleet, chain publishing, KMS, or S4 append. `restore.rs` is a stub; SelectObjectContent/GetObjectTorrent/STS/per-user-ACL-grants are `NotImplemented`.

**Reusability verdict (revised down after the file-by-file harvest audit, `docs/rust-rewrite/08`):** of the 14k LOC, roughly **~3.5% is reusable as-is, ~32% with edits (~5k LOC total, ~35%), ~63% must be rewritten, ~1% discarded**. The reusable third is the hard-to-get-right standards code — SigV4/SigV2 signing (`auth/signing.rs`), the `S3Error` catalog + `<Error>` XML, the IAM policy engine (`policy.rs`), the aws-chunked decoder, RFC-7232 conditional-header eval, and per-feature XML codecs. It needs a clean seam (`CredentialStore` / `MetadataStore` / `ObjectBackend` traits) so it's decoupled from both sled and Postgres. Several **error-code/XML divergences must be reconciled toward hippius's contract** (e.g. `MalformedPolicy`→`InvalidPolicyDocument`, added `x-amz-error-code` headers, and hippius does auth in the gateway rather than the S3 layer).

### 1.3 hcfs-server `main` — a reference, not the destination

Already on PostgreSQL (`HcfsStore`/`PgPool`) + a `StorageBackend` abstraction (Arion/S3/dual-write), and it already runs a *lightweight inline* S3-compatible gateway (bearer auth, no SigV4, no bucket/object model) that reuses production billing/auth/storage seams. Useful as a **worked reference** for how a Rust axum service wires Postgres + streaming storage + billing — but hcfs-server is a **different product** (client-side-encrypted file sync; "server never sees plaintext"; drive/share semantics) whose storage model is not hippius-s3's. It is not where the S3 product should live.

### 1.4 Precedent that matters: the `drain` subsystem

`crates/hippius-drain-*` (28k LOC Rust) already replaced the Python upload-promoter and **hard-cut-over to production July 2026** (`docs/drain-direct-rollout.md`). It proves (a) Rust is viable and welcome in this stack, and (b) the team can carve a hot subsystem out and cut it over cleanly. A big-bang should **reuse the drain as-is** and inherit its cutover playbook.

---

## 1.5 Live-customer signal (sn85 evidence store) — a deploy/gap problem, **not** a rewrite one

A customer (sn85 "evidence store", auditing/weights data — the sealed, write-once audit trail the validator depends on) reported three blockers. Grounding them against current `main`:

| Reported blocker | Actual state on `main` today | Verdict |
|---|---|---|
| **If-None-Match ignored** — 2nd PUT overwrote (200, not 412); breaks write-once | **Fixed.** `If-None-Match: *` create-only honored on PutObject + CompleteMultipartUpload → 412 `PreconditionFailed`. PR **#523** (`fix/put-if-none-match`), merged **2026-09-13**. Code: `objects/put_object_endpoint.py:93`, `common/headers.py` `parse_write_if_none_match`, `errors.py:247`. | **Already fixed — deploy lag.** The "200 instead of 412" is the exact pre-#523 behavior. |
| **Content-MD5 not verified** — wrong digest accepted | **Fixed.** MD5 parsed + verified on PutObject/UploadPart/append; mismatch → 400 `BadDigest`, malformed → 400 `InvalidDigest`. PR **#522** (`fix/content-md5-verification`), merged **2026-09-13**. Code: `put_object_endpoint.py:86`, `errors.py:272`. | **Already fixed — deploy lag.** |
| **No per-prefix policies** — bucket public as a whole; policy + versioning APIs return 403 | **Partly real.** Bucket policy is a **public/not-public toggle at the bucket grain** (`bucket_policy_endpoint.py:74` `set_bucket_policy` + `_validate_public_policy`), not general per-prefix/per-object IAM policy — so you cannot seal a *subset* of a public bucket. Versioning PUT exists (ENABLED ok; SUSPENDED → 501). The **403** is not explained by the policy/versioning code paths (they return 400/501/NoSuchBucketPolicy) — it points to either a **stale deployment** or an **auth/ACL rejection**, and must be reproduced against current `main`. | **Genuine gap** (per-prefix sealing) **+ needs reproduction** (the 403s). |

**Takeaways:**
1. **Two of three are already fixed and landed on `main` two days before the report (2026-09-13).** The most likely cause is that sn85 tested a **deployment behind `main`** (repo default is `origin/staging`; `chore/branch-flow-main-deploys-prod` + `docs/path-to-prod-cutover` indicate the prod cutover is in flight). **First action: verify the deployed version of the env sn85 hit and get them re-testing on current `main`.** None of this waits on — or is helped by — the Rust rewrite.
2. **The genuine gap is finer-grained access control** (seal a prefix inside an otherwise-public bucket). Decide whether to build it in Python now or design it into the Rust ACL model (§7).
3. **Write-once vs. true WORM:** create-only (`If-None-Match: *`, now shipped) prevents overwriting an *absent* key. If the validator's audit trail needs *guaranteed immutability* of an existing object, that is **object-lock**, whose enforcement is incomplete: the protocol dive found **delete** of a locked version *is* now blocked (403), **but the S4 `append` verb bypasses the lock and mutates a version in place** — so an append-based flow is not WORM-safe today. Confirm with sn85 whether they need (a) just no-clobber-on-create (shipped), or (b) true immutability (needs the object-lock/S4 gap closed first — see §4 Phase 4).

**These three become permanent conformance requirements / behavioral oracle for the rewrite** (§4 Phase 0): create-only → 412, Content-MD5 → 400 `BadDigest`, and a decided per-prefix policy model. They are exactly the "fix in Python first, then port with tests" case in the backlog policy (§5, risk 5).

---

## 2. What a big-bang rewrite must actually reproduce

Ordered roughly by (effort × risk). The service branch helps only with #1.

| # | Subsystem | Service branch helps? | Notes |
|---|-----------|-----------------------|-------|
| 1 | **S3 protocol front-end** (SigV4×2, XML, error codes, routing, bucket/object/multipart/policy) | ✅ **Yes — harvest it** | ~5k LOC (~35%) reusable, mostly *with edits* (~3.5% as-is); ~63% rewrite. The one real head start — but smaller than it first looks. |
| 2 | **Version-native storage engine** (`objects`/`object_versions`/`parts`/`part_chunks`/`chunk_backend`/`cid`, soft-delete, serveable-version predicate) | ❌ No | Must be schema-compatible with the live 86-migration DB, or migrated. |
| 3 | **Envelope encryption** (per-version DEK, bucket KEK, OVH KMS + local fallback; AES-256-GCM chunks with random prepended nonce; exact AAD layouts) | ❌ No | **Must be format-compatible to read existing objects.** Highest-risk item — the nonce is random (no derivation to match), but the AAD byte layouts, envelope wrap, and KEK derivation are exact contracts. |
| 4 | **Multi-tier cache + streaming** (NVMe→peer→pool→Ceph, `meta.json` protocol, prefetch, peer-serving internal API) | ❌ No | Interacts with the already-Rust drain. |
| 5 | **Async work system** (5 Redis, durable queues + retry ZSETs + DLQ, ~13 workers) | ❌ No | Each worker is its own port. |
| 6 | **Auth/authorization** (4 methods, ACLs, sub-token scopes, HMAC admin/frontend, suspension, plan-gate) | ⚠️ Partial (SigV4 canonicalization only) | The trust model differs fundamentally: hippius verifies against a **remote token-auth service** (no stored secret), the service branch stores raw secrets in sled. Harvest the SigV4 math, rebuild the orchestration/ACL/quota model. |
| 7 | **Credit + plan accounting** (read-only Substrate credit query + HTTP plan scrape, both cached in redis-accounts; object pinning delegated to Arion) | ❌ No | No extrinsic-submission in the write path. If direct chain writes are wanted, `hcfs-chain-reporter`'s subxt pattern is the template — but that's a product change. |
| 8 | **S4 atomic append** (proprietary verb, CAS, WORM reconciliation) | ❌ No | Not standard S3; unfinished interaction with object-lock. |
| 9 | **Object-lock enforcement** | ⚠️ Partial (config surface) | Unfinished in Python too — don't inherit a half-spec; finish the spec first. |
| 10 | **Observability/ops** (OTel/LGTM/Sentry/Cachet, runbooks, k8s) | ❌ No | Port config + dashboards; drain already shows the pattern. |

---

## 3. Proposed target architecture (Rust)

A single Cargo workspace (extend the existing one that houses `hippius-drain-*`), so the drain crates are reused directly, not reimplemented:

```
crates/
  hippius-drain-core / -agent / -allocator   # EXISTING — reuse as-is
  s3-protocol        # harvested from hcfs service branch (~5k LOC, ~35%, mostly with edits):
                     #   SigV4/SigV2, XML (de)serialize, S3Error + status/XML mapping, routing/dispatch,
                     #   IAM policy eval. Decoupled via CredentialStore/MetadataStore/ObjectBackend traits.
  s3-metadata        # version-native storage engine over sqlx/Postgres; owns the app schema
                     #   (dbmate, main DB) + keystore DB; serveable-version predicate; soft-delete.
                     #   Coexists with the drain's sqlx-owned cephor_* tables in the same main DB.
  s3-crypto          # envelope encryption: per-version DEK / bucket KEK, OVH KMS (mTLS) + local wrap,
                     #   AES-256-GCM chunks with a RANDOM prepended nonce (read back, not derived).
                     #   MUST match Python on AAD byte layouts + envelope/KEK wrap.
  s3-storage         # data plane: NVMe/peer/pool/Ceph tiers, meta.json protocol, prefetch
                     #   streamer, Arion client, peer-serving internal API. Bridges to drain.
  s3-queue           # Redis queues/DLQ/retry-ZSET vocabulary (mirror hippius_s3/queue.py wire format)
  s3-chain           # read-only Substrate credit query + HTTP plan scrape → redis-accounts caches
                     #   (NOT extrinsic submission; Arion owns pinning). subxt only if a chain-write path is added.
  s3-api             # the axum binary: middleware chain, 4 auth methods, ACL/quota gate, wires all above
  s3-workers         # the ~13 worker binaries (uploader, unpinner, janitor, mpu-reaper, purger,
                     #   usage-rollup, orphan-checker, cachers, …)
```

Key design rules to carry over from `hcfs`/CLAUDE and the drain:
- Zero-copy streaming on the data path; `spawn_blocking` for disk-heavy work; `CancellationToken` on every loop; store & await all `JoinHandle`s.
- No `debug_assert!` for real invariants (CI runs `--release`); prefer `Result`.
- Wire formats (Redis queue payloads, `meta.json`, on-chain calls, DB rows, ciphertext envelopes) are **contracts** with a live system — they are frozen inputs, not design freedom.

---

## 4. Phased plan (so a "big-bang" isn't a flag-day gamble)

The goal is a big-bang *destination* reached by *incremental, independently-verifiable* steps. Read-compatibility before write-compatibility; shadow before cutover.

- **Phase 0 — Foundations & contract capture.** Stand up the workspace + `s3-protocol` crate (harvest from service branch, make it compile & test against the AWS SDK conformance vectors). Freeze and document the wire contracts: ciphertext envelope layout (chunk framing `nonce‖ct‖tag`, the exact chunk + DEK AAD byte layouts, KEK wrap), the FS `meta.json` format (a hard contract with the drain agent), Redis queue payloads (`UploadChainRequest`/`UnpinChainRequest`), and the DB schema. Build a **cross-impl fixture suite**: objects written by Python must decrypt/serve identically in Rust.
- **Phase 1 — `s3-crypto` + `s3-metadata`, read-only.** Implement crypto and the version-native engine to the frozen contracts. **Gate:** a Rust "reader" can GET/HEAD/list *existing production objects* (byte-identical, all version/delete-marker semantics) — verified by replaying real requests in shadow against prod responses. This retires the single largest risk before any writing.
- **Phase 2 — `s3-storage` + write path (shadow).** Data-plane tiers + streaming, bridged to the existing drain. Rust handles writes in **shadow/dual-run** (write to Rust, serve from Python) with a reconciler diffing DB rows, ciphertext, and chain publishes.
- **Phase 3 — `s3-api` full surface + `s3-queue`/`s3-workers`.** Bring the whole 41-action surface + auth methods + quota/ACL under the Rust API; port workers one at a time behind the same Redis queues (they can run mixed Python/Rust during migration, as the drain cutover did).
- **Phase 4 — S4 append + object-lock.** Do these *last*, and **finish the object-lock/WORM spec first** (it's unfinished in Python). S4's WORM interaction is the open correctness question.
- **Phase 5 — Cutover.** Per-node / per-bucket canary → full, following the `drain-direct-rollout.md` playbook (hard cutover, no lingering flag, documented rollback).

**De-risking principles:** (1) read-compat is provable *before* you write anything; (2) every phase runs in shadow against prod traffic; (3) mixed Python/Rust operation is a supported state throughout (proven by the drain); (4) the ciphertext/queue/chain formats are never "improved" during the rewrite — do that later, separately.

---

## 5. Top risks

1. **Ciphertext format-compatibility (highest).** Existing objects are AES-256-GCM per chunk (`nonce(12)‖ct‖tag(16)`, **random** nonce read back on decrypt — so there's no derivation to match, which *lowers* this risk), under per-version-DEK / bucket-KEK / OVH-KMS envelopes. What must match exactly: the **chunk AAD** (`bucket_id‖object_id‖part_number‖chunk_index`, per-part index), the **DEK-wrap AAD** (`hippius-dek:{bucket}:{object}:{version}`), and the **local-KEK wrap key derivation**. Watch two live hazards the dives found: (a) the v5 **copy fast-path** reuses source chunk CIDs while the reader rebuilds AAD with the *destination* object_id → likely `InvalidTag` on read (verify before relying on CID reuse); (b) ~200k prod `object_versions` rows have a **NULL envelope** and 500 on read (a Python P0) — the greenfield Rust schema should add the `v5_requires_envelope` CHECK. Mitigation: Phase-1 read-only gate against real objects; cross-impl fixtures in CI; treat the AAD/envelope formats as frozen contracts.
2. **Live-product cutover.** hippius-s3 ships daily; the rewrite is a moving target. Mitigation: shadow/dual-run, mixed operation, per-node canary — the drain already proved this is achievable here.
3. **86-migration schema + 2 DBs + keystore.** The engine must match live rows exactly. Mitigation: reuse the existing schema verbatim; no schema redesign inside the rewrite.
4. **Inheriting unfinished semantics.** Object-lock enforcement and the S4/WORM interaction are open in Python. Mitigation: finish the *spec* before porting; don't reimplement a half-defined behavior.
5. **Scope creep from `todo.md` P0/P1s.** There's a live backlog: NULL-envelope `object_versions` rows (~200k, 500 on read), silent PUT stall on Arion placement, quota enforcement lagging a ~120s refresh with nothing accruing between refreshes, overwrites mis-charged as additive, the `active` plan flag ignored (the "quota keyed on caller-not-owner" P1 is now *partly stale* — the main path resolves the bucket owner; only the pay-as-you-go `can_upload` path still keys on the caller). Decide per-item: fix-in-Python-first vs. fix-in-Rust. A rewrite that also fixes bugs mid-flight loses its "identical behavior" oracle.
6. **The service branch tempting a shortcut.** Its storage/metadata layer looks done but is sled/single-Arion and must be thrown away. Only harvest the protocol layer; resist "just wire it up."
7. **Schema ownership is three-way in one place.** The main DB holds both the dbmate-owned app schema *and* the drain's **sqlx-owned `cephor_*` tables** (`sqlx::migrate!` in `hippius-drain-core`), and the keystore DB's two tables are **lazily created, not migrated**. A Rust rewrite must decide who owns migrations without colliding with the drain's migrator. Fail-open (billing) vs fail-closed (sub-token scope cache) asymmetries and the bucket-name-keyed Redis cache that outlives a bucket are load-bearing behaviors to preserve.

---

## 6. Rough sizing (order-of-magnitude, to be refined)

Not a commitment — a shape. Assumes a small senior Rust team already fluent in this stack (drain authors).

- **Harvest `s3-protocol` (Phase 0):** weeks. The code exists and is good; the work is de-coupling, deps, and conformance tests.
- **`s3-crypto` + `s3-metadata` read-only (Phase 1):** the pacing item — the crypto must be exactly right and proven against prod. Plan in months, not weeks.
- **`s3-storage` + write shadow (Phase 2):** months, overlapping the drain's existing surface.
- **Full API + workers (Phase 3):** the long tail — ~13 workers + 41 actions + 5 auth methods. Months.
- **S4 + object-lock + cutover (Phases 4–5):** gated on spec completion.

Realistically a **multi-quarter program** for a full, data-compatible replacement — dominated by crypto/data-compat and the worker fleet, *not* by the S3 protocol (the part the service branch already covers).

---

## 7. Open decisions (need answers to finalize the plan)

1. **Home:** does the Rust rewrite live in the existing `hippius-s3` Cargo workspace (alongside the drain), or a new repo? (Recommendation: same workspace — reuse drain, share CI.)
2. **Schema:** freeze and reuse the live 86-migration schema exactly, or take the rewrite as a chance to consolidate? (Recommendation: freeze; consolidate later, separately.)
3. **Backlog policy:** which `todo.md` P0/P1s get fixed in Python *before* the rewrite starts (to give a clean behavioral oracle) vs. carried into Rust?
4. **Object-lock:** finish the enforcement spec (`specs/s3-object-lock*.md`) before or during the rewrite? (Recommendation: before — Phase 4 depends on it.)
5. **KMS:** is OVH KMS staying, or is there appetite to change the KEK-wrapping story during the move? (Recommendation: keep for compat; revisit post-cutover.)
6. **Team & timeline:** who staffs it, and is multi-quarter acceptable vs. continuing the in-place strangler-fig the drain started?
7. **Migrations ownership:** does the Rust `s3-metadata` crate take over dbmate's app schema (and how does it coexist with the drain's sqlx `cephor_*` migrator in the same DB), or does dbmate stay authoritative during the transition? (Recommendation: dbmate stays authoritative until Phase 3; Rust reads the schema, doesn't own it, until then.)
8. **S4-append WORM hole:** fix the append-bypasses-object-lock gap in Python now (it's a correctness/compliance issue independent of the rewrite), or only in Rust? (Recommendation: Python now if any customer relies on object-lock — e.g. sn85.)
9. **Seed-phrase auth:** it was removed — is that permanent, or does the Rust build need to revive it? (Affects whether `s3-crypto`/auth pulls in sr25519/bip39.)

---

## 8. Appendix — key source pointers

- **Deep subsystem specs (implementation-grade, code-anchored):** `docs/rust-rewrite/` — `00-index.md` (synthesis), `01-crypto-envelope.md`, `02-storage-engine-schema.md`, `03-data-plane-cache-streaming.md`, `04-queues-and-workers.md`, `05-auth-authz-billing.md`, `06-s3-protocol-conformance.md`, `07-chain-and-accounting.md`, `08-service-branch-harvest.md`, `09-ops-deploy-observability.md`.

- **Target product:** `hippius_s3/api/s3/` (router, buckets/, objects/, multipart.py, extensions/append.py), `hippius_s3/{writer,reader,cache,services,repositories,gateway,workers,dlq}/`, `hippius_s3/{config.py,queue.py,main.py}`, `hippius_s3/sql/migrations/` (86), `workers/run_*_in_loop.py`, `crates/hippius-drain-{core,agent,allocator}/`. Docs: `object-lock.md`, `object-versions.md`, `todo.md`, `docs/drain-direct-rollout.md`, `docs/s3-compatibility.md`, `docs/s4.md`, `specs/s3-object-lock*.md`.
- **Asset (harvest protocol layer):** hcfs branch `service` @ `778ea38`, `hcfs-server/src/s3/` — reuse `auth/signing.rs`, `auth/mod.rs`, `error.rs` (minus sled/bincode `From` impls), `mod.rs` helpers, `utils.rs`, `chunked.rs`, `policy.rs`, XML models, `server.rs` dispatch. Discard: `db.rs` + all 19 sled trees, `arion.rs` hard-coded HTTP, the billing free-fn calls. Reference: `hcfs-server/S3_TODO.md` (**stale** — predates Postgres/StorageBackend).
- **Reference impl (Rust axum + Postgres + streaming storage + billing seams):** hcfs `main` — `hcfs-server/src/{state.rs,store/,storage/backend.rs,http/router.rs,http/handlers/upload.rs,auth/}`, and the `hcfs-retry-worker` / `hcfs-chain-reporter` worker crates.
```
