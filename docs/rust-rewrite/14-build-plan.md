# 14 — Greenfield build plan (crate DAG + logical build order)

**Date:** 2026-09-15 · **Status:** first cut, ready to refine · **Companions:** all of [`00-index.md`](./00-index.md)

The capstone that sequences the whole implementation. Every architectural decision it rests on is settled (see `00-index` → "Architecture decided"). It assumes: greenfield Rust in a new hippius-s3 branch · separate Postgres + optimized schema (doc 12) · single axum app · SSD-staging ingest → HCFS (doc 10) · **HCFS as the sole, unchanged backend** (doc 13) · the S3 service owns dedup/refcount/metadata/envelope-crypto/chunking · **full vanilla-S3 parity, no incomplete ship** · existing data re-encrypted in a later migration. Sole implementor, no timeline pressure — so this optimizes for **logical order and completeness**, not for cutting scope.

## Crate DAG

```
        ┌───────────────┐      ┌───────────────┐
        │  s3-crypto    │      │  s3-protocol  │  (harvest ~35% from hcfs service branch:
        │ envelope/KMS/ │      │ SigV4/SigV2,  │   SigV4, XML, S3Error, policy eval, routing;
        │ AEAD + OLD-   │      │ XML, errors,  │   decoupled via traits)
        │ format decrypt│      │ policy, dispatch│
        └──────┬────────┘      └──────┬────────┘
               │                      │
        ┌──────▼──────────────────────▼───────┐        ┌────────────────────┐
        │            s3-metadata               │        │   s3-hcfs-client    │  (zero-change:
        │ optimized schema (doc 12), sqlx;     │        │ store/get-range/    │   existing hcfs
        │ serveable predicate; dedup+refcount; │        │ delete via existing │   endpoints, admin
        │ staged_blobs; multipart/versioning   │        │ hcfs endpoints;     │   bearer + per-
        │ metadata                             │        │ per-tenant ss58     │   tenant ss58)
        └──────┬───────────────────────────────┘        └─────────┬──────────┘
               │                                                   │
        ┌──────▼───────────────────────────────────────────────────▼──────┐
        │                          s3-ingest                                │
        │ SSD landing (reuse drain-core pure + localfs.rs meta.json/flock); │
        │ per-node forwarder → s3-hcfs-client; staged_blobs state; cleanup  │
        └──────┬────────────────────────────────────────────────────────────┘
               │
        ┌──────▼───────────────────────────────────────────┐     ┌──────────────────┐
        │                     s3-api                        │     │  s3-migration    │
        │ axum app: middleware, 4 auth methods, ACL/policy, │     │ read old fmt →   │
        │ per-bucket ack policy, serve-pending-from-SSD,    │     │ decrypt (old) →  │
        │ wires metadata + crypto + ingest + hcfs-client;   │     │ re-encrypt → new │
        │ HCFS credit-gate + usage→chain inherited          │     │ service (backfill)│
        └───────────────────────────────────────────────────┘     └──────────────────┘

Reused as-is: `drain-core` (pure types/algorithms), `localfs.rs` (SSD meta.json/flock contract).
```

## Build order (phases, each with an exit gate)

### Phase 0 — Foundations & contracts
Stand up the base everything hangs off. Parallelizable internally.
- **Workspace + CI** mirroring the drain (edition 2024, `deny.toml`/rustfmt/toolchain pins, the `unwrap/panic`-deny lints, the `Dockerfile.drain` 2-stage pattern).
- **`s3-crypto`** — envelope scheme **FROZEN** ([`25-crypto-verification.md`](./25-crypto-verification.md), suite `hip-enc/aes256gcm-ctx-frames-v1`): random per-blob DEK; **STREAM nonce** `prefix(7)‖frame_index(4 BE)‖final_flag(1)`; 256 KiB **CTX frames** with AAD = **opaque `blob_id`** ‖ `LE32(frame_index)` ‖ suite (pre-assigned, DEK-independent — not object_id, not a hash); key commitment **intrinsic to the frame wire** (32-byte BLAKE3 `CT` per frame — CMT-4; **no** stored `PRF(DEK, blob_id)`); **per-owner copy-oriented dedup** (encrypt once, refcount, re-wrap the DEK per owner → O(1) CopyObject); **reject cross-tenant convergent/MLE**. Also implement OVH KMS matching Python (doc 01) and the **old-format decrypt path** the migration needs. Remaining human step: a cryptographer's **code review of the wrapper** (`ctx-poc/` is the reference), not a spec ratification.
- **`s3-metadata`** — the doc-12 schema as sqlx migrations: core object/version/part/chunk/blob tables, the serveable predicate defined **once**, dedup+refcount primitives, `staged_blobs`.
- **`s3-hcfs-client`** — store / ranged-GET / delete against the *existing* hcfs surface (doc 13), admin bearer + per-tenant ss58, idempotent re-POST. **Integration-tested against a real hcfs now** (no hcfs changes needed).
- **`s3-protocol`** — harvest from the service branch (SigV4/SigV2, XML, `S3Error`, policy eval, dispatch), decouple via `CredentialStore`/`MetadataStore`/`ObjectBackend` traits, pass SigV4 conformance vectors.
- **Conformance test harness** — stand up the AWS-SDK/`s3-tests`-style suite that becomes the acceptance oracle (doc 06 is the checklist).

**Gate:** a Rust binary encrypts a blob → stores via hcfs → reads it back ranged → decrypts, byte-identical; schema migrates; SigV4 vectors pass.

### Phase 1 — Vertical slice (core object path, end to end)
- **`s3-ingest`** — SSD landing + `meta.json` (reuse `localfs.rs`), the forwarder → `s3-hcfs-client`, `staged_blobs` land→replicated state, post-confirm cleanup; **fast-ack** default path.
- **`s3-api` (minimal)** — one auth method (access-key SigV4), CreateBucket / DeleteBucket / HeadBucket / ListBuckets, PutObject / GetObject(range) / HeadObject / DeleteObject / ListObjectsV2. Wire metadata + crypto + ingest + hcfs-client; **credit-gate + usage attribution inherited from hcfs**; scaffold the **per-bucket ack policy** (fast-ack + serve-pending-from-SSD; WORM sync-before-ack stub).

**Gate:** `aws s3` / SDK does bucket + object CRUD + list against the Rust service; bytes are durable in hcfs; usage is attributed to the tenant ss58; read-after-write holds (serve-pending-from-SSD + sticky routing).

### Phase 2 — Full vanilla-S3 parity (breadth)
Everything required to *not ship incomplete*, built on the slice:
- **Multipart** (create / upload-part / upload-part-copy / complete / abort / list-parts / list-uploads) with the **MPU staging tree → promote-on-complete + DEK re-wrap** (doc 12's biggest design bet — review here).
- **Versioning** (native), delete markers, version-scoped GET/HEAD/DELETE, ListObjectVersions.
- **CopyObject / UploadPartCopy** — O(1) metadata copy via `content_hash` refcount (enabled by the AAD rebind).
- **Conditional requests** (If-None-Match create-only → 412; If-Match / If-*-Since), **Content-MD5** verification, full **Range** edge cases.
- **Tagging, ACLs** (canned + grants), **bucket policy incl. real per-prefix** (the genuine sn85 gap, done right), **CORS, lifecycle, GetObjectAttributes, PostObject**.
- **All 4 auth methods** (SigV4 header, presigned, bearer, anonymous).
- Complete **error catalog + XML** parity.

**Gate:** the doc-06 conformance checklist is **fully green** — the "no incomplete ship" bar.

### Phase 3 — Object-lock / WORM (+ S4)
The deliberate-correctness features, done right rather than inherited half-done:
- **Finish the object-lock enforcement spec first**, then implement retention / legal-hold / GOVERNANCE+COMPLIANCE so that delete, overwrite, **and** any append all respect the lock — closing the Python WORM hole by construction. WORM buckets use **sync-to-HCFS-before-ack**.
- **S4 append** — decide scope (it's a non-vanilla Hippius extension; include only if a client needs it), and if included, make it lock-aware.

**Gate:** object-lock enforcement tests pass; WORM guarantees demonstrably hold (esp. for sn85's use case).

### Phase 4 — Data migration (re-encrypt backfill)
- **`s3-migration`** — read existing prod objects via the old crypto/format (from `s3-crypto`'s old-decrypt path), re-encrypt under the new scheme, write into the new service; **idempotent + resumable**, with reconciliation (counts, plaintext hashes).

**Gate:** a pilot bucket migrates with plaintext-identical verification and serves from the new service.

### Phase 5 — Cutover
- Shadow / dual-run, then per-bucket / per-account traffic shift, following the drain-direct playbook (doc 09); decommission Python.

**Gate:** production traffic on Rust; Python scaled to zero.

## Critical path & parallelism

- **Critical path:** `s3-crypto` (needs the AAD sign-off) → `s3-metadata` → Phase-1 vertical slice → Phase-2 parity.
- **Parallelizable early:** `s3-hcfs-client` (zero hcfs changes → integration-testable against prod hcfs on day one), `s3-protocol` harvest, and the conformance harness.
- **Must be in `s3-crypto` from the start:** the **old-format decrypt** path — Phase 4 depends on it, and designing it in late is costly.
- **Cross-cutting, continuous:** the conformance suite (grows every phase), observability/OTel metric parity (doc 09), security review of the crypto.

## Decisions still needing sign-off before/within these phases

| Decision | Needed by | Doc |
|---|---|---|
| **AAD/dedup model** — FROZEN (doc 25): opaque `blob_id` AAD + per-owner copy dedup + intrinsic CTX-frame commitment; reject convergent. Remaining: the Phase-0 **code review** of the wrapper (not a spec ratification). | Phase 0 (blocks `s3-crypto`) | 25, 12 |
| **Tenant → ss58 account model** (so per-tenant billing attribution is correct + non-exempt) | Phase 1 | 11/13 |
| **Object-lock enforcement spec** (finish before implementing) | Phase 3 | 06, `specs/` |
| **S4 append scope** (include the non-vanilla verb or not) | Phase 3 | 06 |
| **MPU staging-tree + promote/re-wrap** design review (biggest schema bet) | Phase 2 | 12 |

## What this plan deliberately does *not* build

(Because HCFS owns it, or the decisions removed it): Arion/S3 dual-write, the Arion retry-worker, usage→chain reporting, the Ceph pool, the drain-allocator, the multi-tier read cache, the janitor/hydrate system, peer-serving. These are HCFS's job or were cut — see docs 09, 10, 13.
