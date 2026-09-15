# 20 — Data Migration / Re-encrypt Backfill (old hippius-s3 → greenfield Rust S3)

**Date:** 2026-09-15 · **Status:** first cut, ready to refine · **Owns:** doc 14 Phase 4 (`s3-migration`) and feeds Phase 5 (cutover).
**Companions:** [`01-crypto-envelope.md`](./01-crypto-envelope.md) (OLD format to decrypt), [`02-storage-engine-schema.md`](./02-storage-engine-schema.md) (OLD schema to read), [`12-schema-design.md`](./12-schema-design.md) (NEW schema to write), [`crypto-dedup-research.md`](./crypto-dedup-research.md) (NEW crypto scheme), [`13-hcfs-as-is-integration.md`](./13-hcfs-as-is-integration.md) (NEW backend write path), [`14-build-plan.md`](./14-build-plan.md) (phases), [`09-ops-deploy-observability.md`](./09-ops-deploy-observability.md) §5 + [`../object-migration.md`](../object-migration.md) + [`../drain-direct-rollout.md`](../drain-direct-rollout.md) (prior migration/cutover mechanics).

---

## 0. What this migration is, and what makes it different

The greenfield Rust service has its **own separate Postgres**, an **optimized schema** (doc 12), and a **new crypto scheme** (per-owner copy-oriented dedup, content-identity AAD, key-committing AEAD — doc `crypto-dedup-research.md`). It shares **one thing** with the old system: the durable-byte backend is the *same* HCFS (doc 13). So migration is **not** a schema copy and **not** a bit-compat in-place upgrade. It is:

> **read each old object-version → decrypt with the OLD envelope (doc 01) → re-encrypt with the NEW scheme through the real new write path → write into the new service → prove it faithful → shift that bucket's traffic → decommission the old copy.**

This is exactly what buys the greenfield build its freedom from in-place bit-compat (doc 12 preamble; doc 14 §5 "existing data re-encrypted in a later migration"). Two consequences drive the whole design:

1. **The old ciphertext is never rewritten.** The migrator only *reads* the old store and *writes new bytes*. The old store is a read-only replica for the whole window. This is what makes **rollback = repoint** (§4) and what makes the migration abort-safe (nothing old is mutated).
2. **Re-encryption is non-deterministic.** The new scheme uses a **random DEK + random nonce**, encrypt-once (doc `crypto-dedup-research.md` §Q4). Re-encrypting the same plaintext twice yields *different* ciphertext and a *different* `blobs.content_hash`. So idempotency and dedup cannot rely on "encrypt and let the content hash collide" — they must be driven by a **migration-side plaintext→blob map** (§1.4). This is the single most important structural fact of the backfill.

> ⚑ **The old service already ships a migrator** (`../object-migration.md`): it upgrades objects *below* `HIPPIUS_TARGET_STORAGE_VERSION` to v5 **in place, in the old schema/crypto**. This doc's migrator is a *different animal* — cross-schema, cross-crypto, cross-database. We **reuse its proven operational contract** (worklist + resumable state file + CAS-swap-on-finalize + cleanup-is-sole-unpin-authority + source-change detection; doc 09 §5.5) but not its code.

---

## 1. The migrator

### 1.1 Architecture — a Rust job, `s3-migration` (doc 14 Phase 4 crate)

A standalone Rust binary in the new workspace, built on the **same crates the live service uses** so that a migrated object is byte-format-identical to a natively-PUT object. It never re-implements crypto or metadata writes.

```
                      ┌──────────────────────────── s3-migration (this crate) ───────────────────────────┐
                      │  enumerate → claim → read+decrypt(OLD) → re-encrypt(NEW write path) → verify →     │
                      │  record progress.  Idempotent · resumable · parallel · throttled · abort-safe.     │
                      └───────────────┬───────────────────────────────────────────┬─────────────────────┘
   OLD side (read-only)               │                                            │            NEW side (write)
 ┌──────────────────────┐            │                                            │      ┌──────────────────────────┐
 │ OLD main Postgres     │───(enum + ov/parts/chunks/acl/tags/lock/names)──────────┐    │ NEW Postgres (doc 12)     │
 │ (86-migration schema) │            │                                            │    │ accounts/buckets/objects/ │
 ├──────────────────────┤            │                                            │    │ object_versions/parts/    │
 │ OLD keystore Postgres │──(bucket_keks: wrapped KEK, kms_key_id)──┐              │    │ chunks/blobs/chunk_blobs/ │
 │ (hippius_keys)        │            │                            │              │    │ bucket_keks/acl/tags…     │
 └──────────────────────┘            │                            ▼              │    └──────────────────────────┘
 ┌──────────────────────┐            │                    ┌───────────────┐      │      ┌──────────────────────────┐
 │ OVH KMS (mTLS)        │◀───(unwrap KMS-wrapped KEK)─────│  s3-crypto    │      └────▶ s3-ingest / s3-hcfs-client │
 │  or local wrap key    │            │                    │  OLD-decrypt  │             │ POST /upload (admin bearer,│
 └──────────────────────┘            │                    │  + NEW-encrypt│             │ account_ss58=tenant),      │
 ┌──────────────────────┐            │                    └───────────────┘             │ ranged GET, DELETE (doc 13)│
 │ HCFS (SHARED backend) │◀──(GET old chunk ciphertext by old backend_identifier)───────┤ (writes NEW blobs here too)│
 │  holds OLD *and* NEW  │────────────────────────(PUT new ciphertext blobs)────────────┘                            │
 └──────────────────────┘                                                                └──────────────────────────┘
```

- **Read side** binds to the OLD main DB + OLD keystore DB (`HIPPIUS_KEYSTORE_DATABASE_URL`, doc 01 C13) read-only, and to OVH KMS with the OLD mTLS certs (doc 01 §5.1) for KMS-wrapped KEKs. Old chunk ciphertext is fetched from HCFS by the old `chunk_backend.backend_identifier` (the HCFS file_id / path hash; doc 02 §2.7) or legacy CID.
- **Write side** uses the **exact live new-write path**: `s3-crypto` (NEW encode) → `s3-ingest`/`s3-hcfs-client` → HCFS, and `s3-metadata` for all rows. A migrated blob lands in HCFS with a unique name and flips `blobs.replication_state pending→durable` on HCFS confirm, identical to a live PUT (doc 12 §2.9, §7; doc 13 store contract).
- **Deploy shape:** one Rust image, `command:`-selected binary (the drain pattern, doc 09 §5.2). Run it as a scalable batch **Job/Deployment** with N worker replicas coordinating through the progress tables (§1.5) — not a singleton. It is **not** on the request path.

> **Because HCFS is shared, the window doubles HCFS storage for a migrating bucket** (old ciphertext + new ciphertext coexist until the old copy is GC'd post-cutover). See §4.4 and OQ-7.

### 1.2 Enumeration — worklist over buckets → objects → versions

Two-level, snapshot-then-catch-up (§3.3). The **unit of work is one old object-version** `(bucket_id, object_id, object_version)`; objects and buckets are grouping/ordering keys. Enumeration runs entirely off the OLD DB (no byte reads):

1. **Accounts & buckets** — enumerate `users`→`accounts` and `buckets` (config: versioning, `object_lock` default, ACL `acl_json`, policy/CORS/lifecycle where present). Metadata-only.
2. **Objects** — per bucket, all `objects` rows (live and soft-deleted-but-name-held).
3. **Versions** — per object, **all** `object_versions` rows we must preserve: every committed content version *and* every delete marker, within the S3-visible set. Migrate the full version chain, not just `current_object_version`, when the bucket has versioning history that S3 clients can address (`ListObjectVersions`, `?versionId` GET). ⚑ OQ-9 (scope: full history vs. current-only) is a product call.
4. **Aliases** — `object_names` rows (same-bucket CopyObject aliases in the old schema; doc 02 §2.11) become **metadata-only copies** in the new schema (§2.6, doc 12 §9 Q2).
5. **In-progress MPU** — `multipart_uploads` with `is_completed=false`: **not migrated by default** (§3.2, OQ-4).

The worklist is materialized into the migrator's own **`mig_object_versions`** progress table (§1.5), keyed by the old composite id, so enumeration is itself resumable and the O(objects) selection query can be **batched by bucket** to avoid materializing millions of tasks at once (a limit `../object-migration.md` calls out explicitly). Filters mirror the Python CLI: `--account`, `--bucket`, `--bucket-prefix`.

### 1.3 Read + decrypt (OLD envelope) — the frozen-contract read path

For a content version, decrypt exactly as doc 01 §9 specifies (this is a **hard byte-exact contract**, doc 00-index "Frozen contracts", C1–C15):

1. Load the old `object_versions` row: `bucket_id, object_id, object_version, storage_version, enc_suite_id, kek_id, wrapped_dek, size_bytes(plaintext), md5_hash, body_blake3, content_type, metadata, multipart, completed_part_numbers`, plus the version's `parts` (each `part_number`, `size_bytes`, `chunk_size_bytes`) and `part_chunks` (`chunk_index`, `cipher_size_bytes`, `plain_size_bytes`) and `chunk_backend` handles.
2. **Unwrap the DEK:** fetch the bucket KEK for `(bucket_id, kek_id)` from the **old keystore** and unwrap via OVH KMS (mTLS, JWE) or the local wrap key (`SHA-256("hippius-local-kek-wrap-v1:"‖secret)`, AAD=None; doc 01 §5.4/C6). Then `unwrap_dek(kek, wrapped_dek, aad="hippius-dek:{bucket_id}:{object_id}:{object_version}")` (C5) → 32-byte plaintext DEK.
3. **Per chunk:** fetch the ciphertext blob from HCFS by the old handle, read the **prepended random 12-byte nonce** (C2 — never derive), reconstruct the chunk AAD `LE16(len bucket_id)‖bucket_id‖LE16(len object_id)‖object_id‖LE32(part_number)‖LE32(chunk_index)` (C3, `chunk_index` **per-part**, C9), `AES-256-GCM.decrypt`. Use the **per-part `chunk_size_bytes` from the DB**, never config (C8).
4. Stream the decrypted plaintext chunk-by-chunk (per-part order) into the re-encrypt pipeline; **never buffer the whole object** (respect the RAM warning in `../object-migration.md` and CLAUDE.md's no-blocking-runtime rule).

Delete markers and 0-byte objects carry no chunks — metadata-only.

> ⚑ **Legacy storage_version < 5.** The old *read* service rejects `< 5` (C15) — so production's *serveable* set should be all-v5, because the old in-place migrator (`../object-migration.md`) upgrades below-target versions to v5. But rows with `storage_version ∈ {1,2}` may still physically exist (doc 02 §2.4). The backfill must **either** (a) confirm none are serveable and skip them, **or** (b) carry v1/v2 decrypt too. Decide before Phase 4 (OQ-1; doc 12 Q10).
> ⚑ **Copy fast-path AAD (doc 01 Q1).** If any old object was produced by `execute_v5_fast_path_copy` reusing source chunk CIDs under a changed `object_id`, its chunks may fail `InvalidTag` when decrypted with the destination `object_id` in the AAD. Those objects **cannot be decrypted the normal way**. Detection + policy (re-key from the source object_id, byte-preserve, or quarantine) is OQ-2. Probe this on the pilot bucket first.

### 1.4 Re-encrypt (NEW scheme) — through the real write path, with a plaintext→blob dedup map

**Principle: the migrator re-encrypts by feeding plaintext through the same `s3-crypto` + `s3-ingest` write path a live PUT uses.** It passes the *preserved* identity (`content_type`, `user_metadata`, `object_lock`, tags, ACLs, the **exact old `version_id`**, the **exact old ETag**) as the write's intended metadata, and lets the write path mint the NEW envelope (frozen, doc 25): random **per-blob** DEK, **STREAM nonce** (`prefix(7)‖frame_index(4 BE)‖final_flag(1)`), 256 KiB **CTX frames** with AAD = **opaque `blob_id`** ‖ `LE32(frame_index)` ‖ suite (`blob_id` pre-assigned/DEK-independent, *not* object_id and *not* a content hash), key commitment **intrinsic to the frame wire** (32-byte BLAKE3 `CT` per frame — *no* separate `PRF(DEK, blob_id)`), DEK wrapped under the bucket's **new** KEK, refcounted `blobs`/`chunk_blobs` (doc 12 §2.9–§2.10).

**The dedup/idempotency map (mandatory, because re-encrypt is non-deterministic — §0.2):**

- Maintain a persistent, **owner-scoped** table `mig_plaintext_blobs(account_id, plaintext_chunk_key) → (new content_hash, new blob DEK-ref)` where `plaintext_chunk_key = blake3(plaintext_chunk)` at the new scheme's chunk boundary.
- When re-encrypting a chunk: look up `(account_id, plaintext_chunk_key)`.
  - **Miss:** encrypt once (random DEK/nonce), write the new blob to HCFS, insert `blobs`+`chunk_blobs`, record the map row. This realizes per-owner "encrypt once" (doc `crypto-dedup-research.md` §Q5).
  - **Hit:** **reuse** the existing blob — add a `chunk_blobs` reference (bump refcount) and re-wrap the DEK for this reference under the owner KEK. No re-encryption, no second nonce (preserves the write-once model, avoids nonce reuse — §Q4).
- This map is **the** thing that makes the backfill idempotent: a re-run of a half-done object re-derives the same `plaintext_chunk_key`, hits the map, and reattaches references instead of creating duplicate blobs. It is also what delivers the migration's storage-dedup savings within an account.

> ⚑ **DEK placement dependency (schema vs. research).** Doc 12's draft schema still carries `wrapped_dek` on `object_versions` (Python-inherited, one DEK per version), while `crypto-dedup-research.md` (and doc 12's own top-note) prescribe a **per-blob** DEK re-wrapped per reference. These are inconsistent and unresolved (doc 12 Q2, "Draft — needs review"). The migrator must target **whatever `s3-crypto` Phase-0 finalizes** — it consumes the write API, it does not choose the layout. But the choice changes (a) where wrapped DEKs are written and (b) whether the dedup map keys blobs or object-versions. This is OQ-3 and **blocks** `s3-crypto`, which blocks Phase 4 (doc 14 critical path).

**Metadata to preserve exactly (all of it, not just bytes):**

| Field | Old source | New target | Verified? |
|---|---|---|---|
| `version_id` | `object_versions.object_version` | `object_versions.version_id` (write via the out-of-band allocator; doc 12 §5.2 notes reuse is safe under content-addressing) | count + spot |
| ETag (single MD5 / composite `md5-N`) | `object_versions.md5_hash`, per-part `parts.etag` | `object_versions.etag`, `parts.etag` | **100%** |
| plaintext size | `object_versions.size_bytes` | `object_versions.size_bytes` | **100%** |
| `body_blake3` (plaintext BLAKE3) | `object_versions.body_blake3` | recomputed by write path → `object_versions.body_blake3` | **100%** (the anchor, §2) |
| content-type, user metadata | `content_type`, `metadata` | `content_type`, `user_metadata` | 100% (equality) |
| delete marker | `is_delete_marker` | `is_delete_marker` | count |
| object lock | `object_lock_mode`, `object_lock_retain_until`, `object_lock_legal_hold` | `lock_mode`, `lock_retain_until`, `legal_hold` | **100%** |
| tags | (current-version scope) | `object_tags` | set equality |
| object ACL | `object_acls.acl_json` | `object_acl_grants` (normalized, doc 12 §2.11) | grant-set equality |
| bucket ACL/policy/CORS/lifecycle/versioning/lock-default | `buckets.*`, `bucket_acls.acl_json` | `buckets.*`, `bucket_acl_grants` | equality |
| same-bucket copy alias | `object_names` row | metadata-only copy (shared `chunk_blobs`, new `object_id`; doc 12 §9 Q2) | count |

### 1.5 Progress, idempotency, resumability, parallelism, throttling

Progress lives in the **migrator's own tables** (a `mig` schema in the NEW database — same DB the new service uses, so a single sqlx migrator owns it, doc 12 §8; keeps state and target transactionally close):

```sql
-- one row per old object-version unit of work
CREATE TABLE mig.object_versions (
    old_bucket_id  uuid NOT NULL,
    old_object_id  uuid NOT NULL,
    old_version    bigint NOT NULL,
    account_id     text NOT NULL,
    kind           text NOT NULL CHECK (kind IN ('content','delete_marker','alias')),
    status         text NOT NULL DEFAULT 'pending'
                     CHECK (status IN ('pending','claimed','writing','written','verified','failed','skipped','superseded')),
    -- source fingerprint for source-change detection (§3.3)
    src_md5        text,
    src_blake3     text,
    src_last_modified timestamptz,
    src_append_version int,
    -- results
    new_object_id  uuid,
    new_version    bigint,
    verify_blake3  text,
    attempts       int NOT NULL DEFAULT 0,
    claimed_by     text,           -- worker lease (id)
    claimed_at     timestamptz,
    last_error     text,
    updated_at     timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (old_bucket_id, old_object_id, old_version)
);
CREATE INDEX ON mig.object_versions (status, account_id) WHERE status IN ('pending','failed');

-- per-owner plaintext→blob dedup/idempotency map (§1.4)
CREATE TABLE mig.plaintext_blobs (
    account_id         text NOT NULL,
    plaintext_chunk_key text NOT NULL,   -- blake3(plaintext chunk)
    new_content_hash   text NOT NULL,    -- REFERENCES blobs
    created_at         timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (account_id, plaintext_chunk_key)
);

-- per-bucket state machine cursor (§ state machine below)
CREATE TABLE mig.buckets (
    old_bucket_id uuid PRIMARY KEY,
    account_id    text NOT NULL,
    phase         text NOT NULL DEFAULT 'discovered',
    watermark     timestamptz,           -- last_modified high-water for catch-up
    frozen_at     timestamptz,
    cutover_at    timestamptz,
    counts_json   jsonb,                  -- reconciliation snapshot (§2)
    updated_at    timestamptz NOT NULL DEFAULT now()
);
```

- **Idempotent:** every unit keyed by the old composite id; `--resume` skips `written`/`verified`/`skipped` (the Python `--resume` contract, `../object-migration.md`). Re-encrypt hits the plaintext map (§1.4) so a retried unit never duplicates blobs. A partially-written object is safe: the new `object_versions` row stays `state='pending'` (doc 12 §2.6) — invisible to reads — until finalize flips it `committed`; an abandoned pending row is reaped, never served (doc 12 W1).
- **Resumable:** a worker lease (`claimed_by`/`claimed_at`) with a TTL; a crashed worker's stale claims are reclaimable. Checkpointing is the DB row itself (stronger than the Python atomic state-file; keep an optional periodic JSON export for offline inspection to match the Python `--state-file` habit).
- **Parallel:** N workers `SELECT ... FOR UPDATE SKIP LOCKED` a batch of `pending` rows per account/bucket. Concurrency is **objects-in-parallel** (matching the Python `--concurrency` semantics — parts within an object stream), and it is bounded per-account so one huge tenant can't starve others.
- **Throttled — but not by artificial caps in the data path.** Per CLAUDE.md ("no semaphores/rate-limiters/throughput caps in the *server* data path"), the *server* is never throttled. This is a **batch job**, so it self-limits its *own* footprint: a worker-count / per-account-parallelism knob, and an adaptive backpressure signal off HCFS ingest latency and OVH KMS 429/5xx backoff (doc 01 §5.5) so the backfill yields to live traffic. Tuning is operational, not a correctness gate.
- **Abort-safe:** on any failure the unit is marked `failed` and skipped; **nothing on the old side is mutated** and no new version is made serveable. This mirrors the Python "mark failed, don't inline-unpin, cleanup is the sole authority" contract (`../object-migration.md`; doc 09 §5.5).

---

## 2. Reconciliation & verification — proving faithfulness

**The anchor is the plaintext BLAKE3.** The old row stores `body_blake3 = blake3(plaintext)` (doc 01 C10), the new write path recomputes `blake3(plaintext)` over the re-decrypted-in-flight stream, and both must equal the value the *new* row stores. Three layers, cheapest-first:

### 2.1 Per-object-version verification (L0/L1/L2)

- **L0 — metadata equality (100% of units, cheap).** Assert new==old for: plaintext `size_bytes`, ETag (single MD5 and the composite `md5-N` for MPU — recompute `MD5(concat(fromhex(part_md5)))+"-N"`, doc 01 C12), `part_count` and every per-part `etag`, `content_type`, `user_metadata`, `is_delete_marker`, object-lock triple, tag set, ACL grant set, `version_id`. Any mismatch → `failed`.
- **L1 — plaintext hash match (100% of content units).** Compare the newly recomputed `blake3(plaintext)` to the old `body_blake3`. Where the old `body_blake3` is **NULL** (older rows predating the column, doc 02 §2.4), fall back to the old ETag: for single-part, `MD5(plaintext)` must equal `md5_hash`; for multipart, per-part MD5s must equal `parts.etag` and compose to the old composite ETag. If neither anchor exists, escalate to L2. ⚑ OQ-5.
- **L2 — end-to-end byte diff (sample + all locked).** Independently **read the migrated object back through the new service's GET path** (decrypt with the new envelope) and byte-compare to a fresh read through the old service's GET path. This exercises the *new decrypt path*, not just the writer's in-memory hash, and catches a class of bug L1 cannot (a writer that hashes correctly but stores an unreadable envelope — e.g. a key-commitment or AAD-binding mistake, doc `crypto-dedup-research.md` §Q3). Run L2 on a **statistical sample per bucket** plus **100% of object-lock/legal-hold/WORM objects** and 100% of the pilot bucket.

### 2.2 Per-bucket & per-account counts

Reconcile before a bucket may leave `RECONCILING`:

- **Counts:** live object count, total version count, delete-marker count, alias count — new == old (over the migrated scope, §1.2).
- **Bytes:** Σ plaintext `size_bytes` of the serveable set — new == old (this is the number that must also match `bucket_usage`, doc 12 §2.14, and downstream billing).
- **Config:** versioning status, object-lock default, policy/CORS/lifecycle/tags, bucket ACL — equal.
- **Account:** Σ over the account's live buckets of counts and plaintext bytes — new == old. This is the number `hcfs-chain-reporter` will eventually report for the tenant (doc 13 §4), so it is also the **billing reconciliation** number. See §4.5 for the dual-run double-count hazard.

### 2.3 Metadata that is easy to forget (all verified above, called out because "bytes match" is not enough)

Versions, delete markers, multipart composite ETags, object-lock (mode/retain-until/legal-hold), tags, ACLs (bucket+object), user metadata, content-type, same-bucket copy aliases, and the exact version-ids. A migration that copies bytes but drops a legal hold or an ACL grant is a **compliance/security regression**, not a cosmetic one. Object-lock and ACL equality are **hard gates**, not samples.

---

## 3. Ordering & dual-run

### 3.1 Unit of migration: per-bucket (grouped by account)

A **bucket** is the blast-radius and cutover unit — it matches S3 semantics (versioning, object-lock, policy, ACL are bucket-scoped) and the drain-direct "per-bucket/per-account gating" primitive (doc 09 §5.1, §5.2 Phase 3). Accounts are the scheduling/reconciliation grouping and the dedup scope (§1.4). Order buckets: **pilot bucket first** (doc 14 Phase 4 gate), then low-risk/small, then large, with **object-lock/WORM buckets handled last and most carefully** (§4.4).

### 3.2 In-progress multipart uploads

**Default: freeze, don't drain.** An incomplete `multipart_uploads` (parts uploaded, not completed) is client-resumable state. Migrating half-uploaded MPUs faithfully (with the provisional-DEK re-wrap dance, doc 12 §2.13) is high-effort for transient data. Policy: at a bucket's freeze (§3.3), **new MPUs are blocked** and in-flight ones are allowed to complete on the **old** service during a short drain, or are **abandoned** (the client re-initiates against the new service post-cutover — standard S3 behavior for an aborted upload). Do not attempt to move mid-flight MPU staging across the crypto boundary. ⚑ OQ-4.

### 3.3 How live writes during the window are handled — snapshot → catch-up → short freeze

The old service keeps **serving reads and accepting writes** for a bucket until that bucket cuts over. We do **not** dual-write (that would require changing the old Python writer and risks split-brain across two crypto schemes). Instead, three passes bound the downtime to a small tail:

1. **Backfill pass (bucket live).** Migrate the full snapshot as of enumeration. Long-running, parallel, throttled. Old serves normally throughout.
2. **Catch-up pass(es) (bucket live).** Re-enumerate the bucket for units whose old `last_modified` / `append_version` / `md5_hash` moved past the recorded `src_*` fingerprint, or that are new since the snapshot (the **source-change detection** primitive the Python migrator already uses to mark a changed object `failed`-and-retry; `../object-migration.md` step 5, doc 09 §5.5). Migrate the delta. Repeat until the delta is small.
3. **Freeze + final delta + cut (bucket briefly read-only for writes).** Put the bucket into **write-freeze on the old service** (reject/park PUT/DELETE/MPU for that bucket only — reads still served by old). Migrate the last delta, run reconciliation (§2), and only on green flip traffic (§4). Freeze duration ≈ the last delta + reconcile, typically seconds-to-minutes for all but the hottest buckets.

> **⚠ PREREQUISITE — a per-bucket write-freeze does NOT exist on the old Python side (verified 2026-09-15).** The Python gate has only (a) **global** `HIPPIUS_READ_ONLY_MODE` (fleet-wide — too coarse) and (b) **per-account** suspension `read_only` (`account_suspensions`, doc 05 §4a — freezes *all* of an owner's buckets, not one). So the per-bucket freeze this step assumes must be provided by one of: **(1)** a small old-side change — a `buckets.write_frozen` flag (or a Redis frozen-bucket set) checked in the write path; **(2)** migrate at **account granularity** using the existing per-account `read_only` suspension (freeze one account's buckets together in a window — zero old-side code, longer per-account freeze); or **(3)** the OQ-6 old-side dual-write fallback. **Decide this before Phase 4** — option (2) is the zero-old-side-change default if per-account freeze windows are acceptable.

**Read-after-write during the window:** because reads and writes both stay on the **old** service until the atomic per-bucket flip, read-after-write is exactly the old service's guarantee throughout — there is no split-brain window where a client could read new and write old or vice-versa. The freeze closes the seam: no write is accepted on old after the final delta is captured, so nothing is lost or stranded.

> ⚑ If a per-bucket write-freeze is operationally unacceptable for a 24/7 tenant, the fallback is **old-side dual-write** (Python writer also forwards each PUT to the new service during catch-up) — but that requires old-service changes and careful idempotency, and it is explicitly the heavier option. Recommended only if a specific bucket cannot tolerate a short freeze. OQ-6.

---

## 4. Cutover interplay (feeds doc 14 Phase 5) & rollback

Phase 4's exit gate is "a pilot bucket migrates with plaintext-identical verification and serves from the new service" (doc 14). Phase 5 then generalizes it with the **drain-direct playbook primitives** (doc 09 §5.1): *order over flags, idempotent overlap, read-only verification backstop, per-bucket blast-radius, reads-before-writes, staging-gated-before-prod, keep the old side deployable as the rollback target.*

### 4.1 Shadow (Phase 5 step 1; doc 09 §5.2 Phase 0)

Stand the new service up reading **migrated-and-verified** buckets with **no traffic selector** (or a shadow route). Mirror read traffic (or run the smoke/conformance suite continuously) and **diff new vs old on GET/HEAD/LIST** for plaintext bytes, ETag, and headers. This is L2 (§2.1) at traffic scale and is the analog of the drain-direct read-only backstop. Reads first — a read bug is recoverable; the migration itself already guaranteed the write side.

### 4.2 Per-bucket traffic shift (Phase 5 step 2)

Cut over **one bucket at a time**, after that bucket reaches `VERIFIED`, by repointing its request routing (gateway/DNS/Service selector, or an `HIPPIUS_RUST_BUCKET_ALLOWLIST` config gate — the one place doc 09 §5.2 Phase 3 sanctions a flag) from old → new. Order matters (drain-direct lesson): the bucket is **write-frozen on old first** (§3.3 step 3), then **enabled for writes on new**, so no window lets both accept writes for the same bucket. Constants that both sides stamp (backend set, KEK/KMS ids) are pinned identically (doc 09 §5.1 point 4).

### 4.3 Decommission (Phase 5 step 3)

After a soak window per bucket (smoke green, alerts clean — doc 09 §5.3), and only after the new copy is **verified durable** in HCFS (`blobs.replication_state='durable'`): (1) drop the old `object_versions`/`parts`/`chunk_backend` rows for the bucket, and (2) issue HCFS `DELETE` for the **old** ciphertext blobs (the old backend_identifiers) — reclaiming the doubled storage (§4.4). Old service scales to zero **last**, kept deployable 24–48h as the rollback target (doc 09 §5.2 Phase 4). Old-blob deletion is the analog of the Python migrator's "cleanup is the sole unpin authority, with an age guard" (`../object-migration.md`); do it as a separate aged job, never inline with the cut.

### 4.4 Object-lock / WORM buckets

COMPLIANCE-mode retention and legal holds mean the **old bytes may not be deletable** until retention expires. The migration only *reads* old (never deletes during retention — safe), and writes a new copy carrying the **same** lock (verified 100%, §2.1 L2). So decommission of old WORM bytes waits until either (a) retention expires, or (b) legal/compliance signs off that the verified new copy satisfies the WORM obligation. WORM buckets also use **sync-to-HCFS-before-ack** on the new side (doc 12 §7, doc 14 Phase 3), so their migrated blobs are durable before the version is committed. Migrate WORM buckets **last**, verify at 100%, decommission slowest.

### 4.5 Rollback

Rollback is **repoint the bucket back to old** — instant, no data loss, because the migration never mutated old (§0.1). This holds cleanly **only within the rollback window**: once the new service has accepted new-only writes for a bucket (post-cut), rolling that bucket back to old would strand those writes. So:

- **Before/at cut:** rollback = flip routing back to old, re-open old writes. Zero data loss.
- **During soak (new-only writes accumulating):** rollback requires **reverse-replay** of the new-only writes back onto old (or accepting their loss). Keep the soak short and monitored; treat the end of soak as the **forward-commit point**. Flag this boundary explicitly to operators — it is the one place the "old is a pristine replica" property lapses. OQ-8.
- **Gating discipline:** staging first, with a gating test (a pilot bucket full-cycle: backfill→freeze→verify→cut→read-parity); if it fails, STOP — do not proceed to prod (doc 09 §5.4).

### 4.6 Billing / usage during the window (double-count hazard)

The new service inherits per-tenant usage→chain reporting via HCFS (doc 13 §4). During the window a bucket's bytes exist in **both** old and new HCFS objects under the **same tenant ss58** → naive usage accounting would **double-report** to chain (doc 13 OQ-3, §4). Mitigations: (a) do **not** let the new service report a bucket's usage until that bucket has cut over and its old bytes are scheduled for decommission; or (b) attribute migration-written blobs to a non-reporting/exempt path until cutover (but note exempt ⇒ unreported *and* ungated, doc 13 §4/§5). Pick one and reconcile the account total (§2.2) against the on-chain number post-cut. OQ-10.

---

## 5. The old-format decrypt dependency (confirming doc 14)

**Confirmed: `s3-crypto` MUST carry the OLD decrypt path from Phase 0.** Doc 14 already states this ("Must be in `s3-crypto` from the start: the old-format decrypt path — Phase 4 depends on it, and designing it in late is costly"). This doc pins *why* and *exactly what*:

- The migrator's read side (§1.3) is a **byte-exact reimplementation of doc 01's read path** — the frozen contracts C1–C15: chunk framing `nonce(12)‖ct‖tag(16)`, the V2 chunk AAD (`bucket_id‖object_id‖part‖chunk`, per-part, LE-length-prefixed — *not* the stale V1 5-tuple), the DEK-wrap AAD `hippius-dek:{bucket}:{object}:{version}`, the DEK/KEK sizes, the local-KEK wrap-key derivation, and the OVH KMS datakey/decrypt mTLS protocol. Any drift and old objects become permanently undecryptable (doc 01 preamble).
- It is a **read-only, migration-only** surface: the live new service never encrypts in the old format, but it must **decrypt** it for the entire backfill. It also needs **OVH KMS access with the old mTLS certs** (for KMS-wrapped KEKs) and a reader for the **old keystore DB**. These are inputs the new deployment must be granted for the duration of Phase 4/5, then revoked.
- Keep the old-decrypt code behind a clear module boundary (`s3-crypto::legacy_v5`) with the doc-01 fixtures as its test oracle, so it can be **deleted after decommission** without touching the live crypto.
- It does **not** need the old *write* path, the deprecated V1 adapter, or the NaCl legacy suite (doc 01 §1.1 legacy note) — **unless** OQ-1 (v1/v2 objects) resolves to "carry them."

---

## Per-bucket migration state machine

```mermaid
stateDiagram-v2
    [*] --> DISCOVERED : enumerate bucket + config
    DISCOVERED --> BACKFILLING : worklist materialized (mig.object_versions)
    BACKFILLING --> BACKFILLED : all snapshot units written (state=committed on new)
    BACKFILLED --> CATCHING_UP : re-enumerate delta (last_modified/append_version/md5 moved)
    CATCHING_UP --> CATCHING_UP : delta still large → repeat (bucket LIVE on old)
    CATCHING_UP --> FROZEN : delta small → write-freeze bucket on OLD
    FROZEN --> RECONCILING : final delta migrated
    RECONCILING --> VERIFIED : counts + bytes + config + L0/L1 all match; L2 sample + 100% locked pass
    RECONCILING --> FAILED : any mismatch
    VERIFIED --> CUTOVER : repoint routing old→new; enable writes on new
    CUTOVER --> SOAKING : new serves reads+writes; old still deployable
    SOAKING --> DECOMMISSIONED : soak green → drop old rows + GC old HCFS blobs (age-gated; WORM waits)
    DECOMMISSIONED --> [*]

    FAILED --> BACKFILLING : fix + re-drive (units keyed by old id; --resume)
    CUTOVER --> ROLLED_BACK : pre-soak repoint back to old (zero loss)
    SOAKING --> ROLLED_BACK : reverse-replay new-only writes (bounded window) OR accept loss
    ROLLED_BACK --> BACKFILLING
    FROZEN --> CATCHING_UP : abort freeze → re-open old writes (rollback of the freeze)
```

Per-**unit** sub-states (within `BACKFILLING`/`CATCHING_UP`): `pending → claimed → writing → written → verified` (or `failed` / `skipped` / `superseded`), exactly the `mig.object_versions.status` domain (§1.5).

---

## Reconciliation checklist (quick reference)

| Scope | Check | Gate |
|---|---|---|
| version | plaintext `size_bytes` new==old | 100% (L0) |
| version | ETag single MD5 + composite `md5-N` | 100% (L0) |
| version | per-part count + per-part ETag | 100% (L0) |
| version | content-type, user metadata | 100% (L0) |
| version | `blake3(plaintext)` new == old `body_blake3` (fallback: ETag) | 100% (L1) |
| version | GET-back byte diff new vs old | sample + 100% of locked + pilot (L2) |
| version | object-lock (mode, retain_until, legal_hold) | 100% (hard gate) |
| version | delete-marker flag, version_id | 100% |
| object | tags set, object ACL grants | 100% (hard gate for ACL) |
| object | same-bucket copy aliases → metadata-copies | count |
| bucket | object/version/marker/alias counts | equal |
| bucket | Σ plaintext bytes (serveable set) | equal |
| bucket | versioning, lock-default, policy, CORS, lifecycle, bucket ACL, tags | equal |
| account | Σ counts + Σ plaintext bytes over live buckets | equal (== billing number) |

---

## Open questions (⚑ = needs a decision or a runtime probe before Phase 4)

- **OQ-1 ✅ RESOLVED (prod count).** No v1/v2/v3 exist at all — only v4 (212,498) + v5. All v4 GETs error today (read path rejects <5), so pre-v5 is **skip-and-report**; `s3-crypto::legacy` is **v5-only**. (register E1; doc 01 C15.)
- **OQ-2 ✅ RESOLVED.** The v5 copy fast-path was **never enabled** (born disabled — `should_use_v5_fast_path` returns False before any eligibility logic), so no objects have the destination-object_id AAD bug → the undecryptable-copy class is **empty**. Keep the pilot decrypt probe as a safety net; genuine bit-rot → quarantine + alert, never silent drop. (register E2; python-side-findings #11.)
- **OQ-3 ✅ RESOLVED.** **Per-blob DEK** (doc 12 §2.9, wrapped under the bucket KEK); the per-object-version `wrapped_dek` was reconciled away. (register A2.)
- **OQ-4 ✅ In-progress MPU policy.** **Freeze-and-abandon** (register E3) — new MPUs blocked at freeze, in-flight ones abandoned (standard S3 abort); do not migrate staging across the crypto boundary. (§3.2.)
- **OQ-5 ⚑ NULL `body_blake3` rows.** Confirm the ETag fallback (MD5) is sufficient as the L1 anchor, or force L2 for those units. (doc 02 §2.4.)
- **OQ-6 ⚑ Per-bucket write-freeze acceptability.** Is a short freeze OK for all buckets, or does a 24/7 tenant need old-side dual-write? (§3.3.)
- **OQ-7 ⚑ HCFS storage doubling.** Confirm HCFS capacity headroom for old+new coexistence per migrating bucket, and the aged old-blob GC cadence post-cut. (§4.3; doc 13 §6.)
- **OQ-8 ⚑ Rollback window / forward-commit point.** Define the soak length and the operator-visible boundary past which rollback means reverse-replay, not repoint. (§4.5.)
- **OQ-9 ⚑ Version-history scope.** Migrate the full version chain + all delete markers, or current-serveable-only + collapse history? Product/compliance call. (§1.2.)
- **OQ-10 ✅ Billing double-count.** Resolved to option (a): **don't report a bucket's usage from the new service until it cuts over** (cleaner than the exempt path, which also disables the quota gate). (register E4; §4.6.)
- **OQ-11 Dedup map scale.** `mig.plaintext_blobs` grows with distinct plaintext chunks per account; confirm its size/throughput is acceptable and decide its retention after decommission (it is the record of which blobs are shared). (§1.4.)
- **OQ-12 ✅ Key-committing AEAD — construction FROZEN (doc 25).** The suite is `hip-enc/aes256gcm-ctx-frames-v1`; the only remaining crypto gate is the Phase-0 **code review** of the wrapper (`ctx-poc/` is the reference), not a spec ratification. Migration writes the frozen format.
```
