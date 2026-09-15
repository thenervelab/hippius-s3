# 18 — Object Lock / Retention / Legal Hold enforcement, and the S4 append decision

Status: **DRAFT SPEC** for the greenfield Rust reimplementation of hippius-s3. Testable. Normative.

This document is the enforcement contract for WORM (write-once-read-many) in the Rust rewrite:
retention modes, legal hold, bucket default retention, the versioning prerequisite, the exact
allowed/denied matrix for every mutating operation, the durability tie-in for object-lock buckets,
and a scope decision for the proprietary **S4 append** verb.

It reconciles four internal sources with AWS canonical semantics:

- [`06-s3-protocol-conformance.md`](./06-s3-protocol-conformance.md) — the Python conformance matrix
  (§5 append, §6 object lock, §7 errors), including the live WORM hole (§5.4, gap G9).
- [`10-write-path-decision.md`](./10-write-path-decision.md) — the SSD-staging decision and its
  **per-bucket durability policy** (default = fast-ack on SSD; WORM = sync-to-HCFS-before-ack).
- [`../../object-lock.md`](../../object-lock.md) — the Object Lock design (the `protected()`
  predicate, weakening rules, the enforcement-point inventory E1–E9, the COMPLIANCE/`terminated_at`
  decision, S4-vs-WORM §5.2, and the full test plan §8).
- [`../../specs/s3-object-lock.md`](../../specs/s3-object-lock.md) and
  [`../../specs/s3-object-lock-tier2-handoff.md`](../../specs/s3-object-lock-tier2-handoff.md) — the
  tiered spec and the Tier-2 enforcement handoff (two-layer guard, the delete-shape split, the
  data-layer boundary).
- [`../s4.md`](../s4.md) — the S4 append wire contract (O(delta), `append-if-version` CAS,
  `append-version` counter, idempotency).

AWS semantics are cited inline as **[AWS]** and listed in Sources; hippius-specific decisions are
cited to the internal doc that made them.

The whole feature reduces to one sentence (from `object-lock.md` §2): **a lock is a property of a
version; a protected version's bytes and its lock metadata cannot be destroyed or weakened.** The
Python product violates this in exactly one place — **S4 append mutates a version in place without
consulting the lock** (`06` §5.4 / `object-lock.md` §5.2, gap G9). §4 closes that by construction.

---

## 0. Canonical predicate and vocabulary

### 0.1 The predicate (the entire feature)

```
protected(version) := account.terminated_at IS NULL
                      AND ( object_lock_legal_hold == ON
                            OR ( object_lock_retain_until IS NOT NULL
                                 AND now() < object_lock_retain_until ) )
```

- Retention and legal hold are **independent**; either one locks the version. A version with an
  **expired retention and an active legal hold is still protected**; a version whose legal hold was
  removed but whose retention is still in the future is still protected. [AWS] (`object-lock.md`
  §2.1, P5–P8).
- **Boundary:** `retain_until` exactly equal to `now()` is **not protected** (strict `<`). Pin this
  (`object-lock.md` test P4).
- **Mode does not affect *whether* a version is protected** — only *who* may weaken/delete it
  (`object-lock.md` P9). Mode is consulted only by the weakening/delete rules in §1.4.
- **`terminated_at` is the single escape hatch**, folded into the predicate itself. No destructive
  call site anywhere takes a `force`/`bypass_locks` parameter; account termination works by making
  the predicate false for every version the account owns, once, in one audited place
  (`object-lock.md` §5.3.1). This is a hard design rule for the rewrite — see §3.4.

The same predicate lives in Python and in SQL and is the join point for every rule below
(`s3-object-lock-tier2-handoff.md` §4: `is_version_locked`).

### 0.2 Principals and the bypass model (deliberate deviation from AWS)

We have no IAM. The mapping (`object-lock.md` §5.4, `s3-object-lock-tier2-handoff.md` §6, `06` §6.1):

| Term used below | Meaning in hippius-s3 |
|---|---|
| `any-caller` | any authenticated principal with normal write access (incl. delegated `WRITE_ACP` grantees and sub-tokens). |
| `bypass` | a **master token whose account is the bucket owner**, sending `x-amz-bypass-governance-retention: true`. **Both** are required; the header alone, or a sub-token / delegated grantee with the header, does **not** bypass. This stands in for AWS's `s3:BypassGovernanceRetention` [AWS]. `request_is_bucket_owner` compares `account.main_account` vs `bucket_owner_id` (NOT `main_account_id`) — `06` §6.1. |
| `termination` | setting `users.terminated_at` — the only thing that lifts COMPLIANCE / legal hold, and it lifts *everything the account owns* at once (`object-lock.md` §5.3). |

**`s3:BypassGovernanceRetention` is bucket-owner-master-token only, by deliberate design** — a
delegated `WRITE_ACP` grantee must never bypass, or "may upload" silently becomes "may make
undeletable" (`s3-object-lock-tier2-handoff.md` §6). The `?retention` / `?legal-hold` subresources
grade `READ_ACP` / `WRITE_ACP`, **not** `WRITE`; do not regrade them.

### 0.3 Version identity

Version IDs are **decimal integers**; literal `null` / empty means "current version" (`06` §8.5). A
`?versionId` on `?acl` or `?tagging` is `501` (`06` §0.3). Locks attach to versions, never keys
(`object-lock.md` §2). Distinct from the S4 **`append-version`** counter (§4), which is a separate
per-object integer surfaced as `x-amz-meta-append-version`.

---

## 1. Enforcement matrix (the testable core)

Legend: **✓** allowed; **403** `AccessDenied`; **✓ (new version)** = allowed *because it does not
touch the protected version* — it creates a new current version and leaves the protected one intact
underneath. "Target" = the specific version the op would destroy/mutate/weaken.

### 1.1 The mental model (three rules that generate the whole table)

1. **Anything that creates a new version is always allowed** — `PutObject` overwrite,
   `CompleteMultipartUpload`, `CopyObject` to a key, a simple (versionId-less) `DELETE` (which stacks
   a delete marker). The protected version survives underneath. "Overwrite protection means the old
   version survives, not that writes fail." [AWS] (`object-lock.md` §2.3; `s3-object-lock-tier2-handoff.md`
   §1; `06` §6.2).
2. **Anything that names and destroys/weakens a specific protected version is the guarded surface** —
   permanent `DELETE ?versionId`, retention shortening/removal/downgrade, and (hippius-specific) any
   in-place mutation of that version (S4 append). COMPLIANCE = 403-for-everyone; GOVERNANCE =
   403-unless-`bypass`; legal hold = 403-until-removed.
3. **The destructive background workers are the real boundary** — the unpin path, the hard-delete
   ring, and the ops scripts destroy bytes *without going through the API*, so the predicate must
   also gate them in SQL (`s3-object-lock-tier2-handoff.md` §2, §4). The FS/Ceph cache janitor is the
   **exception**: eviction is not deletion, so it must **not** be gated (§2c below; `object-lock.md`
   §5.1 E8).

### 1.2 API-surface matrix

| Mutating op | Target | NONE | GOVERNANCE (active) | COMPLIANCE (active) | LEGAL_HOLD | Who may bypass | Test |
|---|---|---|---|---|---|---|---|
| **PutObject overwrite** (same key, no versionId) | current | ✓ (new version) | ✓ (new version) | ✓ (new version) | ✓ (new version) | n/a — never touches old version | `object-lock.md` D13 |
| **DeleteObject, simple** (no versionId) | current | ✓ | ✓ (delete marker) | ✓ (delete marker) | ✓ (delete marker) | n/a — nothing destroyed | D8; `tier2-handoff` §5 #2 |
| **DeleteObject, version-scoped** (`?versionId`) | that version | ✓ | 403 unless `bypass` | **403 (nobody, incl. root)** | 403 until hold removed | GOV→`bypass`; COMPLIANCE→none; hold→none | D1–D7; `06` §6.2 |
| **Delete of a delete-marker** (`?versionId` = marker) | the marker | ✓ | ✓ | ✓ | ✓ | n/a — **markers are never protected** | D9; `object-lock.md` §2.3 |
| **DeleteObjects batch, version-scoped** | each named version | ✓ per key | 403 per key unless `bypass` | 403 per key | 403 per key | GOV→`bypass` (whole batch) | D10; `tier2-handoff` §5 #3 |
| **Whole-object (unversioned) DELETE path** (E3) | *all* versions | ✓ | **403 if ANY version protected** | **403 if ANY protected** | **403 if ANY protected** | n/a — refuse; **no unpin enqueued** | D11; `object-lock.md` §5.1 E3 |
| **CompleteMultipartUpload** (same key) | current | ✓ (new version) | ✓ (new version) | ✓ (new version) | ✓ (new version) | n/a — completes into a new version | R6 |
| **CopyObject → locked key** (dest currently protected) | dest current | ✓ (new version) | ✓ (new version) | ✓ (new version) | ✓ (new version) | n/a — dest gets a new version; **source lock not inherited** | R7 |
| **S4 append** (`x-amz-meta-append: true`) | current (in place) | ✓ | **403 if current protected** | **403 if current protected** | **403 if current protected** | n/a — no bypass; §4 | A1–A6; `object-lock.md` §5.2 |
| **Lifecycle expiration** | selected version | ✓ | (no lifecycle engine today — §1.5) | (—) | (—) | lifecycle = system principal, **no bypass** | `object-lock.md` §2.3 |
| **PutObjectRetention — EXTEND** (later date) | that version | ✓ | ✓ | ✓ | ✓ | any put-retention caller | W1, W6 |
| **PutObjectRetention — SHORTEN / remove / downgrade** | that version | ✓ | 403 unless `bypass` | **403 (nobody)** | independent of hold | GOV→`bypass`; COMPLIANCE→none | W2–W9 |
| **PutObjectLegalHold — turn OFF** | that version | ✓ | ✓ | ✓ | ✓ | put-legal-hold caller; **not** gated by mode, **not** by `bypass` | W11 |

Notes bound into the table:

- **Version-scoped delete of a protected version → `403 AccessDenied` *before* any soft-delete or
  unpin enqueue** (`tier2-handoff` §5 #1). The enqueue is the destructive act; the test asserts on
  the queue, not just the status (`tier2-handoff` §8 test 2; `object-lock.md` D11).
- **A GOVERNANCE bypass delete clears the retention after an authorised delete** (`06` §6.2).
- **Multi-alias version delete** (an object published under >1 name via same-bucket CopyObject) →
  `501 NotImplemented`, not a lock decision (`06` §6.2, §9.3). Flag this interaction (⚑ Q4).
- **CopyObject rejects any `If-None-Match` with `501`** (`06` §2.1, §9.2) — orthogonal to lock but do
  not accidentally make copy-to-locked-key depend on it.
- **`Get*`/`Head`/`ListObjectVersions` on a locked version are always allowed** — lock is about
  destruction, not access (`object-lock.md` §2.3). `Get*Retention/LegalHold` on a delete marker →
  `405` (`object-lock.md` T7).

### 1.3 Background-worker / ops matrix (the boundary that actually protects bytes)

| Destroyer | Reaches | Behaviour on a protected version | Layer | Test |
|---|---|---|---|---|
| **Unpin path** (`enqueue_object_unpin` → unpinner) | Arion + OVH backup (real bytes) | Never enqueue; the `object_version = NULL` ("all versions") form must resolve and **skip locked ones**; a locked version reaching the unpinner is a bug — log loudly | data-layer SQL + re-check | E5; `object-lock.md` B3; `tier2-handoff` §2a, §4, §5 #4 |
| **Hard-delete ring** (`find_objects_ready_for_hard_delete` → `hard_delete_object`) | DB rows | Exclude locked versions (`AND NOT <locked predicate>`) | data-layer SQL | E6/E7; B1/B4; `tier2-handoff` §5 #5 |
| **FS / Ceph cache janitor** | a cache copy only | **No lock check — leave it alone.** Eviction ≠ deletion; gating it pins locked objects in NVMe and fills the cache (self-inflicted outage, zero durability benefit) | none — explicit non-goal | E8; B6; `tier2-handoff` §2c |
| **Admin purge** (`nuke_user.py`, `purge_buckets.py`, `purge_source_versions.py`, PR #422 suspension) | everything | Purge **refuses** COMPLIANCE-locked versions; **suspension leaves locked data pinned and untouched**; the only removal is `terminated_at` | predicate (no bypass param) | E9; B7/B8/B11; `object-lock.md` §5.3 |
| **Account termination** (`terminated_at` set) | everything the account owns | Predicate becomes false → locked versions become reapable; account-scoped (does not touch other accounts) | predicate | B9/B10; `object-lock.md` §5.3.1 |

**Layered defence, in order** (`object-lock.md` §5.1): refuse at the API (E1–E4) → gate the queue
producer (E5) → keep the predicate in the reap SQL (E6/E7). A bug in the first two layers still
cannot destroy bytes because the SQL boundary holds. Treat the **data layer as the real boundary**;
the API layer exists for correct error messages (`tier2-handoff` §4).

### 1.4 Weakening rules (who may change a lock)

Strengthening is always allowed; weakening depends on mode (`object-lock.md` §2.4):

| Change | GOVERNANCE | COMPLIANCE |
|---|---|---|
| Extend `retain_until` | ✓ | ✓ |
| Shorten / remove `retain_until` | needs `bypass` | **never** |
| GOVERNANCE → COMPLIANCE | ✓ (strengthen) | — |
| COMPLIANCE → GOVERNANCE | — | **never** |
| Delete the version | needs `bypass` | **never** |
| Legal hold ON → OFF | ✓ always | ✓ always |

Two consequences to internalise: **legal hold is never bypassed** (bypassing governance does not
touch it — `object-lock.md` §2.4); **COMPLIANCE has no escape hatch but `terminated_at`** [AWS: "the
only way to delete… is to delete the associated AWS account"] (`object-lock.md` §2.4, §5.3).

### 1.5 Lifecycle expiration

`PutBucketLifecycle` is **parse-and-discard repo-wide** and there is no lifecycle enforcement engine
(`06` §1.1, §9.2). So today the lifecycle row is inert. **Decision for the rewrite:** if/when a
lifecycle engine is added, it runs as a system principal **with no bypass** and MUST respect the
`protected()` predicate — a retention date wins over an expiration rule, matching AWS
(`object-lock.md` §2.3; `tier2-handoff` §10). Kept out of v1 (`tier2-handoff` §7). ⚑ Q5.

### 1.6 Test-ID crosswalk

The matrix above is already backed by the design's test plan — reuse those IDs verbatim rather than
inventing new ones: predicate P1–P9, weakening W1–W16, delete enforcement D1–D13, workers B1–B12,
append A1–A6, config/prereq C1–C11, default retention R1–R7, round-trip/read T1–T10, concurrency
X1–X4, E2E E1–E6 (`object-lock.md` §8), plus the two that "actually prove it": end-to-end durability
(delete → run unpinner + hard-delete → bytes still fetchable from Arion) and cache-eviction-still-works
(`tier2-handoff` §8 tests 7–8). **Per the repo style guide these assert on HTTP status / queue state /
byte-fetchability — never on a `debug_assert!`** (`debug_assert!` is dead in `--release`).

---

## 2. Modes & fields — exact semantics

### 2.1 GOVERNANCE vs COMPLIANCE

| Property | GOVERNANCE | COMPLIANCE |
|---|---|---|
| Delete protected version | 403 unless `bypass` (bucket-owner master token + header) | **403 for everyone incl. root**; lifts only at `retain_until`, or via `terminated_at` [AWS] |
| Shorten `retain_until` | 403 unless `bypass` | **never** (extend only) [AWS] |
| Change mode | GOV→COMPLIANCE ✓; lift with `bypass` | **cannot change** while locked [AWS] |
| Extend `retain_until` | ✓ (any put-retention caller) | ✓ |
| Intended use | accidental-deletion protection with an admin escape hatch | regulatory WORM; no escape but account termination |

### 2.2 Fields (per version, on `object_versions`)

Schema (`object-lock.md` §4.3, `tier2-handoff` §3) — nullable/defaulted, metadata-only migration, no
table rewrite; the enforcement partial index is built `CONCURRENTLY` in a `transaction:false`
migration:

```sql
ALTER TABLE object_versions
  ADD COLUMN IF NOT EXISTS object_lock_mode text NULL,            -- 'GOVERNANCE' | 'COMPLIANCE' | NULL
  ADD COLUMN IF NOT EXISTS object_lock_retain_until timestamptz NULL,
  ADD COLUMN IF NOT EXISTS object_lock_legal_hold boolean NOT NULL DEFAULT false;

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_object_versions_locked
  ON object_versions (object_id, object_version)
  WHERE object_lock_retain_until IS NOT NULL OR object_lock_legal_hold;

ALTER TABLE users ADD COLUMN IF NOT EXISTS terminated_at timestamptz NULL;   -- §0.1 escape hatch
```

> **Greenfield column mapping (doc 12 is the schema authority).** The `ALTER` above is the *Python
> in-place migration* shape. In the greenfield Rust schema these are defined fresh on
> `object_versions` as **`lock_mode` / `lock_retain_until` / `legal_hold`** (doc 12 §2.6, §2.12) and
> the escape hatch is **`accounts.terminated_at`** (doc 12 §2.1), not `users.terminated_at`. The
> `protected()` predicate (§0.1) reads those columns; names differ, semantics are identical.

- **`retain_until`** is a UTC timestamp; protection is active while `now() < retain_until` (§0.1).
- **Extend** replaces the stored date with a later one (both modes); **shorten** = earlier date
  (GOVERNANCE needs `bypass`; COMPLIANCE forbidden). [AWS] (`object-lock.md` §2.4).
- **`retain_until` in the past on a fresh PUT → `400 InvalidArgument`** (`object-lock.md` W16).
- **`mode` without a date, or a date without `mode`, → `400`** — the pair is atomic
  (`object-lock.md` T10).
- **Retention cap**: `HIPPIUS_OBJECT_LOCK_MAX_RETENTION_DAYS` rejects a `retain_until` beyond the cap
  with `400 InvalidArgument` (`06` §6.1; `object-lock.md` §5.3.2, W15). Given the COMPLIANCE cost
  commitment (§3.4), this cap is **load-bearing, not a nicety** — it is the only bound on how much
  unreclaimable storage one account can create. **RESOLVED (register D2):** absolute max **36,500
  days**; default cap **2555 days (~7 years)** — covers the regulatory cases (SEC 17a-4 is 6y)
  without a century-long pin. A **"year" = 365 days** (not calendar).

### 2.3 Legal hold

Boolean per version, **independent of retention, no date** (`object-lock.md` §2.1). Placed/removed by
any caller with put-legal-hold (`WRITE_ACP`) permission; **not gated by mode and `bypass` does not
apply** — the only way past a legal hold is to remove it (`object-lock.md` §2.4, W11). Composes with
retention: deletable only when **both** are clear (and the account is not terminated).

### 2.4 Bucket default retention

- Optional bucket property `{ mode, Days XOR Years }` stored in `buckets.object_lock` JSONB (`06`
  §6.2; `spec` Tier 1). Validation: `Mode ∈ {GOVERNANCE, COMPLIANCE}`, exactly one of `Days`/`Years`,
  positive; else `400 MalformedXML` (`spec` Tier 1; `object-lock.md` C6).
- **Materialised at write time**, not computed by a lazy join: on a write carrying no explicit lock
  headers, stamp `retain_until = version_creation_time + duration` and the mode onto the new version.
  Two reasons (`object-lock.md` §4.3): it matches AWS (the rule in force at PUT time is the one that
  sticks) and it turns every enforcement check into a single-row predicate.
- **Explicit `x-amz-object-lock-*` headers override the bucket default** for that version. [AWS]
  (`06` §6.2; `object-lock.md` R3).
- **Not retroactive** — applies only to versions created after the rule is set; existing versions keep
  their stamped dates (or none). [AWS] (`object-lock.md` R2/R4; `tier2-handoff` §7). ⚑ confirm Q6.
- Applied on `PutObject`, `CopyObject` destination (**source lock not inherited**, R7), and the
  version reserved by `CreateMultipartUpload` (`06` §6.2; `object-lock.md` §4.1, R6/R7).
- **`Years` convention: RESOLVED (register D2) — a year is 365 days** (not calendar), matching AWS.

### 2.5 Versioning prerequisite (hard requirement)

- **Object Lock requires versioning `Enabled`.** [AWS: "Object Lock works only in buckets that have S3
  Versioning enabled."] (`object-lock.md` §2.5; `tier2-handoff` §1).
- `PUT ?object-lock` on a bucket whose `versioning_status != Enabled` → `409 InvalidBucketState`
  (`06` §6.2; `object-lock.md` C1).
- `CreateBucket` with `x-amz-bucket-object-lock-enabled: true` **implies `versioning_status =
  Enabled`** (`06` §6.2; `object-lock.md` C2). (Wire header drops boto3's `-for-bucket` suffix —
  `spec` §"Request-side touchpoints".)
- Once lock is on, **versioning cannot be suspended and lock cannot be disabled**. A
  `PutBucketVersioning Suspended` on a lock-enabled bucket → `409 InvalidBucketState` — assert the
  specific `409`, distinct from the blanket `501` that generic `Suspended` returns today (`06` §1.1
  note; `object-lock.md` §2.5, C3, C11).
- **This is what makes E3 structurally impossible** rather than merely guarded: a simple `DELETE` on a
  lock-enabled-but-unversioned bucket (the Tier-1 hole) would enqueue one unpin covering *every*
  version — unrecoverable. Enforcing "lock ⇒ versioning Enabled" at config time removes that path
  (`object-lock.md` §5.1 E3). ⚑ this is the single most important prerequisite.

---

## 3. Durability tie-in — where enforcement sits

Per [`10-write-path-decision.md`](./10-write-path-decision.md) (DECIDED 2026-09-15, SSD-staging with a
**per-bucket durability policy**):

- **Default buckets:** fast-ack on the durable local-SSD write; a per-node forwarder drains to HCFS
  afterwards. A `200` means "on one node's SSD", not yet in HCFS/Arion — a durability window.
- **Object-lock / WORM buckets (audit/evidence, e.g. sn85):** **sync-to-HCFS-before-ack. No
  durability window; a `200` means durable in HCFS.**

Enforcement must be placed so that a locked version is **(a) never mutable**, **(b) never
acked-before-durable**, and **(c) never destroyed by a background worker**. Three independent
guarantees.

### 3.1 Why WORM buckets need sync-before-ack (the tie-in's whole reason)

`object-lock.md` §5.5 states the hazard plainly: **"a lock is not durability."** With default fast-ack,
a locked object whose SSD→HCFS drain later lands in `failed`/`corrupt` is a **single-copy locked
object** — WORM raises the stakes on exactly that window. The per-bucket policy in doc 10 closes it:
because a WORM-bucket PUT is not acked until HCFS has durably accepted the bytes, there is no window
in which a "locked" object exists on one node's disk only. **A WORM 200 = durable in HCFS AND the
version row (with materialised lock metadata) committed.** This is the direct dependency between §2
(lock metadata materialised at write time) and doc 10 (WORM = sync-before-ack): the two must ship
together, or a lock is a promise over possibly-non-durable bytes.

### 3.2 Placement: the predicate is a metadata gate at two layers, bracketing the byte path

Lock metadata (`object_lock_mode`, `object_lock_retain_until`, `object_lock_legal_hold`) lives on the
`object_versions` row; bytes live in HCFS content-addressed storage. The `protected()` predicate is
enforced at **two layers** (`tier2-handoff` §4):

```
WRITE (PutObject / MPU complete / Copy / S4 append) on a WORM bucket
  (1) authorize op against the TARGET version's lock state  ── violates §1 ? ─▶ 403 (no bytes, no marker, no enqueue)
  (2) materialise NEW-version lock metadata from explicit headers or bucket default (§2.4)
  (3) write bytes to HCFS, AWAIT durable            ◀── doc 10: sync-to-HCFS-before-ack
  (4) COMMIT the version row + lock metadata, RE-CHECK the target under the same
      optimistic-concurrency guard (a legal hold placed since step 1 must not be lost)
  (5) ACK 200 only after (3) durable AND (4) committed

DESTROY (version-scoped delete, unpin, hard-delete ring, ops script)
  API layer:  evaluate protected() → 403 AccessDenied for a good client error (delete surface)
  DATA layer: the SQL feeding the unpinner and hard-delete ring carries AND NOT <locked predicate>;
              the unpinner re-checks before issuing a backend delete and logs a locked hit as a bug
```

Rules that fall out:

- **R1 — Immutability by addressing.** A locked version's content-addressed bytes are never a write
  target. Every "change" to a locked key makes a *new* version at a *new* address (S4 append is the
  one in-place exception, handled in §4 by refusing when the current version is protected).
- **R2 — Enforce before side effects.** The 403 decision precedes any byte write, delete-marker
  insert, or unpin enqueue (`tier2-handoff` §5 #1).
- **R3 — Ack after durable (WORM buckets only).** No fire-and-forget storage push on the WORM ack
  path (contrast the default-bucket SSD fast-ack — doc 10). Test: WORM 200 arrives only after HCFS
  durable.
- **R4 — Re-check at commit (TOCTOU).** Lock metadata can change between step 1 and step 4 (a
  concurrent legal hold / retention extend). The commit re-authorizes under the same
  optimistic-concurrency guard the append path already uses (`FOR UPDATE` on the version row —
  `object-lock.md` X1–X4).
- **R5 — Lock-metadata writes are themselves durable-before-ack on WORM buckets.**
  `PutObjectRetention` / `PutObjectLegalHold` must commit durably before acking; a lock you were told
  was applied must survive a crash, and a lost *extend* silently weakens WORM.

### 3.3 Why the data layer is the real boundary

The destructive paths in this system are mostly **background workers that never touch the API**
(`object-lock.md` §5, `tier2-handoff` §2). Putting the predicate in the SQL that feeds the unpinner
and hard-delete ring means WORM correctness does not depend on every API path remembering to check —
which is exactly the class of omission that produced the S4 hole (§4). The API check is for good
error messages; the SQL check is what protects bytes. The FS cache janitor is explicitly **excluded**
(eviction ≠ deletion — §1.3).

### 3.4 The single-predicate escape hatch (no bypass parameter anywhere)

COMPLIANCE is absolute for the retention window; the **only** removal is account termination, folded
into the predicate via `terminated_at` (§0.1). **No unpinner, reaper, `hard_delete_object`, or ops
script takes a `force`/`bypass_locks` argument** — such a boolean in a worker signature is one
careless refactor away from silent, unrecoverable data loss (`object-lock.md` §5.3.1). Termination is
a recorded, audited DB fact with a timestamp and an actor; suspension (PR #422) sets no such flag, so
suspension inherently cannot destroy locked data (`object-lock.md` §5.3, B8/B12). Accepted
consequences (`object-lock.md` §5.3): Hippius **absorbs** the storage cost of locked bytes on
suspended/non-paying accounts (making the §2.2 cap load-bearing); GDPR erasure vs COMPLIANCE has no
technical fix and is resolved contractually (ToS/DPA warranty) — ⚑ Q7.

### 3.5 A lock does not protect the key

`object-lock.md` §5.5: the DEK is wrapped by the bucket KEK wrapped by the OVH KMS master key.
Destroying a bucket KEK makes locked data permanently unreadable while leaving it undeletable — the
worst outcome, and it defeats WORM as surely as deletion (a regulator does not distinguish "deleted"
from "cryptographically destroyed" — `object-lock.md` §5.3.3). **KEK destruction MUST be blocked for
lock-enabled buckets**, and crypto-shredding is not a GDPR escape. Wire this into the key-management
path, not just the object path. ⚑ Q8.

---

## 4. S4 append — scope decision

### 4.1 What S4 append is, and the Python hole (precisely)

S4 append ([`../s4.md`](../s4.md); `06` §5) is a hippius extension triggered by
`PUT /{bucket}/{key}` with `x-amz-meta-append: true`. Its defining property is **atomic O(delta)**: it
publishes only the new delta as a new `parts` row on the **same** `object_version`, updates size and
the composite ETag, and **does not mint a new version** (`06` §5.1; `s4.md` "Server behavior"). It is
serialised by `SELECT … FOR UPDATE` on the object row and guarded by a **version CAS**,
`x-amz-meta-append-if-version: <int>`, against a per-object `append-version` counter (mismatch → `412
PreconditionFailed` with the current value; missing/malformed → `400 InvalidRequest`). Base `PutObject`
returns `append-version: 0` so clients can chain appends without a HEAD.

**The hole** (`06` §5.4; `object-lock.md` §5.2, gap G9): because append mutates a version **in place**
— same version id, more bytes, new composite MD5 — and `ObjectWriter.append_stream` takes only the DB
row lock and **never consults `is_version_locked`**, an append to an Object-Lock-protected current
version silently breaks WORM. Nothing in the AWS test suite would ever catch it, because AWS has no
append verb.

### 4.2 Recommendation: **INCLUDE S4 append in v1, with a structural WORM gate — do NOT copy-on-append**

Ship S4 append in v1 (it is a live, documented hippius extension with real log-append clients, and the
write path already reads across append part boundaries — `06` §4.3). Keep its O(delta) in-place
semantics unchanged for unprotected versions, and close the hole exactly as the design decided
(`object-lock.md` §5.2, A1–A6):

> **Rule S4-WORM.** `PUT` with `x-amz-meta-append: true` is refused with **`403 AccessDenied` when the
> current version is `protected()`**. It is allowed (`200`) when the current version is unprotected —
> including in a lock-enabled bucket, and including when a *superseded* version is locked but the
> current one is not (`object-lock.md` A4, A6).

**Reject the copy-on-append-to-a-new-version alternative.** It is superficially attractive (it would
let an append "succeed" over a locked version by forking a new unprotected version), but it is wrong
for this product:

1. It **destroys the O(delta) guarantee** that is S4's entire reason to exist (`s4.md`
   "Performance"): forking a new version means copying the whole prior object, turning every append
   into O(total size).
2. It **breaks the `append-version` CAS/chaining model** (`s4.md`): the counter and the
   linearizable-append contract are defined against a single evolving version; forking on a lock
   boundary makes "chain appends without a HEAD" incoherent.
3. The design already **decided 403** and wrote the tests for it (`object-lock.md` §5.2, A1–A3).

Make the gate **structural, not a bolt-on check** — this is the specific fix for how the Python bug
happened (a verb that forgot to consult the predicate). In the rewrite, the append writer already
holds `FOR UPDATE` on the version row for its CAS; it MUST evaluate `protected()` **in that same
transaction** and refuse before publishing the delta. Enforcement and the CAS share one lock and one
transaction, so there is no code path that appends without having consulted the lock (`object-lock.md`
X4: an append racing a `PutObjectRetention` is refused if the lock commits first). Because append is
in-place and enqueues no unpin/delete, the §1.3 worker layer is not the concern here — the write-path
predicate is the whole defence, which is why it must be un-forgettable.

### 4.3 Wire behaviour (unchanged from `s4.md`/`06` except the added gate)

| Condition (append) | Result |
|---|---|
| current version `protected()` | **`403 AccessDenied`** (new; the fix) — size/MD5 unchanged; `object-lock.md` A1–A3 |
| current version unprotected (lock bucket or not) | `200`, O(delta) in-place; `append-version` incremented; `object-lock.md` A4/A6 |
| `append-if-version` missing / malformed | `400 InvalidRequest` (`s4.md`; `06` §5) |
| `append-if-version` mismatch | `412 PreconditionFailed` + `x-amz-meta-append-version: <current>` + `Retry-After` (`06` §5) |
| `If-None-Match: *` (append targets an existing key) | existing key → `412`; absent key → `404 NoSuchKey`; judged inside the same row lock (`s4.md`; `06` §5.2) |
| `Content-MD5` mismatch (covers the delta only) | `400 BadDigest` (`06` §3.1, §5.2) |
| empty delta | `400 InvalidRequest` (`06` §5.2) |
| durability (WORM bucket) | the appended delta chunk is subject to the same sync-to-HCFS-before-ack policy as any WORM write (§3.1) before the `200` |

⚑ Q3: idempotency (`x-amz-meta-append-id`, Redis-cached result) is best-effort in Python (`06` §5.1);
confirm whether a cached success may be *replayed* after the current version becomes protected — the
predicate must be re-evaluated on replay, not served from cache, or the cache reopens the hole.

---

## 5. ⚑ Open questions

Carried forward from the source docs, plus reconciliation items surfaced here:

- **✅ Q1 — Retention cap default: RESOLVED (register D2).** `HIPPIUS_OBJECT_LOCK_MAX_RETENTION_DAYS`
  default **2555 days (~7y)**; absolute max **36,500 days**. Load-bearing given §3.4's cost commitment.
- **✅ Q2 — `Years` convention: RESOLVED (register D2).** A year = **365 days** (not calendar).
- **⚑ Q3 — S4 idempotency replay vs lock.** Confirm a cached append-id success is not replayed over a
  now-protected version (§4.3).
- **⚑ Q4 — Multi-alias versioned delete.** Same-bucket CopyObject creates a name alias on one
  `object_id` (`06` §9.3); versioned delete of a >1-name object is `501` today (`06` §6.2). Confirm
  the lock decision defers to that `501` and does not need its own rule.
- **⚑ Q5 — Lifecycle × lock.** No lifecycle engine exists; if one is added, a retention date must win
  over expiration (AWS: yes), lifecycle runs with no bypass (§1.5; `tier2-handoff` §10).
- **⚑ Q6 — Retroactive default retention.** AWS does not apply a newly-set default to existing
  versions; confirm we match (`object-lock.md` Q4, R4).
- **⚑ Q7 — COMPLIANCE as regulator-grade + GDPR.** We have no Cohasset assessment; recommend
  describing it as "compliance-mode semantics" and **not** claiming SEC 17a-4 / FINRA / CFTC
  (`object-lock.md` Q5). GDPR erasure vs WORM is contractual (ToS/DPA warranty + a warning at
  lock-enable time) and needs legal sign-off before COMPLIANCE is offered (`object-lock.md` §5.3.3).
- **⚑ Q8 — KEK-destruction block.** Wire the "cannot destroy a lock-enabled bucket's KEK" rule into
  the key-management path (§3.5; `object-lock.md` §5.5).
- **⚑ Q9 — Stale enforcement docstring.** `object_lock_enforcement.py`'s module docstring says delete
  enforcement is "NOT WIRED YET" but the delete paths do call it (`06` §10 Q1). The rewrite should
  treat delete-of-locked as `403`, not `501`; do not carry the stale docstring forward.

---

## Sources

**AWS** (canonical Object Lock semantics cited inline as [AWS], and the basis the internal docs
themselves cite): Amazon S3 User Guide, *Locking objects with Object Lock* — retention periods,
GOVERNANCE vs COMPLIANCE, retain-until-date, legal holds, "how deletes work" (permanent DELETE →
403; simple DELETE → 200 + delete marker), the versioning prerequisite, and
`s3:BypassGovernanceRetention` / `x-amz-bypass-governance-retention`:
https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lock-overview.html and
.../object-lock.html (retrieved 2026-09-15).

**Internal:** [`06-s3-protocol-conformance.md`](./06-s3-protocol-conformance.md) (§5 append, §6 object
lock, §7 errors, §9 divergences, §10 open questions);
[`10-write-path-decision.md`](./10-write-path-decision.md) (per-bucket durability policy);
[`../../object-lock.md`](../../object-lock.md) (predicate, weakening, E1–E9, §5.2 S4-vs-WORM, §5.3
COMPLIANCE/`terminated_at`, §5.5 durability/KEK, §8 test plan);
[`../../specs/s3-object-lock.md`](../../specs/s3-object-lock.md) (tiered surface, validation);
[`../../specs/s3-object-lock-tier2-handoff.md`](../../specs/s3-object-lock-tier2-handoff.md) (two-layer
guard, the three destroyers, the delete-shape split, tests 7–8); [`../s4.md`](../s4.md) (append wire
contract).
