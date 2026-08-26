# Object Lock — design

What S3 Object Lock is, the subset `hippius-s3` should implement, what already exists, and the test
plan that has to define it before any of it is written.

Companion to [object-versions.md](object-versions.md). Object Lock is meaningless without
versioning, so this document assumes PR #437 has landed.

Research date: **2026-08-24**.

---

## 1. Summary

Object Lock is WORM (write-once-read-many) for S3: an object *version* can be pinned so that nothing
— not the owner, not us — can destroy it until a deadline passes or a hold is lifted.

Two things already exist and neither is enough on its own:

- A **buried branch**, `origin/feat/object-lock-tier0` (2 commits, May 2026, 1,075 commits behind
  staging). It ships the *bucket-level* configuration surface and returns a clean `501` for
  everything per-object. It was written when we had no versioning, so it explicitly deferred all
  enforcement.
- **PR #437** (`feat/object-versioning`) supplies exactly the substrate that branch said it was
  missing: `versioning_status`, delete markers, `?versionId` on every verb, `ListObjectVersions`,
  per-version soft delete, and a version-scoped reaper.

Merging the two is mechanically easy — I did it (§7). The work that remains is **enforcement**, and
enforcement is where all the risk is.

**Recommendation: do not fold this into PR #437.** #437 is a 332-file release already through
review. Land it, then ship Object Lock as its own PR against the merged base. §7 has the sequencing.

---

## 2. The rules, distilled

Everything in Object Lock follows from one sentence:

> **A lock is a property of a version. A protected version's bytes and its lock metadata cannot be
> destroyed or weakened.**

### 2.1 State

Each object version carries two independent things:

| Field | Values |
|---|---|
| Retention | `mode ∈ {GOVERNANCE, COMPLIANCE}` + `retain_until` (a timestamp) |
| Legal hold | `ON` / `OFF` — no expiry, no mode |

```
protected(version) := legal_hold == ON  OR  now() < retain_until
```

That predicate is the whole feature. Everything else is which operations consult it.

### 2.2 What protection blocks

Exactly two things, and one of them is not what people expect:

| Operation | Protected version |
|---|---|
| `DELETE ?versionId=` | **refused** — 403 `AccessDenied` |
| Weakening the lock (shorten / remove retention, downgrade mode) | **refused** — 403 `AccessDenied` |

### 2.3 What protection does *not* block

This is the half that gets misread, so it is worth being explicit:

| Operation | Result | Why |
|---|---|---|
| `PUT` same key | **allowed** — new version | "Overwrite protection" means *the old version survives*, not that writes fail |
| `DELETE` (no `versionId`) | **allowed** — inserts a delete marker | The key appears gone; nothing is destroyed |
| Deleting a delete marker | **allowed** | Markers are **never** WORM-protected, regardless of the lock on the version beneath |
| `GET` / `HEAD` / `ListObjectVersions` | **allowed** | Lock is about destruction, not access |
| Lifecycle expiration of a locked version | refused | (We have no lifecycle enforcement anyway) |

### 2.4 Weakening rules

"Weakening" is the subtle part. Strengthening is always fine; weakening depends on mode.

| Change | GOVERNANCE | COMPLIANCE |
|---|---|---|
| Extend `retain_until` | allowed | allowed |
| Shorten / remove `retain_until` | needs bypass | **never** |
| `GOVERNANCE` → `COMPLIANCE` | allowed | — |
| `COMPLIANCE` → `GOVERNANCE` | — | **never** |
| Delete the version | needs bypass | **never** |
| Legal hold `ON` → `OFF` | always allowed | always allowed |

"Needs bypass" = caller holds the `s3:BypassGovernanceRetention` permission **and** sends
`x-amz-bypass-governance-retention: true`. Both, explicitly.

Two consequences worth internalising:

- **Legal hold is never bypassed.** Bypassing governance does not touch it. A version under legal
  hold with an expired retention is still protected.
- **COMPLIANCE has no escape hatch.** In AWS the only exit is closing the AWS account. Whatever we
  build, COMPLIANCE means *we* cannot delete it either. See §5.3.

### 2.5 Prerequisites

- Object Lock requires versioning `Enabled`.
- Once Object Lock is on, versioning **cannot be suspended** and Object Lock **cannot be disabled**.
- A bucket default retention rule applies a lock to every new version at write time. It is computed
  at PUT time from the then-current rule; changing the rule later does not touch existing versions.
- Explicit per-request `x-amz-object-lock-*` headers override the bucket default.

---

## 3. What R2 does, and why it is the wrong target

The ask was "something a la R2". Worth flagging before it shapes the scope: **R2 does not implement
S3 Object Lock at all.** Its "bucket locks" are a different feature — prefix-scoped retention *rules*
on the bucket (up to 1,000), configured through Cloudflare's own REST API and Wrangler, with no
per-object `x-amz-object-lock-*` surface, no legal hold, and no compliance mode.

That matters because of who actually asks for this. The driver recorded when the buried branch was
written was **backup tools probing for Object Lock support** — Veeam, restic, Commvault. Those tools
call `GetObjectLockConfiguration` and then `PutObjectRetention` per object, and Veeam's immutability
feature specifically wants COMPLIANCE mode. An R2-shaped feature answers none of those calls, so it
would buy us nothing with the audience that motivated the work.

The right reference is **MinIO / Backblaze B2 / NetApp StorageGRID**: the real S3 Object Lock API,
minus the AWS-ecosystem extras. That is what §4 proposes.

---

## 4. Proposed scope

### 4.1 In

| Surface | Notes |
|---|---|
| `PutObjectLockConfiguration` / `GetObjectLockConfiguration` | Bucket config + optional default retention rule |
| `CreateBucket` with `x-amz-bucket-object-lock-enabled: true` | Implies `versioning_status = 'Enabled'` |
| `PutObjectRetention` / `GetObjectRetention` | Per-version, `?versionId` optional |
| `PutObjectLegalHold` / `GetObjectLegalHold` | Per-version |
| `x-amz-object-lock-{mode,retain-until-date,legal-hold}` | On `PutObject` and `CreateMultipartUpload` |
| Same three headers echoed on `GET` / `HEAD` | Read-side |
| Both modes: GOVERNANCE and COMPLIANCE | With a server-side retention cap — §5.3 |
| `x-amz-bypass-governance-retention` | Master token only — §5.4 |
| Enforcement on versioned `DELETE`, `DeleteObjects`, unpinner, reaper, janitor, purge scripts | §5 — this is the actual work |

### 4.2 Out

| Feature | Why |
|---|---|
| `x-amz-bucket-object-lock-token` | AWS's confirmation gate for enabling lock on an existing bucket. Accept and ignore. |
| `s3:object-lock-remaining-retention-days` bucket-policy condition | Replaced by a flat server config cap (§5.3) |
| MFA delete | No MFA device binding exists |
| Replication / S3 Inventory / Storage Lens / Batch Operations | We have none of these |
| `Content-MD5` requirement on locked PUTs | AWS mandates it; being more lenient breaks nobody |
| Lifecycle `NoncurrentVersionExpiration` interaction | `PutBucketLifecycle` is still parse-and-discard repo-wide |

### 4.3 Data model

Bucket config reuses the buried branch's `buckets.object_lock` JSONB. Per-version state needs three
new columns on `object_versions`:

```sql
ALTER TABLE object_versions
  ADD COLUMN IF NOT EXISTS object_lock_mode text NULL,          -- 'GOVERNANCE' | 'COMPLIANCE'
  ADD COLUMN IF NOT EXISTS object_lock_retain_until timestamptz NULL,
  ADD COLUMN IF NOT EXISTS object_lock_legal_hold boolean NOT NULL DEFAULT false;
```

All three are metadata-only on PG 11+ (nullable, or `NOT NULL DEFAULT false`), so no rewrite of the
~146M-row table — the same property [20260822120000](hippius_s3/sql/migrations/20260822120000_object_versioning.sql)
relies on. The partial index that backs the enforcement predicate must be built `CONCURRENTLY` in a
separate `transaction:false` migration, exactly as `20260822120001` does and for the same reason
(migrations run on API pod startup; a blocking index build is a data-plane outage).

**Default retention is materialised at write time**, not computed lazily by joining `buckets`. Two
reasons: it matches AWS semantics (the rule in force at PUT time is the one that sticks), and it
turns every enforcement check into a predicate on one row instead of a join against bucket config
that may since have changed.

---

## 5. Enforcement points

This is the safety-critical inventory. Blocking the API verb is the easy half; the destructive paths
in this system are mostly *background workers*, and they do not go through the API.

### 5.1 The paths that can destroy a locked version

| # | Path | Today | Needed |
|---|---|---|---|
| E1 | `DELETE ?versionId=` → `delete_object_version` | soft-deletes, enqueues unpin | refuse if protected |
| E2 | `DeleteObjects` bulk with per-key `VersionId` | same, per key | per-key `AccessDenied` in the result XML, not a whole-request failure |
| E3 | **Whole-object `DELETE` on an unversioned/lock-free path** | `soft_delete_object` + unpin with `object_version = NULL` → **every version** | refuse if *any* version is protected |
| E4 | `DeleteBucket` | already refuses non-empty | verify a delete-marked-only bucket cannot slip through |
| E5 | **Unpinner** | deletes from Arion; `NULL` version = all versions | gate on the lock predicate |
| E6 | **Version reaper** (`find_versions_ready_for_reap.sql`) | reaps `parts` of soft-deleted versions | add lock predicate as belt-and-braces |
| E7 | `hard_delete_object.sql` | cascades once no live backend rows | same |
| E8 | **Janitor FS eviction** | replication-gated | *no change needed* — FS is a cache; bytes live on Arion |
| E9 | **Admin purge / account suspension** (PR #422), `nuke_user.py`, `purge_buckets.py` | unconditional | **must respect COMPLIANCE locks** — §5.3 |

E3 is the one that bites. A simple `DELETE` on a bucket that is lock-enabled but *not* versioning-
enabled — which the buried branch's Tier 1 permits, since it never enforced the versioning
prerequisite — enqueues one unpin covering every version of the object. That is unrecoverable.
Enforcing "lock ⇒ versioning Enabled" at configuration time is what makes E3 structurally impossible
rather than merely guarded.

Layered defence, in order: refuse at the API (E1–E4), gate the queue producer (E5), and keep the
predicate in the reap SQL (E6, E7) so that a bug in the first two layers still cannot destroy bytes.

### 5.2 S4 append is a WORM violation

The sharpest hippius-specific finding, and it has no AWS analogue.

`ObjectWriter.append_stream` ([object_writer.py:972](hippius_s3/writer/object_writer.py)) does **not**
mint a new version. It reads `objects.current_object_version` as `cov`, takes `FOR UPDATE` on that
row, and inserts a new `parts` row at `(object_id, object_version = cov, part_number = MAX+1)`.

So an append **mutates an existing version in place**: same version id, more bytes, different size,
different composite MD5. If that version is protected, WORM has been broken silently — the exact
thing the feature promises cannot happen.

**Rule: `PutObject` with `x-amz-meta-append: true` must be refused with 403 `AccessDenied` when the
current version is protected.** This needs a test of its own; nothing in the AWS test suite would
ever catch it.

### 5.3 COMPLIANCE mode — DECIDED (2026-08-24)

**Every internal destructive path respects COMPLIANCE locks. The only bypass is deleting the entire
account.** This mirrors AWS exactly, and it is a deliberate product commitment, not a default.

Consequences we are accepting, stated plainly so nobody is surprised later:

- **Suspension does not reclaim, and we absorb the cost.** Suspending or purging a non-paying
  account leaves its compliance-locked bytes pinned until retention expires. **Decided 2026-08-24:
  Hippius absorbs that storage cost rather than passing it on or forcing termination.** This makes
  the retention cap (§5.3.2) the only thing standing between us and unbounded liability, so it is a
  hard requirement of PR D, not a follow-up.
- **Abuse and legal takedown lose their fine-grained tool.** Neither can remove a single locked
  object. The only lever is account termination, which is all-or-nothing.
- **GDPR erasure conflicts.** An erasure request against compliance-locked data cannot be satisfied
  short of terminating the account. There is no technical escape — see §5.3.3. Needs a
  contractual answer, not an engineering one.
- **A bucket holding a locked version can never be deleted** until the lock expires. That is correct
  AWS behaviour (`BucketNotEmpty`), but it will read as a bug to the first user who hits it.

#### 5.3.1 Make the escape hatch declarative, not a force flag

The obvious implementation — thread a `force=True` / `bypass_locks=True` parameter from the
termination path down through the unpinner, the reaper, and the purge scripts — is the wrong shape.
A boolean that means "destroy WORM-protected data" living in the signature of functions that
background workers call is one careless refactor away from being passed `True` by something that
should never have it, and the failure is silent and unrecoverable.

Instead, fold termination into the predicate itself. Record it as an account-level fact:

```sql
ALTER TABLE users ADD COLUMN IF NOT EXISTS terminated_at timestamptz NULL;
```

```
protected(version) := account.terminated_at IS NULL
                      AND (legal_hold == ON OR now() < retain_until)
```

Now **no call site anywhere takes a bypass parameter.** The API gates, the unpinner, the reaper and
`hard_delete_object` all keep the single predicate they already had, and account termination works
by making that predicate false for every version the account owns — once, in one recorded, audited
place. Terminating an account becomes a deliberate DB fact with a timestamp and an actor, which is
also exactly what you want to be able to show an auditor.

Suspension (PR #422) sets no such flag, so suspension inherently cannot destroy locked data. The two
operations stop being two settings on one code path and become genuinely different things.

#### 5.3.2 Still required

1. **A server-side maximum retention cap** (`HIPPIUS_OBJECT_LOCK_MAX_RETENTION_DAYS`). AWS expresses
   this through a bucket-policy condition key we do not have; a flat config bound is the cheap
   equivalent and stops a `Years=100` lock on 100 TB. Given §5.3's cost commitment this is now
   load-bearing, not a nicety — it is the only bound on how much unreclaimable storage one account
   can create.
2. **Metrics**: locked bytes per account, split GOVERNANCE / COMPLIANCE, plus locked bytes belonging
   to suspended accounts — that last one is the number that turns into a bill.
3. **Termination runbook**: terminating an account is now the only way to remove locked data, so it
   needs to actually work, be audited, and be hard to invoke by accident.

#### 5.3.3 GDPR erasure vs. WORM

Not a lawyer; this is the shape of the problem so whoever owns it knows what they are deciding.

**The conflict.** GDPR Article 17 gives a person the right to have their personal data erased. If
that data sits inside an object under a COMPLIANCE lock, we cannot delete it until retention
expires — and per §5.3 the only override is terminating the whole account, which is absurd as a
response to one erasure request.

**Who the request actually reaches.** Under GDPR our customer is normally the *controller* and we
are the *processor*. The person asks the customer; the customer instructs us. So the failure mode is
not "a stranger demands deletion and we refuse" — it is **our own customer instructing us to delete
an object they themselves locked, and us having to say no.** That framing matters: the bind is
largely of the customer's making, but we are still a processor that cannot execute a controller's
lawful instruction.

**Why this is usually fine.** Article 17(3)(b) exempts erasure where processing is necessary for
compliance with a legal obligation. WORM retention driven by SEC 17a-4, FINRA or CFTC *is* exactly
such an obligation, and in that case the retention duty outranks the erasure right. This is the
standard industry resolution and it is why AWS ships COMPLIANCE mode at all.

**Where it is not fine.** When someone uses COMPLIANCE mode casually — no underlying legal
retention obligation — and then needs to erase. Now there is a hard lock with no legal justification
backing it, and no way out. That is the case to design against.

**No technical escape exists — including crypto-shredding.** Worth stating explicitly because it is
the first idea everyone has, and it looks especially attractive here given our envelope encryption.
Destroying a version's `wrapped_dek` would render the object permanently unreadable without deleting
a single byte. It does not work:

- Rendering locked data unreadable *is* the harm WORM exists to prevent. A regulator does not
  distinguish "deleted" from "cryptographically destroyed" — both fail an audit that asks to read
  the record.
- It contradicts §5.5, which requires blocking KEK destruction for lock-enabled buckets for exactly
  this reason.
- It would not even be a real erasure under GDPR without careful argument, since the ciphertext
  persists.

So the answer has to be contractual.

**Recommendation.** Put it in the ToS / DPA: enabling COMPLIANCE mode requires the customer to
warrant they have a lawful basis for the retention period, and to accept that erasure requests
against locked objects cannot be honoured until expiry. Surface the same warning at the point of
enabling lock on a bucket, not buried in docs. Combined with a conservative retention cap (§5.3.2),
this bounds the exposure. This mirrors AWS, where the same responsibility sits with the customer as
controller.

### 5.4 Permissions

We have no IAM. The mapping that fits the existing model:

- `s3:BypassGovernanceRetention` → **master tokens only**. Master tokens already bypass ACL entirely
  ([acl.py:126-130](hippius_s3/gateway/middlewares/acl.py)), so this is consistent.
- Sub-tokens never bypass. `sub_token_scope.py` already lists `object-lock` in
  `_BUCKET_META_SUBRESOURCES` but is dormant and currently imports a nonexistent `TokenAcl`; do not
  make Object Lock the thing that wakes it up.
- The `?retention` and `?legal-hold` object subresources need adding to the ACL middleware's
  subresource map so they are authorised as writes, not reads.

### 5.5 Durability and keys

Two honest caveats that belong in user-facing docs:

- **A lock is not durability.** PUT returns 200 once bytes are on the node SSD; the Rust drain
  replicates to Ceph afterwards and can land in `failed` / `corrupt`. A locked object whose drain
  failed is a single-copy locked object. Object Lock raises the stakes on that existing gap.
- **A lock does not protect the key.** The DEK is wrapped by the bucket KEK, wrapped by the OVH KMS
  master key. Destroying a bucket KEK makes locked data permanently unreadable while leaving it
  undeletable — the worst of both. KEK destruction must be blocked for lock-enabled buckets. AWS
  documents the same hazard for SSE-KMS.

---

## 6. Where we are

### 6.1 What the buried branch already gives us

`origin/feat/object-lock-tier0`, 1,672 insertions:

| File | What |
|---|---|
| `bucket_object_lock_endpoint.py` | GET/PUT `?object-lock`, namespace-tolerant XML parse, full validation |
| `object_lock_guard.py` | Central 501 for per-object surface |
| `20260521000000_add_buckets_object_lock.sql` | `buckets.object_lock` JSONB |
| `update_bucket_object_lock.sql` + two `SELECT` additions | Queries |
| `bucket_create_endpoint.py` | `x-amz-bucket-object-lock-enabled` honoured |
| `specs/s3-object-lock.md` | 358-line spec, tiers 0/1/2 |
| 4 test files | 947 lines, e2e + unit |

The XML parsing and validation are genuinely reusable — mode validation, `Days`/`Years` XOR,
positive-integer checks, `MalformedXML` shapes. That is the fiddly, boring part and it is done.

### 6.2 What it gets wrong now

| Issue | Detail |
|---|---|
| Stale `request.state` contract | Uses `request.state.account.main_account`; the gateway merge made handlers use `request.state.main_account_id` (bucket-owner attribution with caller fallback). Merges cleanly, resolves to the wrong account for delegated access. Fixed in my probe merge. |
| No versioning prerequisite | `PUT ?object-lock` succeeds on any bucket. Post-#437 this must require `versioning_status = 'Enabled'` — see E3. |
| `x-amz-bucket-object-lock-enabled` does not enable versioning | AWS makes it imply versioning. |
| Uses bare `ET.fromstring` | Repo convention is `parse_untrusted_xml` ([xml_helpers.py](hippius_s3/xml_helpers.py)) — a default parser loads DTDs and expands entities. **Security fix, not style.** |
| Builds XML by hand | Should use `create_element`/`to_xml_bytes` so values are escaped. |
| Spec's Tier 2 plan is stale | Written pre-#437; its "implement versioning first" steps are now done. |

### 6.3 The probe merge

I unburied it and merged forward, in a scratch worktree, in two steps:

```
origin/feat/object-lock-tier0
  ← origin/staging              2 conflicts (buckets/router.py, objects/router.py — ?acl dispatch)
  ← feat/object-versioning      2 conflicts (both SELECT column lists)
```

All four conflicts were trivial and are resolved. Result: **2,973 unit tests pass, 37 skipped.**
The branch is not rotten — it is just old, and its foundations arrived after it.

Worktree: `…/scratchpad/ol-merge`, branch `tmp/object-lock-merge-probe`. Nothing pushed.

### 6.4 Gap list

Everything below is unwritten. Ordered by risk, not by effort.

| # | Gap | Where |
|---|---|---|
| G1 | Per-version lock columns + `CONCURRENTLY` index | 2 migrations |
| G2 | Versioning prerequisite on lock enablement; lock-enabled ⇒ versioning Enabled | `bucket_object_lock_endpoint.py`, `bucket_create_endpoint.py` |
| G3 | `PutObjectRetention` / `GetObjectRetention` | new endpoint |
| G4 | `PutObjectLegalHold` / `GetObjectLegalHold` | new endpoint |
| G5 | Write-path headers on `PutObject` + `CreateMultipartUpload`; bucket default materialised | `put_object_endpoint.py`, `multipart.py` |
| G6 | Read-path header echo | `get_object_endpoint.py`, `head_object_endpoint.py` |
| G7 | **Enforcement E1–E4** (API delete paths) | `delete_object_endpoint.py`, `delete_objects_endpoint.py` |
| G8 | **Enforcement E5–E7** (unpinner, reaper, hard delete) | worker + SQL |
| G9 | **Append refusal** (§5.2) | `extensions/append.py` |
| G10 | Bypass permission wiring + ACL subresource map | `acl.py` |
| G11 | Retention cap config; E9 purge decision; locked-bytes metrics | `config.py`, purge scripts |
| G12 | Replace `ET.fromstring` with `parse_untrusted_xml`; escaped XML building | `bucket_object_lock_endpoint.py` |

---

## 7. Sequencing

**Do not add this to PR #437.** #437 already touches 332 files, carries the highest-risk change in
the system (the serveable predicate on the hottest read and list paths), and has been through
review. Object Lock adds two migrations and a new enforcement surface across three background
workers. Bundling them means re-reviewing both.

1. Land PR #437.
2. PR A — **foundation, no enforcement.** The buried branch, merged forward, plus G1, G2, G12. Ships
   bucket config + per-version columns; per-object verbs still 501. Independently useful (backup
   tools get a truthful answer) and independently reviewable.
3. PR B — **the lock surface.** G3–G6. Locks can be set and read but nothing enforces them yet.
   Every enforcement test from §8 is written here and fails.
4. PR C — **enforcement.** G7–G10. The §8 tests go green. This is the PR that gets the paranoid
   review.
5. PR D — **operational.** G11, docs, metrics, `docs/s3-compatibility.md`.

PR C is the one where a bug means permanent data loss or permanently unreclaimable storage. Keeping
it small and separate is the point of the split.

---

## 8. Test plan

Safety-critical, so the tests are written first and define the feature. Ordered by what they protect.

Convention note: the buried branch used `xfail(strict=False)` for unwritten tiers. Use
`strict=True` instead — a non-strict xfail that silently starts passing tells you nothing, and for
this feature an unexpected pass is exactly the signal worth catching.

### 8.1 The predicate (unit, pure)

`protected()` is the entire feature. Test it in isolation, no DB, no HTTP.

| # | Case | Expect |
|---|---|---|
| P1 | no retention, no hold | not protected |
| P2 | `retain_until` in future | protected |
| P3 | `retain_until` in past | not protected |
| P4 | `retain_until` exactly now | not protected (boundary — pick and pin it) |
| P5 | legal hold ON, no retention | protected |
| P6 | legal hold ON, retention expired | **protected** |
| P7 | legal hold OFF, retention in future | protected |
| P8 | legal hold ON + retention in future | protected |
| P9 | mode COMPLIANCE vs GOVERNANCE | identical result — mode does not affect *whether* protected, only who may weaken |

### 8.2 Weakening (unit)

Table-driven over §2.4. For each `(current_mode, current_until, new_mode, new_until, bypass)` assert
allow/deny.

| # | Case | Expect |
|---|---|---|
| W1 | GOVERNANCE, extend | allow |
| W2 | GOVERNANCE, shorten, no bypass | deny 403 |
| W3 | GOVERNANCE, shorten, with bypass | allow |
| W4 | GOVERNANCE, remove (empty retention), with bypass | allow |
| W5 | GOVERNANCE → COMPLIANCE | allow |
| W6 | COMPLIANCE, extend | allow |
| W7 | COMPLIANCE, shorten, with bypass | **deny 403** |
| W8 | COMPLIANCE, remove, with bypass | **deny 403** |
| W9 | COMPLIANCE → GOVERNANCE, with bypass | **deny 403** |
| W10 | set retention on an unlocked version | allow |
| W11 | legal hold ON → OFF, no bypass | allow |
| W12 | legal hold OFF → ON on a COMPLIANCE version | allow |
| W13 | bypass header present, caller lacks permission | deny 403 |
| W14 | caller has permission, header absent | deny 403 — **both** required |
| W15 | `retain_until` beyond the configured cap | deny 400 |
| W16 | `retain_until` in the past on a fresh PUT | deny 400 `InvalidArgument` |

### 8.3 Delete enforcement (the ones that matter)

Integration, real DB. Each asserts **both** the HTTP response and that the bytes are still there.

| # | Case | Expect |
|---|---|---|
| D1 | `DELETE ?versionId` on GOVERNANCE-locked version | 403; version still readable |
| D2 | D1 + bypass + master token | 204; version gone |
| D3 | D1 + bypass + sub-token | 403 |
| D4 | `DELETE ?versionId` on COMPLIANCE-locked version, with bypass | 403; still readable |
| D5 | `DELETE ?versionId` on legal-held version, with bypass | 403 |
| D6 | D5 after legal hold removed, retention expired | 204 |
| D7 | Retention expired, no hold | 204 |
| D8 | **Simple `DELETE` on locked key** | 204 + delete marker; locked version still readable by id |
| D9 | **`DELETE ?versionId` of the delete marker from D8** | 204 — markers are never protected |
| D10 | **`DeleteObjects` bulk, one locked + one free** | free key deleted, locked key `AccessDenied` in result XML, HTTP 200 |
| D11 | **Whole-object delete path (E3) with any locked version** | refused; **no unpin enqueued** |
| D12 | `DeleteBucket` containing a locked version | `BucketNotEmpty` |
| D13 | `PUT` over a locked key | 200, new version; old version intact and still locked |

D11 asserts on the queue, not just the response. The unpin is the destructive act.

### 8.4 Background workers (§5.1 E5–E7)

Unit, mirroring the existing janitor-safety tests. These are the belt-and-braces layer — they must
hold **even when the API layer is bypassed**, so construct the DB state directly.

| # | Case | Expect |
|---|---|---|
| B1 | Reaper over a soft-deleted-but-locked version | not in ready set |
| B2 | B1 under critical disk pressure | still not in ready set |
| B3 | Unpinner given `object_version = NULL` where one version is locked | locked version's chunks skipped |
| B4 | `hard_delete_object` readiness with a locked version present | not ready |
| B5 | Reaper on a version whose lock expired mid-sweep | ready — no stale caching of the predicate |
| B6 | Janitor FS eviction of a locked version's chunks | **allowed** — FS is a cache; assert it does not over-block |
| B7 | **Admin account purge vs COMPLIANCE lock** | purge **refuses**; bytes and `chunk_backend` rows intact |
| B8 | **Account suspension (PR #422) vs COMPLIANCE lock** | suspension succeeds, locked data untouched and still pinned |
| B9 | **Account termination** (`terminated_at` set) then reaper/unpinner sweep | locked versions now reapable — the one escape hatch works |
| B10 | Termination of account A vs locked versions owned by account B | B untouched — the predicate is account-scoped |
| B11 | `nuke_user.py` / `purge_buckets.py` / `purge_source_versions.py` against locked versions | refuse, per script |
| B12 | **No destructive worker accepts a lock-bypass parameter** | static/signature assertion — §5.3.1; guards the design, not just the behaviour |

B6 matters as much as the rest: over-blocking eviction would fill the cache disk and take the data
plane down. The gate belongs on backend deletion, not cache reclamation.

### 8.5 Append (§5.2)

| # | Case | Expect |
|---|---|---|
| A1 | Append to a GOVERNANCE-locked current version | 403; size and MD5 unchanged |
| A2 | Append to a COMPLIANCE-locked current version | 403 |
| A3 | Append to a legal-held current version | 403 |
| A4 | Append to an unlocked version in a lock-enabled bucket | 200 |
| A5 | Append after retention expiry | 200 |
| A6 | Append where the locked version is *not* current | 200 — the lock is on a superseded version |

### 8.6 Configuration and prerequisites

| # | Case | Expect |
|---|---|---|
| C1 | `PUT ?object-lock` on a bucket with versioning off | 409 `InvalidBucketState` |
| C2 | `CreateBucket` with lock header | bucket has lock enabled **and** `versioning_status = 'Enabled'` |
| C3 | `PutBucketVersioning Suspended` on a lock-enabled bucket | 409 `InvalidBucketState` (today's blanket 501 is not the same thing — assert the specific code) |
| C4 | `GET ?object-lock` on a bucket that never configured it | 404 `ObjectLockConfigurationNotFoundError` |
| C5 | Config round-trips: GOVERNANCE/Days, COMPLIANCE/Days, GOVERNANCE/Years | XML equivalence |
| C6 | Both `Days` and `Years` / neither / `≤ 0` / bad `Mode` / `ObjectLockEnabled != Enabled` | 400 `MalformedXML` |
| C7 | Empty body / non-XML body | 400 |
| C8 | XML without the S3 namespace | accepted (matches tagging) |
| C9 | **XXE probe** — body with an external entity | rejected, no entity expansion, no outbound fetch |
| C10 | Non-owner calls `PUT ?object-lock` | 403 |
| C11 | Attempt to disable Object Lock once enabled | refused |

C9 is the test that pins G12.

### 8.7 Default retention

| # | Case | Expect |
|---|---|---|
| R1 | Bucket default GOVERNANCE/Days=7; `PUT` with no headers | version locked, `retain_until ≈ created + 7d` |
| R2 | R1 then change the default to 30d | **existing version unchanged**; next PUT gets 30d |
| R3 | Explicit headers present | override the default |
| R4 | Default set on a bucket with existing versions | those versions stay unlocked |
| R5 | `Years=1` default | ≈ 365d, and pin the exact convention chosen |
| R6 | Default applied through `CompleteMultipartUpload` | version locked |
| R7 | Default applied through `CopyObject` destination | destination locked; **source lock not inherited** |

### 8.8 Round-trip and read path

| # | Case | Expect |
|---|---|---|
| T1 | `PutObjectRetention` → `GetObjectRetention` | mode + date round-trip, second precision |
| T2 | `PutObjectLegalHold ON` → `Get` | `ON` |
| T3 | `Get*` on a version with no lock | 404 `NoSuchObjectLockConfiguration` |
| T4 | `Get*` with `?versionId` of a superseded version | that version's lock, not current's |
| T5 | `HEAD` / `GET` on a locked version | all three `x-amz-object-lock-*` headers echoed |
| T6 | `HEAD` on an unlocked version | headers absent, not empty-valued |
| T7 | `Get*` on a delete marker | 405 `MethodNotAllowed` |
| T8 | `PutObjectRetention` on a nonexistent version | 404 `NoSuchVersion` |
| T9 | Malformed `x-amz-object-lock-retain-until-date` on PUT | 400 |
| T10 | `x-amz-object-lock-mode` without a date, and vice versa | 400 — the pair is atomic |

### 8.9 Concurrency

| # | Case | Expect |
|---|---|---|
| X1 | Concurrent `PutObjectRetention` extend + `DELETE ?versionId` | serialised; version survives if the extend commits first |
| X2 | Retention expiring during a delete transaction | no torn state — decide and pin read-time semantics |
| X3 | Concurrent legal hold OFF + delete | delete succeeds only if the hold removal committed first |
| X4 | Append CAS racing a `PutObjectRetention` | append refused if the lock commits first (§5.2 takes `FOR UPDATE` on the version row — reuse it) |

### 8.10 E2E

Against the real stack with boto3, since the SDK is what customers actually use.

| # | Case |
|---|---|
| E1 | `create_bucket(ObjectLockEnabledForBucket=True)` → `put_object_lock_configuration` → `get_…` |
| E2 | `put_object(ObjectLockMode='GOVERNANCE', ObjectLockRetainUntilDate=…)` → `head_object` echoes |
| E3 | `delete_object(VersionId=…)` raises `ClientError` / `AccessDenied` |
| E4 | Same with `BypassGovernanceRetention=True` on a master token → succeeds |
| E5 | Full Veeam-shaped flow: enable lock, write, verify immutable, wait out a short retention, delete |
| E6 | `list_object_versions` shows locked versions with correct `IsLatest` |

E5 short-circuits with a retention of a few seconds so it can run in CI.

---

## 9. Open questions

1. ~~**E9 / COMPLIANCE vs account purge.**~~ **Decided 2026-08-24**: all internal destructive paths
   respect COMPLIANCE locks; the only bypass is full account deletion. See §5.3. Two follow-ons that
   are also now settled:
   - ~~**Commercial**~~ — **decided 2026-08-24: Hippius absorbs the storage cost** for locked data on
     suspended/non-paying accounts. Makes the retention cap (§5.3.2) a hard requirement, since it is
     now the only bound on that liability.
   - **Legal** — GDPR erasure vs. WORM is analysed in §5.3.3. There is no technical fix; the
     resolution is contractual (ToS/DPA warranty + a warning at bucket lock-enable time). Still
     needs a lawyer's sign-off on the wording before COMPLIANCE mode is offered to customers.
2. **Retention cap default.** What does `HIPPIUS_OBJECT_LOCK_MAX_RETENTION_DAYS` start at? AWS's
   ceiling is 100 years. Something like 7 years covers the regulatory cases (SEC 17a-4 is 6) without
   allowing a century-long pin.
3. **`Years` convention.** 365 days, or calendar years? Pin it in R5 either way.
4. **Retroactive default retention** on existing versions when a rule is first set — AWS says no
   (R4). Confirm we want to match.
5. **Do we advertise COMPLIANCE as regulator-grade?** We have no Cohasset assessment. Recommend
   describing it as "compliance mode semantics" and not claiming SEC 17a-4 / FINRA / CFTC.
