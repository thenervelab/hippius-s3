# Handoff: Object Lock Tier 2 — real WORM enforcement

Status: **ready to implement**. Tier 0 and Tier 1 shipped in PR #463.

This is the implementation brief for making locked objects genuinely undeletable. It is
deliberately scoped to the happy path: everything outside the list in §7 stays `501`, exactly
as Tier 0 does today.

**The one-line goal:** once an object version is locked, nothing may remove its bytes from
Arion or the OVH backup — but the Ceph read cache may still evict it freely, because eviction
is not deletion.

---

## 1. What AWS actually guarantees

Read from the AWS Object Lock user guide, reduced to the rules an implementation must honour.

**Locks live on an object VERSION, never on a key.** Two versions of the same key can carry
different modes and different expiry dates. Everything below is per-version.

**Versioning is a hard prerequisite.** "Object Lock works only in buckets that have S3
Versioning enabled." A bucket cannot have Object Lock without versioning.

**Two independent protections, either of which locks a version:**

| | Retention | Legal hold |
|---|---|---|
| Shape | `Mode` + `RetainUntilDate` | on / off |
| Expires | at the timestamp | never — only when removed |
| Removable | see modes below | by anyone with `s3:PutObjectLegalHold` |

They compose: a version with an expired retention **and** a legal hold is still locked. A
version with an active retention stays locked after a legal hold is removed.

**Two retention modes:**

- **GOVERNANCE** — deletion is refused *unless* the caller holds
  `s3:BypassGovernanceRetention` **and** sends `x-amz-bypass-governance-retention: true`.
  Both are required; the header alone is not enough.
- **COMPLIANCE** — nobody can delete or overwrite the version, including the account root.
  The mode cannot be changed and the retention period **cannot be shortened**, only extended.

**Retention may always be extended, never shortened** (in COMPLIANCE; in GOVERNANCE a
shortening requires the bypass). Any holder of `s3:PutObjectRetention` may extend.

**Deletes — the two shapes behave differently, and this is the subtle part:**

- `DELETE key?versionId=X` on a locked version → **`403 AccessDenied`**. This is a permanent
  delete and it is refused.
- `DELETE key` with no versionId on a locked version → **`200 OK`**, and a **delete marker**
  is inserted as a new current version. The locked version is untouched and still there. Locks
  "don't prevent new versions of the object from being created, or delete markers to be added
  on top of the object."

**Overwrites (`PUT` of the same key) are always allowed.** They create a new version; the old
locked version keeps its own retention. A `PUT` never fails because of a lock on a previous
version.

**Bucket default retention** is a duration (`Days` XOR `Years`). On upload, S3 computes
`RetainUntilDate = version creation time + duration`. An explicit per-object retention on the
`PUT` **overrides** the bucket default.

---

## 2. Where this codebase actually deletes data

Three distinct destroyers. Tier 2 must gate the first two and deliberately leave the third alone.

**(a) The unpin path — this is the one that reaches Arion and OVH.**
`delete_object_endpoint.enqueue_object_unpin()` pushes an `UnpinChainRequest` onto
`{backend}_unpin_requests` the moment a delete is accepted. The unpinner worker consumes it and
issues the backend delete. `UnpinChainRequest` already carries `object_version: int | None`
(`None` = every version), so it is version-addressable — no schema change needed to gate it.

**(b) The hard-delete ring — removes the DB rows.**
`workers/run_janitor_in_loop.py` runs `find_objects_ready_for_hard_delete` then
`hard_delete_object`. Note it only considers objects whose `chunk_backend` rows are already
`deleted`, i.e. it runs *after* (a). Gating (a) therefore keeps (b) from ever seeing a locked
object — but gate both anyway; see §4 on defence in depth.

**(c) The Ceph / FS cache janitor — leave this completely alone.**
Evicting a cached chunk removes a copy, not the object. The durable copies are on Arion and the
backup backends, and a cache miss simply refetches. **Do not add a lock check to the FS cache
GC.** Doing so would pin locked objects in NVMe forever and fill the cache — a self-inflicted
outage with no durability benefit. This is an explicit non-goal.

Also in scope because they bypass the API entirely: `scripts/nuke_user.py`,
`purge_buckets.py`, `purge_source_versions.py`, `delete_legacy_object_versions.py`.

---

## 3. Schema

One migration. Locks are per-version, so they live on `object_versions`, which already carries
`is_delete_marker` and `deleted_at`.

```sql
-- migrate:up
ALTER TABLE object_versions
    ADD COLUMN IF NOT EXISTS object_lock_mode TEXT,             -- 'GOVERNANCE' | 'COMPLIANCE' | NULL
    ADD COLUMN IF NOT EXISTS object_lock_retain_until TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS object_lock_legal_hold BOOLEAN NOT NULL DEFAULT FALSE;

-- Partial index: the guard asks "is this version locked", and locked versions are the rare case.
CREATE INDEX IF NOT EXISTS idx_object_versions_locked
    ON object_versions (object_id, object_version)
    WHERE object_lock_retain_until IS NOT NULL OR object_lock_legal_hold;
```

Nullable, defaulted, no rewrite. Name it with a **current** timestamp — a stale one sorts before
migrations already applied everywhere (this bit Tier 1; see PR #463).

---

## 4. The guard — one function, used everywhere

Everything hinges on a single predicate. Put it in `hippius_s3/api/s3/object_lock_enforcement.py`
and let both SQL and Python go through the same definition.

```python
def is_version_locked(row, *, now) -> bool:
    """A version is locked if EITHER protection is active. They are independent."""
    if row["object_lock_legal_hold"]:
        return True
    return row["object_lock_retain_until"] is not None and row["object_lock_retain_until"] > now
```

Enforce it at **two layers**, because they fail differently:

1. **API layer** — returns the correct S3 error to the client.
2. **Data layer** — the SQL that feeds the unpinner and the hard-delete ring excludes locked
   versions. This is what protects against the ops scripts and any future code path that
   forgets the check. Treat it as the real boundary; the API layer is for good error messages.

Concretely, for the data layer: add `AND NOT <locked predicate>` to
`find_objects_ready_for_hard_delete`, and have the unpinner re-check before issuing a backend
delete. A locked version reaching the unpinner is a bug, so log it loudly rather than silently
skipping.

---

## 5. Enforcement points, in priority order

| # | Path | Behaviour when the version is locked |
|---|---|---|
| 1 | `DELETE key?versionId=X` | `403 AccessDenied` before any soft-delete or unpin enqueue |
| 2 | `DELETE key` (no versionId) | **Succeeds** — insert a delete marker, enqueue no unpin for the locked version |
| 3 | `POST ?delete` (DeleteObjects) | Per key, same rules; locked entries come back in `<Error>` with `AccessDenied`, unlocked ones still succeed |
| 4 | `enqueue_object_unpin` | Never enqueue for a locked version. The `object_version=None` ("all versions") form must resolve and skip locked ones |
| 5 | `find_objects_ready_for_hard_delete` | Exclude locked versions |
| 6 | `DELETE /bucket` | Already refuses non-empty buckets with `BucketNotEmpty`; verify a bucket holding only locked versions is non-empty by that query's definition |
| 7 | Ops scripts | Refuse locked versions unless given an explicit `--i-know-this-breaks-worm` flag |

**#2 is the one people get wrong.** A simple DELETE on a locked object must return `200`, not
`403`. Getting this backwards breaks ordinary clients against a lock they never asked about.

---

## 6. API surface to build

Four operations, all per-object, all currently answering `501` via `object_lock_guard`:

- `PUT /{bucket}/{key}?retention` — body `<Retention><Mode/><RetainUntilDate/></Retention>`.
  Reject shortening unless GOVERNANCE + valid bypass. Reject any change to a COMPLIANCE version.
- `GET /{bucket}/{key}?retention` — echo it back; `404 NoSuchObjectLockConfiguration` if unset.
- `PUT /{bucket}/{key}?legal-hold` — body `<LegalHold><Status>ON|OFF</Status></LegalHold>`.
- `GET /{bucket}/{key}?legal-hold`.

All four take an optional `?versionId=`; without one they act on the current version.

**Two behaviours to wire into the existing write path:**

- `PUT` / `CreateMultipartUpload` accept `x-amz-object-lock-mode`,
  `x-amz-object-lock-retain-until-date`, `x-amz-object-lock-legal-hold` and persist them onto
  the version being created. These currently 501 — that guard comes off as they land.
- **Bucket default retention applies on upload**: if the bucket has a default and the request
  carries no explicit lock headers, compute `retain_until = now() + duration` and store it.
  This is what makes Tier 1's config mean something.

Parse every body with `xml_helpers.parse_untrusted_xml`, never `etree.fromstring` — see the
P1 in PR #463's review.

Permission grading is **already correct** in `gateway/middlewares/acl.py`: `?retention` and
`?legal-hold` grade `READ_ACP`/`WRITE_ACP` and are in `BUCKET_PUT_SUBRESOURCES`. Do not regrade
them to `WRITE`, or "may upload" becomes "may make undeletable".

`x-amz-bypass-governance-retention` needs a real permission behind it. There is no IAM in this
codebase, so **bucket owner only** is the correct v1 — a delegated `WRITE_ACP` grantee must not
be able to bypass. Say so in the code, since it is a deliberate deviation.

---

## 7. Explicitly NOT building — keep these 501

Prune aggressively; none of these are needed for WORM to be real:

- S3 Batch Operations (`S3PutObjectLegalHold`, `S3PutObjectRetention` jobs).
- `s3:object-lock-remaining-retention-days` bucket-policy condition keys (no policy engine).
- `x-amz-bucket-object-lock-token` (the enable-on-existing-bucket confirmation gate).
- `x-amz-expected-bucket-owner`.
- Object Lock interaction with lifecycle expiration and replication.
- Changing a bucket's default retention retroactively — it applies to new uploads only, as in AWS.

Keep the existing `object_lock_guard` and simply narrow `_QUERY_SUBRESOURCES` as each surface
lands, so anything unbuilt still answers a clean 501.

---

## 8. Tests — the ones that actually prove it

Unit and integration are cheap; the two that matter are the last two.

1. Guard truth table: retention only / legal hold only / both / expired retention + hold /
   expired both. Parametrised, one assertion each.
2. `DELETE ?versionId` on a locked version → 403, **and no unpin was enqueued** (assert on the
   queue, not just the status code — the status is the symptom, the enqueue is the harm).
3. Simple `DELETE` on a locked version → 200 + delete marker + original version still readable.
4. Overwrite `PUT` on a locked key → 200, new version, old version still locked.
5. COMPLIANCE: shorten → 403; extend → 200. GOVERNANCE: shorten without bypass → 403; with
   bypass as owner → 200; with bypass as a WRITE_ACP grantee → 403.
6. `DeleteObjects` mixed batch: locked and unlocked keys in one call — unlocked deleted, locked
   reported as errors, response shape intact.
7. **End-to-end durability (the real proof):** upload → lock → delete → let the unpinner and
   the hard-delete ring run → assert the bytes are **still fetchable from Arion**. This is the
   test that would have caught Tier 1 shipping with no enforcement, and it is the one to write
   first.
8. **Cache eviction still works while locked:** lock an object, force FS-cache eviction, GET it
   again and confirm it refetches and serves. Proves we protected durability without pinning
   the cache — the explicit non-goal in §2(c).

---

## 9. Suggested order

1. Migration + guard function + unit truth table.
2. Data-layer gating (unpin enqueue, `find_objects_ready_for_hard_delete`) + test 7. **After
   this step the durability promise is real**, even with no new API surface.
3. `?retention` / `?legal-hold` GET and PUT + the delete-path 403/200 split (tests 2, 3, 5).
4. `PUT` headers and bucket-default application on upload (test 4).
5. `DeleteObjects` (test 6), ops-script guards, cache test 8.

Steps 1–2 are the ones worth having today. They are small, they are where the guarantee lives,
and they do not depend on any of the new API surface.

---

## 10. Open questions for whoever picks this up

- **Which account may bypass GOVERNANCE?** Recommended: bucket owner only. Needs a decision,
  and it is the only place where the absence of IAM forces a deviation from AWS.
- **What happens to a locked object when its account is deleted?** AWS's answer for COMPLIANCE
  is "delete the account" — `nuke_user.py` needs an explicit, deliberate policy.
- **Retention beyond the object's own lifetime** — does an expiring bucket lifecycle rule (if
  added later) win against a retention date? In AWS, no. Worth writing down before lifecycle
  work starts.
