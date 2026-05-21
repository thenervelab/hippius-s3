# Spec: S3 Object Lock

Status: Tier 0 implemented. Tier 1 and Tier 2 are designed and have contract tests, but
no implementation yet. Drives compatibility with `aws s3api put-object-lock-configuration`
and the surrounding Object Lock APIs from the AWS S3 surface.

---

## Goal

Provide enough Object Lock surface for backup tools and SDKs that *probe for support* to
get a deterministic, S3-compliant answer from hippius-s3. Eventually, support real WORM
semantics (retention + legal hold + delete-marker preservation) for regulated workloads.

Today's gateway *silently misroutes* `PUT /bucket?object-lock` to `CreateBucket` (returns
`409 BucketAlreadyExists`) and `GET /bucket?object-lock` to `ListObjects` (returns 200 +
an empty-looking `ObjectLockConfiguration{}` after SDK XML parsing). Both are misleading;
this spec replaces them with a proper `501 NotImplemented` first, then layers persistence
and enforcement.

---

## AWS surface inventory

### Bucket-level configuration (`?object-lock` subresource)

| API | HTTP | Description |
| --- | --- | --- |
| `PutObjectLockConfiguration` | `PUT /<bucket>?object-lock` | Enables Object Lock on the bucket and optionally sets a default `Rule` with `Mode ∈ {GOVERNANCE, COMPLIANCE}` and exactly one of `Days` / `Years`. |
| `GetObjectLockConfiguration` | `GET /<bucket>?object-lock` | Returns the configuration set above. |

Request body:

```xml
<ObjectLockConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <ObjectLockEnabled>Enabled</ObjectLockEnabled>           <!-- only valid value -->
  <Rule>
    <DefaultRetention>
      <Mode>GOVERNANCE | COMPLIANCE</Mode>
      <Days>N</Days>     <!-- XOR -->
      <Years>N</Years>   <!-- XOR -->
    </DefaultRetention>
  </Rule>
</ObjectLockConfiguration>
```

Notable headers:

- `x-amz-bucket-object-lock-token` — opaque token AWS requires when enabling Object Lock
  on a pre-existing bucket. Acts as an "explicit confirmation" gate.
- `x-amz-expected-bucket-owner` — optional ownership guard; 403 if mismatched.
- `Content-MD5`, `x-amz-sdk-checksum-algorithm` — integrity / SDK checksum metadata.

### Per-object lock state

| API | HTTP | Description |
| --- | --- | --- |
| `PutObjectRetention` | `PUT /<bucket>/<key>?retention[&versionId=…]` | Sets `Mode` + `RetainUntilDate` on a single object version. |
| `GetObjectRetention` | `GET /<bucket>/<key>?retention[&versionId=…]` | Reads it back. |
| `PutObjectLegalHold` | `PUT /<bucket>/<key>?legal-hold[&versionId=…]` | Sets `Status=ON | OFF`. |
| `GetObjectLegalHold` | `GET /<bucket>/<key>?legal-hold[&versionId=…]` | Reads it. |

### Request-side touchpoints sprinkled across regular S3 traffic

- `CreateBucket` accepts `x-amz-bucket-object-lock-enabled: true` so a bucket is born
  Object-Lock-eligible (and implicitly versioning-enabled). Note: the boto3 SDK exposes
  this as the `ObjectLockEnabledForBucket=True` parameter but the wire header drops the
  `-for-bucket` suffix.
- `PutObject` and `CreateMultipartUpload` (not `CompleteMultipartUpload`) accept
  `x-amz-object-lock-mode`, `x-amz-object-lock-retain-until-date`,
  `x-amz-object-lock-legal-hold` to set per-version locks at write time.
- `HeadObject` / `GetObject` echo those three headers on locked versions.
- `DeleteObject` (with `versionId`) and `PutObject` (overwrite of locked version) must
  refuse with `403 AccessDenied` if the version is locked. GOVERNANCE can be overridden
  with `x-amz-bypass-governance-retention: true` + the
  `s3:BypassGovernanceRetention` permission.
- `DeleteObject` (no `versionId`) on a locked object succeeds, but only inserts a delete
  marker; the locked version stays. Requires versioning.

---

## Concept explainer (for the user-facing docs that will eventually link here)

- **Retention period** — wall-clock expiry attached to a specific object version. The
  version cannot be deleted or overwritten until the clock runs out.
- **GOVERNANCE mode** — "locked, but admins can break the lock." Useful for
  accidental-deletion protection where a human escape hatch is acceptable. Override
  requires `s3:BypassGovernanceRetention` + `x-amz-bypass-governance-retention: true`.
- **COMPLIANCE mode** — "locked, period." Even the AWS root account cannot delete or
  shorten the retention. The only way to remove the data is to close the account. Use
  only when regulators demand it (SEC 17a-4, FINRA, CFTC).
- **Legal hold** — a separate boolean flag per version. "Locked until I say otherwise."
  Independent of retention; if either is active the version cannot be deleted.
- **Versioning is mandatory** — locks attach to versions, not keys. A `DELETE` on a
  locked key adds a delete marker (the key looks gone) without touching the locked
  version, so the data remains WORM-preserved.
- **Default rule** — convenience so callers don't repeat the same retention headers on
  every PUT. Per-PUT headers always win.

---

## Tiered implementation

### Tier 0 — clean `501 NotImplemented` (this PR)

Goal: stop misleading clients. No persistence, no enforcement. Every Object Lock-family
entry point returns a proper S3 `501` XML error.

Entry points covered:

- `PUT /<bucket>?object-lock` → 501.
- `GET /<bucket>?object-lock` → 501.
- `PUT /<bucket>/<key>?retention` → 501 (with or without `versionId`).
- `GET /<bucket>/<key>?retention` → 501.
- `PUT /<bucket>/<key>?legal-hold` → 501.
- `GET /<bucket>/<key>?legal-hold` → 501.
- `CreateBucket` with `x-amz-bucket-object-lock-enabled-for-bucket: true` → 501 (do not
  silently create a normal bucket).
- `PutObject` with any `x-amz-object-lock-*` header → 501.
- `CreateMultipartUpload` (`POST ?uploads`) with any `x-amz-object-lock-*` header → 501.
- `DELETE` with `x-amz-bypass-governance-retention: true` → ignored (header is harmless
  in the absence of locks; explicitly a no-op in Tier 0).

Error code: `NotImplemented`. Status: `501`. Message points the reader at this spec.

Risk: zero — only error-path changes.

### Tier 1 — persist & echo (future PR)

Goal: round-trip the configuration so backup tools that probe for Object Lock support
get a believable answer. Still no DELETE/PUT enforcement.

Changes (sketch — full design done when this tier is picked up):

- New migration: `ALTER TABLE buckets ADD COLUMN object_lock JSONB` (one nullable column
  storing `{"enabled": true, "mode": "GOVERNANCE", "days": 30}` or `null`).
- New endpoint file `bucket_object_lock_endpoint.py` modelled on
  `bucket_tagging_endpoint.py` — parse the request XML with `lxml` using the same
  namespace-aware XPath idiom, validate `Mode`, `Days XOR Years`, etc., write to
  `buckets.object_lock`, return 200. GET serialises back to `<ObjectLockConfiguration>`.
- Wire into `buckets/router.py` GET (around line 46) and PUT (delegate from
  `handle_create_bucket` like lifecycle does).
- Honour `x-amz-bucket-object-lock-enabled-for-bucket: true` on `CreateBucket` to
  pre-set the column.

Risk: low. No write-path or encryption changes. Reuses the tagging pattern verbatim.

### Tier 2 — real WORM enforcement (future epic)

Honest WORM semantics require S3 versioning, which hippius-s3 currently does not have.
Implementation scope:

1. **Bucket versioning** — `PutBucketVersioning`, `GetBucketVersioning`,
   `buckets.versioning_state ∈ {Unversioned, Enabled, Suspended}`.
2. **Public version IDs** — `object_versions` already carries per-version rows; surface
   them via a `versionId` query param that base64-encodes `(object_id, object_version)`.
3. **Delete markers** — new boolean column on `object_versions`; every read path filters
   them, `ListObjectVersions` surfaces them.
4. **Per-version lock columns** — `retention_mode TEXT`, `retain_until TIMESTAMPTZ`,
   `legal_hold BOOL` on `object_versions`. Populated from bucket default or from
   `x-amz-object-lock-*` headers on write.
5. **Enforcement hooks**:
   - `delete_object_endpoint.py` — refuse if the version is locked, unless GOVERNANCE +
     `x-amz-bypass-governance-retention: true` + caller has the permission scope.
   - `put_object_endpoint.py` — same on overwrite of a locked version.
   - `initiate_multipart_upload` / completion — apply lock headers or bucket default to
     the resulting version.
6. **New endpoints**: `object_retention_endpoint.py`, `object_legal_hold_endpoint.py`.
7. **Read-side**: `HeadObject` / `GetObject` emit the `x-amz-object-lock-*` response
   headers on locked versions.
8. **Janitor safety**: backend eviction must never delete bytes belonging to a locked
   version. The existing replication-gate in `workers/run_janitor_in_loop.py` is the
   right place to add a `retain_until > NOW() OR legal_hold` predicate.
9. **Permission model**: gateway needs `s3:BypassGovernanceRetention` semantics. The
   `sub_token_scope.py` matrix already has `object-lock` in `_BUCKET_META_SUBRESOURCES`
   but is dormant — turn it on, or use a master-token carve-out.

Rough effort: 2–4 engineering weeks, tightly coupled to a versioning effort.

---

## Test inventory

All tests live in this PR. Tier 0 must pass; Tier 1 and Tier 2 are
`@pytest.mark.xfail(strict=False, reason="…tier N… see specs/s3-object-lock.md")`.
The `strict=False` form lets the CI build stay green; an unexpected pass is informative
but not blocking.

### Tier 0 — must pass

Unit (`tests/unit/api/s3/test_object_lock_routing.py`):

- `test_put_bucket_object_lock_returns_501` — `PUT ?object-lock` returns 501 with
  `<Code>NotImplemented</Code>`.
- `test_get_bucket_object_lock_returns_501` — `GET ?object-lock` returns 501.
- `test_create_bucket_with_object_lock_header_returns_501` — `CreateBucket` with
  `x-amz-bucket-object-lock-enabled-for-bucket: true` returns 501.
- `test_put_object_with_lock_headers_returns_501` — three sub-cases, one per
  `x-amz-object-lock-*` header.
- `test_create_multipart_upload_with_lock_headers_returns_501` — same three sub-cases on
  the `?uploads` POST.
- `test_put_object_retention_returns_501` — both `?retention` and
  `?retention&versionId=…`.
- `test_get_object_retention_returns_501`.
- `test_put_object_legal_hold_returns_501`.
- `test_get_object_legal_hold_returns_501`.
- `test_delete_with_bypass_governance_header_is_noop` — header present, no lock state,
  DELETE succeeds (no behavior change).

E2E (`tests/e2e/test_BucketObjectLock.py`, `tests/e2e/test_ObjectRetention.py`,
`tests/e2e/test_ObjectLegalHold.py`):

- `test_put_object_lock_configuration_returns_not_implemented` — boto3
  `put_object_lock_configuration` raises `ClientError` with code `NotImplemented` and
  HTTP 501.
- `test_get_object_lock_configuration_returns_not_implemented`.
- `test_put_object_retention_returns_not_implemented`.
- `test_get_object_retention_returns_not_implemented`.
- `test_put_object_legal_hold_returns_not_implemented`.
- `test_get_object_legal_hold_returns_not_implemented`.
- `test_create_bucket_with_object_lock_enabled_header_returns_not_implemented` — verifies
  the bucket is NOT created (no bucket leftover after the failing call).

### Tier 1 — xfail until implemented

(`tests/unit/api/s3/test_object_lock_validation.py`,
`tests/e2e/test_BucketObjectLock.py` Tier 1 section)

Round-trip:

- PUT a valid config (Enabled + GOVERNANCE Days=30), GET it back, assert XML equivalence.
- PUT with COMPLIANCE Days=10 — round-trip.
- PUT with GOVERNANCE Years=1 — round-trip.
- PUT replaces previous config (idempotency).
- GET on a bucket that never had a config set → 404
  `ObjectLockConfigurationNotFoundError`.

Validation:

- `Mode` outside `{GOVERNANCE, COMPLIANCE}` → 400.
- Both `Days` and `Years` present → 400.
- Neither `Days` nor `Years` → 400.
- `Days` ≤ 0 or `Years` ≤ 0 → 400.
- `ObjectLockEnabled` ≠ `Enabled` → 400.
- Empty body → 400 `MalformedXML`.
- Non-XML body → 400.
- XML without S3 namespace → accepted (match tagging behavior).

CreateBucket interaction:

- `x-amz-bucket-object-lock-enabled-for-bucket: true` → bucket row has
  `object_lock.enabled=true`, no rule. Subsequent GET returns
  `<ObjectLockConfiguration><ObjectLockEnabled>Enabled</ObjectLockEnabled></ObjectLockConfiguration>`.

Auth:

- Non-owner → 403 (depends on existing ACL middleware; this test just asserts the
  standard hippius-s3 behavior).

### Tier 2 — xfail until implemented

(`tests/e2e/test_ObjectRetention.py`, `tests/e2e/test_ObjectLegalHold.py`,
`tests/unit/test_janitor_object_lock.py`)

Enforcement:

- PUT object with `x-amz-object-lock-mode: COMPLIANCE` +
  `x-amz-object-lock-retain-until-date: <now+1d>`:
  - HeadObject returns those headers back.
  - DELETE `?versionId=…` → 403 AccessDenied.
  - DELETE (no versionId) → 200 with delete marker; locked version still readable via
    versionId.
  - Overwrite (PUT same key, new body) → new version reserved; old locked version
    intact.
- GOVERNANCE + bypass header + permission → DELETE succeeds.
- GOVERNANCE + bypass header without permission → 403.
- COMPLIANCE + bypass header → 403 regardless of permission.
- Retention expired → DELETE succeeds.
- Legal hold blocks DELETE regardless of retention.
- Legal hold + expired retention → still refused.
- Remove legal hold → DELETE succeeds.
- Extending retention allowed; shortening in COMPLIANCE refused; shortening in
  GOVERNANCE allowed with bypass.

Per-object endpoints round-trip:

- `PutObjectRetention` / `GetObjectRetention`.
- `PutObjectLegalHold` / `GetObjectLegalHold`.

Default bucket retention:

- Set bucket default GOVERNANCE Days=7. PUT object without explicit headers → version is
  locked with `Retain-Until = creation + 7d`.
- Explicit per-object headers override the bucket default.

Versioning prerequisite:

- Bucket without versioning → `PutObjectLockConfiguration` with `Enabled` →
  `InvalidBucketState`.
- `PutBucketVersioning` `Suspended` on an Object-Lock-enabled bucket →
  `InvalidBucketState`.

Janitor safety (unit):

- Janitor's deletable-set never includes a chunk belonging to a locked version, even
  under critical disk pressure. Mirrors the replication-gate pattern in
  `tests/unit/test_janitor_*.py`.

---

## Merge criteria

- Tier 0 tests green in CI.
- Tier 1 and Tier 2 tests behave as declared (`xfail strict=False`).
- `docs/s3-compatibility.md` updated to mark the Object Lock family rows as
  "Not supported (501 NotImplemented), see specs/s3-object-lock.md".
- Manual probe against staging: `aws s3api put-object-lock-configuration` returns 501
  with `<Code>NotImplemented</Code>` instead of the current 409.

---

## Out of scope

- `s3:object-lock-remaining-retention-days` condition keys (bucket-policy-level
  retention bounds).
- IAM-style `s3:BypassGovernanceRetention` modelling — defer to Tier 2.
- Cross-region replication interaction with Object Lock.
- AWS-specific quirks around the `x-amz-bucket-object-lock-token` enablement gate; we'll
  accept any token (or none) in Tier 1.
