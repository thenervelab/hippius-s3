# Spec: S3 Object Lock

Status: Tier 0 and Tier 1 implemented. Tier 2 is partly implemented — per-object retention and
legal hold are persisted, and locked versions are refused by the delete endpoints, the unpin
resolution query and the janitor's hard-delete ring. The open items are tracked in
`s3-object-lock-tier2-handoff.md`. Drives compatibility with
`aws s3api put-object-lock-configuration` and the surrounding Object Lock APIs from the
AWS S3 surface.

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

### Tier 1 — persist & echo (IMPLEMENTED)

Goal: round-trip the configuration so backup tools that probe for Object Lock support
get a believable answer. Still no DELETE/PUT enforcement.

What shipped:

- Migration `20260521000000_add_buckets_object_lock.sql`: `ALTER TABLE buckets ADD COLUMN
  object_lock JSONB`. Nullable; stores `{"enabled": true}` or
  `{"enabled": true, "mode": "GOVERNANCE", "days": 30}` (or `"years": N`), or `NULL` when
  never configured.
- `hippius_s3/api/s3/buckets/bucket_object_lock_endpoint.py` — `handle_get_bucket_object_lock`
  and `handle_put_bucket_object_lock`. PUT parses the `ObjectLockConfiguration` XML
  (namespace-tolerant), validates `ObjectLockEnabled == Enabled`, `Mode ∈ {GOVERNANCE,
  COMPLIANCE}`, exactly one of `Days`/`Years`, and a positive period; writes the
  normalised dict via `update_bucket_object_lock.sql`. GET serialises the stored config
  back to XML, or returns 404 `ObjectLockConfigurationNotFoundError` when the column is
  NULL or `enabled` is falsy.
- `buckets/router.py` routes GET/PUT `?object-lock` to the new endpoint.
- `bucket_create_endpoint.py` honours `x-amz-bucket-object-lock-enabled: true` and writes
  `{"enabled": true}` transactionally at creation time. (Wire header — boto3's
  `ObjectLockEnabledForBucket=True` maps to it.)
- The guard (`object_lock_guard.py`) no longer trips on `?object-lock` or
  `x-amz-bucket-object-lock-enabled`; it still 501s the Tier 2 per-object surface.
- The two bucket-lookup queries (`get_bucket_by_name`, `get_bucket_by_name_and_owner`)
  now SELECT the `object_lock` column.

Storage schema in `buckets.object_lock` JSONB:

```
{"enabled": true}                                  # born with x-amz-bucket-object-lock-enabled
{"enabled": true, "mode": "GOVERNANCE", "days": 30}
{"enabled": true, "mode": "COMPLIANCE", "years": 1}
null                                               # never configured
```

Risk: low. No write-path or encryption changes; reuses the tagging pattern.

Tier 1 simplifications vs AWS (documented, acceptable for the backup-probe use case):
- The `x-amz-bucket-object-lock-token` enablement gate is not enforced — a `PUT
  ?object-lock` succeeds on any bucket, not only those born lock-enabled.
- No versioning prerequisite is enforced (that lands in Tier 2).

### Tier 2 — real WORM enforcement (future epic)

Honest WORM semantics require S3 versioning, which shipped separately.
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

## Write-once keys, DeleteBucket and the account lifecycle

Added for the VM-backup use case: a platform writes COMPLIANCE-locked backups into a
customer-owned bucket with a sub-token, and a stolen key must not be able to erase them.

### The `object_read_write_no_delete` sub-token tier (IMPLEMENTED)

Set through `PUT /user/sub-tokens/{access_key_id}/scope` like any other tier
(`gateway/services/sub_token_scope.py` holds the matrix).

| Allowed | Refused |
| --- | --- |
| PutObject, CopyObject (the source must be in scope for reads) | DeleteObject without a concrete `versionId` (absent, empty, `null`, malformed); DeleteObjects (`POST ?delete`) |
| `DELETE ?versionId=<N>`: permanent delete of one named version, refused by Object Lock while retained | any delete carrying `x-amz-bypass-governance-retention: true` |
| CreateMultipartUpload, UploadPart, CompleteMultipartUpload, ListParts | `PUT ?retention`, `PUT ?legal-hold` |
| AbortMultipartUpload of an upload that has not completed | `PUT ?acl`, `PUT ?tagging`, `DELETE ?tagging`, and any write carrying `x-amz-acl` / `x-amz-grant-*` |
| GetObject, HeadObject, ListObjects(V2), ListObjectVersions, GET `?retention` / `?legal-hold` | every bucket-level write, DeleteBucket, ListBuckets |

The tier is a **ceiling**. It also caps a request that another account's bucket ACL authorises:
when a sub-token with a scope row acts on someone else's bucket, the owner's grants decide, but
never beyond the token's tier. Before this change, cross-account requests skipped the tier
entirely. A token with no scope row keeps the plain contractor behaviour, where the grants alone
decide.

Changes that came with the tier, each of which was a way around it:

- **Pruning is a version delete, and only that.** `DELETE ?versionId=<N>` maps to its own op,
  `delete_object_version`, held by `admin_read_write`, `object_read_write` and this tier. It can
  hide nothing (no delete marker), and Object Lock refuses it while the version is retained, so
  a backup writer can prune whole chains once their lock has expired and not before. Only a
  concrete numeric id counts: the handler reads an empty or `null` id as "the current version"
  and would write a marker, so those stay `delete_object`. What a stolen key CAN destroy is
  therefore exactly the versions no lock protects — on a bucket where every write is locked,
  only expired backups.
- **A governance bypass needs `admin_read_write`, on every tier.** A delete (or DeleteObjects)
  carrying `x-amz-bypass-governance-retention: true` maps to `write_object_lock`. A sub-token
  of the bucket owner's account counts as the owner for the bypass, so without this any
  delete-capable key, this tier's version delete included, could remove a GOVERNANCE-retained
  version. AWS gates it behind `s3:BypassGovernanceRetention` for the same reason.
- **Changing a lock needs `admin_read_write`, on every tier.** `?retention` and `?legal-hold`
  writes map to a separate op, `write_object_lock`, which only `admin_read_write` holds. Before
  this change, sub-tokens graded them as ordinary object writes, so `object_read_write` could lift
  a legal hold. The bucket-ACL path already graded them `WRITE_ACP`.
- **Object ACL and tag writes are a separate op, `write_object_meta`.** It is held by
  `admin_read_write` and `object_read_write`, so their behaviour is unchanged. An object ACL can
  grant WRITE, and so delete, to a second key; a tag write replaces the whole set.
- **Abort is graded as a write, and can only abort.** AbortMultipartUpload now answers
  `NoSuchUpload` for a completed upload, or for an upload addressed through a bucket or key other
  than its own. Before, aborting a completed upload deleted its `multipart_uploads` row; `parts`
  cascades from that row, so the committed version's data went with it, past Object Lock.
  - The abort first claims the upload (`claim_upload_for_abort.sql`) and only then cleans
    anything up. The claim deletes only a row that is still open (`is_completed = FALSE`; NULL
    fails closed) and has no serveable version behind it. A simple PUT or streaming CopyObject
    commits its finished version with an open upload row and flips the row a moment later; before
    this change, an abort in that window deleted a finished, possibly locked, object.
  - The abandoned-upload reaper also claims first, so an upload that completes after being
    listed keeps its replication rows.
  - CompleteMultipartUpload flips `is_completed` under the same condition and rolls back if the
    abort won. The two serialise on the upload row.
  - UploadPart and CompleteMultipartUpload check the path the same way.
- **S4 append refuses a locked version** with 403. An append rewrites the current version's size,
  ETag and parts in place, so on a locked version it changed retained data. It is checked under
  both of the append's row locks.
- **Copy sources are parsed exactly as the handlers parse them**: cut the query off the raw
  header, then decode, then split. The sub-token check used to split before decoding, so a
  `%2F`-encoded source skipped it. The general ACL check used to decode before cutting, so
  `allowed%3Fsecret` was authorised as key `allowed`.
- **A scope that cannot be read denies**, cross-account too. Before, a failed read counted as "no
  scope row", and cross-account that means the grants alone decide.
- **A scope change invalidates the cache after it commits**, not concurrently with it. A downgrade
  can still take up to the 60 s cache TTL to apply: a lookup that read the old row just before the
  commit can write it back into the cache after the invalidation.
- **UploadPart writes into its upload's own version**, found from the upload's earlier parts, and
  ListParts lists that version too. Before, both used the key's current version. So after a PUT on
  the same key, a resumed upload wrote its parts into the PUT's finished object, in place. For the
  first part there is no earlier part to go by, and the current version is still the stand-in; if
  that version already holds finished data, UploadPart refuses with 409 rather than write into it.

Known gaps, not fixed here:

- **UploadPart racing CompleteMultipartUpload.** An UploadPart already streaming when Complete
  commits still republishes that part: its bytes in the cache, then its `parts` and `part_chunks`
  rows. So it can change what a completed, even Object-Locked, version serves and replicates. The
  checks at the start of UploadPart cannot see a completion that lands mid-stream. Closing it
  needs part publishes to hold a lock on the upload row that Complete also takes, before Complete
  reads the parts.
- **The first part of an upload has no reliable version.** `multipart_uploads` does not record the
  version that initiate reserved. When the current version is another write's reserved row that
  is still streaming, the refusal above cannot tell it apart from the upload's own. The fix is a
  version column on `multipart_uploads`, written at initiate.

What the tier cannot express:

- **Write-time lock headers.** PutObject and CreateMultipartUpload may carry
  `x-amz-object-lock-*`. A lock only protects the version being created, which is what a backup
  writer needs. It also means any write-capable key can create a version that nobody can delete
  until the lock ends, at most `object_lock_max_retention_days` (3650 days). AWS gates these
  headers behind `s3:PutObjectRetention`, and a legal-hold header behind `s3:PutObjectLegalHold`.
  **Open decision:** cap write-time retention per bucket or per tier?
- **Unversioned buckets.** An overwrite hides the previous version from GET without a
  `versionId` and from every listing. It stays readable by an explicit `versionId`, which the
  client has to already know. The tier protects history from a stolen key only on a versioned
  bucket, and every lock-enabled bucket is versioned.
- **Unlocked versions are prunable.** The version delete does not know whether the writer
  meant a version to be kept; only a lock says so. A write-once key on a bucket without a
  default retention can permanently delete any version written without lock headers.

### Presigned requests carry the lock in signed headers (VERIFIED)

A SigV4 presigned URL can list `x-amz-object-lock-mode` and `x-amz-object-lock-retain-until-date`
in `X-Amz-SignedHeaders`. The verifier folds them into the canonical request, so whoever holds the
URL cannot drop them or change them. PutObject and CreateMultipartUpload then apply the lock from
those headers, and explicit headers override the bucket default. UploadPart verifies the headers
and ignores them, as AWS does. **CompleteMultipartUpload answers 501 when it carries them**,
because the lock was fixed at initiate. A presigning writer must therefore sign them on PutObject
and CreateMultipartUpload only. `tests/unit/gateway/test_presigned_object_lock_headers.py` signs
with botocore's real `S3SigV4QueryAuth` and verifies with the real canonicalisation code.

### DeleteBucket counts every version (IMPLEMENTED)

`bucket_emptiness.sql` replaced the `list_objects` probe. That probe saw only keys whose newest
version is live content, so a versioned bucket whose keys were all hidden behind delete markers
read as empty and could be deleted, which orphaned every non-current version under it.

A bucket is now non-empty while either of these holds:

- a live object has a live completed version or a delete marker. That is AWS's rule: versions AND
  delete markers must all be gone.
- a multipart upload is open.

A reserved row with no data is not counted, even a locked one: an aborted upload leaves one on a
new key, and no client can see it or delete it.

The check and the soft-delete run in one transaction that first takes `FOR UPDATE` on the bucket
row. An in-flight create holds `FOR KEY SHARE` on that row through its foreign key, so the check
waits for it, then sees it.

**Residual:** a write that resolved the bucket before the delete committed, and inserts after it,
still lands in the soft-deleted bucket, because the write path does not re-check
`buckets.deleted_at`. A simple PUT that is still streaming is one such write: its reserved
version is not data yet, and it has no upload row until its last transaction. Closing this means
re-checking bucket liveness under the bucket row lock in every write tail.

### Billing of retained versions (CURRENT STATE, decision needed)

`bucket_storage_usage` counts only the **current** version of each live object
(`20260910120000_storage_usage_rollup.sql`: `ov.object_version = o.current_object_version`). So
these are retained on the backends but **not billed**:

- non-current versions in a versioned bucket, locked or not;
- a locked version whose key sits behind a delete marker;
- locked versions under a soft-deleted object or a purged account.

AWS bills every stored version. Counting them means reworking the five ledger triggers, so that
"which rows count" becomes "every live data version" rather than "the current one". It also
changes the bill of every versioned bucket, not only locked ones. **This is a pricing decision,
so it is not made here.** Until it is made, COMPLIANCE retention on a key that is overwritten or
marker-deleted costs the customer nothing.

The gap is also a lever: an owner can put a delete marker over a locked backup (a plain DELETE,
which Object Lock allows) and stop paying for bytes we are obliged to keep until the lock ends.

**Proposal** (nothing in this PR changes billing):

1. **Minimum:** bill every version that is under an active retention or legal hold, current or
   not — in particular a locked version hidden behind a delete marker. This closes the lever
   without touching the bill of unlocked versioned buckets. The ledger triggers would add a
   version to usage when it gains a lock or loses current-ness while locked, and remove it when
   the lock ends or the version is deleted; the lock expiring is a time event, so it needs a
   periodic sweep, not only a trigger.
2. **Full AWS parity, later:** bill every live data version. Needs a customer-facing notice,
   since every versioned bucket's bill goes up.
3. Whichever is chosen, locked versions under a soft-deleted object or a purged account follow
   the same rule (see the next section), so a purge cannot be used to stop paying either.

### COMPLIANCE across suspension, purge and account deletion (PROPOSED — not implemented)

What the code does today:

| Event | Locked bytes | Billing | Writes into the bucket |
| --- | --- | --- | --- |
| Suspension `read_only` | kept | unchanged (current versions only) | refused for every key of the owner's account, including a platform-held sub-token (`suspension_middleware`), and for other accounts (`acl.py` owner-suspension check) |
| Suspension `full` | kept | unchanged | all access refused |
| Purge job (`workers/purger.py`) | kept: the unpin resolution and hard-delete SQL gates skip locked versions, but the objects and buckets are soft-deleted around them | drops to 0 (soft-deleted objects stop counting) | n/a |
| `scripts/nuke_user.py` | **destroyed**: it unpins every CID of the account directly and `DELETE FROM users` cascades, with no lock check | gone | n/a |

Proposed policy, pending an explicit decision:

1. **Suspension never touches locked data.** This is already true.
2. **A purge keeps locked versions until their retain-until, and they stay billed.** This needs
   the billing change above, or a dedicated "retained after purge" counter. Once the last lock
   ends, the purger (or a follow-up sweep) completes the purge.
3. **`nuke_user.py` refuses an account that holds any locked version** unless it is given an
   explicit `--i-know-this-breaks-worm`, as handoff §5 #7 already asks. AWS's own answer is that
   closing the account is the only way to remove COMPLIANCE data, so allowing it is defensible,
   but it must be deliberate, not a side effect.
4. **Platform writes into a suspended owner's bucket.** Today they are refused like any other
   write. For backups that is arguably right: a suspended customer stops accruing new storage.
   The alternative is to exempt sub-tokens the platform holds. They belong to the customer's
   account, so that needs a marker on the scope row, not an account allowlist. **Open decision.**

---

## Test inventory

Tier 0 and Tier 1 tests must pass. Tier 2 tests are
`@pytest.mark.xfail(strict=False, reason="…tier 2… see specs/s3-object-lock.md")`.
The `strict=False` form lets the CI build stay green; an unexpected pass is informative
but not blocking.

Tier 0 / Tier 1 live in `tests/e2e/test_BucketObjectLock.py` (bucket surface),
`tests/e2e/test_ObjectRetention.py` and `tests/e2e/test_ObjectLegalHold.py` (per-object
surface still 501), and `tests/unit/api/s3/test_object_lock_guard.py` (guard unit
tests).

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
