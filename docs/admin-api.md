# Admin account API (suspend / reactivate / status / purge)

Account-level enforcement endpoints for the billing backend and staff cockpit
(issue #421). All endpoints live under `/admin/` and are HMAC-gated at the gateway
with a **dedicated secret** (`HIPPIUS_ADMIN_HMAC_SECRET`) — deliberately separate from
`FRONTEND_HMAC_SECRET` because this surface can suspend and destroy whole accounts.
An empty/unset secret disables the admin API entirely (every `/admin/*` request 403s).

`account_id` is always the user's substrate SS58 address — the same identifier used in
sub-token scope payloads. One suspension covers **every** credential of the account:
master token, all sub-tokens, presigned URLs, and bearer auth.

## Signing

Same scheme as the sub-token scope endpoints, different secret:

```
X-HMAC-Signature: hex(HMAC-SHA256(HIPPIUS_ADMIN_HMAC_SECRET, METHOD + PATH[ + "?" + QUERY]))
```

The target account travels in the signed path — never in the (unsigned) body.

```python
import hashlib, hmac
sig = hmac.new(secret.encode(), f"POST/admin/accounts/{ss58}/suspend".encode(), hashlib.sha256).hexdigest()
```

## Endpoints

### `POST /admin/accounts/{account_id}/suspend`

Body (optional): `{"mode": "full" | "read_only"}` — default `full`.

- `full`: blocks all access, including anonymous reads of the account's public buckets.
- `read_only`: allows downloads (GET/HEAD, ListObjects, ListBuckets) but blocks
  anything the ACL matrix classifies as a write — PUT, DELETE, `POST ?delete`,
  MPU operations, `PUT ?acl`, etc. Cross-account writes into the owner's buckets are
  blocked too, so the account's stored data cannot keep growing.

Idempotent (mode changes just update). Returns `{"account_id", "state"}` where state is
`suspended` (full) or `read_only`. Takes effect on every gateway pod immediately
(write-through Redis cache); in-flight streaming requests already past the gate finish.

### `POST /admin/accounts/{account_id}/reactivate`

Lifts the suspension. **Sub-token scopes are untouched by suspension and resume exactly
as they were — the backend does not need to re-push them.** Idempotent: returns
`{"state": "active"}` even if the account was not suspended. Returns **409
PurgeInProgress** while a purge job is queued/running.

### `GET /admin/accounts/{account_id}/status`

→ `{"account_id", "state": "active"|"suspended"|"read_only", "buckets": n|null, "bytes": n|null}`

`bytes` is logical size across current object versions. On very large accounts the
aggregate is bounded by a query timeout and degrades to `null` rather than failing.

### `DELETE /admin/accounts/{account_id}/data`

Async purge of ALL the account's buckets + objects. Returns `202 {"job_id"}`
immediately. Implies a `full` suspension (upserted before the job row), and
reactivation is blocked until the job finishes. Idempotent: while a job is
queued/running, repeat calls return the same `job_id`.

The purge worker drives the existing delete pipeline in batches: soft-delete objects →
enqueue real unpin requests (throttled against unpin-queue depth) → soft-delete
buckets → delete the account's `sub_token_scopes` rows. It sweeps repeatedly until a
full pass finds nothing, so writes racing the suspension cannot survive.

### `GET /admin/purge-jobs/{job_id}`

→ `{"job_id", "account_id", "state": "queued"|"running"|"done"|"failed", "deleted_objects": n, "deleted_bytes": n, "error": str|null}`

**Semantics of `done`:** all rows are soft-deleted and every unpin request is enqueued.
`deleted_bytes` is *logical* bytes purged (sum of `object_versions.size_bytes` across
all versions). Physical backend deletion (unpinner) and disk reclaim (janitor) continue
asynchronously after `done`. A `failed` job is safe to retry by issuing
`DELETE /admin/accounts/{account_id}/data` again — soft-deletes are idempotent and the
new job resumes where data remains.

## Operational notes

- Suspension state: `account_suspensions` table (row present = suspended), cached 30s
  in Redis (`hippius_suspension:{ss58}`) with write-through from the endpoints.
- Purge jobs: `purge_jobs` table; the `purger` worker (single replica,
  `workers/run_purger_in_loop.py`) claims jobs with `FOR UPDATE SKIP LOCKED` and
  reclaims stale leases (`HIPPIUS_PURGER_LEASE_SECONDS`, default 600s) after a crash.
- Backpressure: the purger parks while any `{backend}_unpin_requests` list exceeds
  `HIPPIUS_PURGER_UNPIN_QUEUE_HIGH_WATER` (default 50k) — a mass purge must never
  degrade redis-queues (1.29M queued unpins once broke prod GETs).
- Browser-cached public objects (30-day max-age) are beyond revocation; ATS-cached
  objects ARE gated, because every ATS cache hit re-authorizes through the gateway.
