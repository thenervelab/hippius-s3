# Admin account endpoints — how to call them (issue #421)

These are the staff-only endpoints for suspending, reactivating, and purging S3 accounts.
This doc is written for the backend/cockpit integrator: it shows how to sign a request and
gives a copy-paste `curl` for every endpoint.

- **Base URL:** `https://s3.hippius.com` (prod). Same host as normal S3 traffic.
- **`account_id`:** the user's substrate SS58 address — the same identifier you already send
  in the sub-token scope payloads.
- **Not in Swagger:** these endpoints are intentionally hidden from `/docs` and `openapi.json`.

## Authentication

Every `/admin/*` request must carry an `X-HMAC-Signature` header. It's the hex HMAC-SHA256 of
the request line, using a **dedicated admin secret** (separate from `FRONTEND_HMAC_SECRET`):

```
signature = hex( HMAC_SHA256( ADMIN_SECRET, METHOD + PATH [ + "?" + QUERY ] ) )
```

- Sign `METHOD` + `PATH` exactly as sent. Append `?` + the raw query string **only if there is one**.
- The request **body is not signed** — that's why the target account and all parameters travel
  in the **path**, never the body. (The `suspend` mode is the one small exception; see its note.)
- There is no timestamp/nonce yet, so treat a signed URL as a secret and send it over HTTPS only.

The secret is provisioned as the `HIPPIUS_ADMIN_HMAC_SECRET` environment variable on the gateway
(and API) pods, from the `hippius-s3-secrets` k8s secret. If it is unset, the entire admin API
returns `403 {"detail":"Admin API is not enabled"}` — fail-closed by design.

Throughout this doc the placeholder secret is:

```
ADMIN_SECRET="replace-with-real-admin-secret"
ACCOUNT="5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty"
BASE="https://s3.hippius.com"
```

### Signing helper (bash)

```bash
sign() {  # sign METHOD PATH_WITH_QUERY
  printf '%s' "$1$2" | openssl dgst -sha256 -hmac "$ADMIN_SECRET" -r | cut -d' ' -f1
}
```

### Signing helper (Python)

```python
import hashlib, hmac

def sign(method: str, path_with_query: str, secret: str) -> str:
    return hmac.new(secret.encode(), (method + path_with_query).encode(), hashlib.sha256).hexdigest()
```

---

## 1. Suspend an account

Blocks **all** access for every credential of the account (master token, all sub-tokens,
presigned URLs, bearer), regardless of installed scopes. Idempotent.

`POST /admin/accounts/{account_id}/suspend`

Optional JSON body `{"mode": "full" | "read_only"}` (default `full`):
- `full` — blocks everything, including anonymous reads of the account's public buckets.
- `read_only` — allows downloads (GET/HEAD, list) but blocks every write (PUT, DELETE,
  multipart, multi-delete, ACL writes).

> Note: `mode` rides in the body and is **not** covered by the signature. If you need a
> guaranteed-`full` suspension with no reliance on body integrity, omit the body entirely —
> the default is `full`. Send a body only when you specifically want `read_only`.

```bash
PATH="/admin/accounts/$ACCOUNT/suspend"
curl -sS -X POST "$BASE$PATH" \
  -H "X-HMAC-Signature: $(sign POST "$PATH")" \
  -H "Content-Type: application/json" \
  -d '{"mode":"full"}'
# -> {"account_id":"5FHneW...","state":"suspended"}
```

Read-only variant:

```bash
curl -sS -X POST "$BASE$PATH" \
  -H "X-HMAC-Signature: $(sign POST "$PATH")" \
  -H "Content-Type: application/json" \
  -d '{"mode":"read_only"}'
# -> {"account_id":"5FHneW...","state":"read_only"}
```

## 2. Reactivate an account

Lifts the suspension. Existing sub-token scopes were never touched and resume as they were —
no re-push needed. Idempotent (returns `active` even if it wasn't suspended). Returns
**409** if a purge job is queued/running for the account.

`POST /admin/accounts/{account_id}/reactivate`

```bash
PATH="/admin/accounts/$ACCOUNT/reactivate"
curl -sS -X POST "$BASE$PATH" \
  -H "X-HMAC-Signature: $(sign POST "$PATH")"
# -> {"account_id":"5FHneW...","state":"active"}
```

## 3. Get account status

Authoritative suspension state plus storage stats for the cockpit. On very large accounts the
stats can time out and come back as `null` (the state is always returned).

`GET /admin/accounts/{account_id}/status`

```bash
PATH="/admin/accounts/$ACCOUNT/status"
curl -sS "$BASE$PATH" \
  -H "X-HMAC-Signature: $(sign GET "$PATH")"
# -> {"account_id":"5FHneW...","state":"active","buckets":12,"bytes":48210233021}
```

## 4. Purge all account data (async)

Server-side deletion of **all** the account's buckets and objects. Returns immediately with a
`job_id`. Implies a `full` suspension (applied before the job starts), and reactivation is
blocked until the job finishes. Idempotent: while a job is queued/running, repeat calls return
the **same** `job_id`.

`DELETE /admin/accounts/{account_id}/data`

```bash
PATH="/admin/accounts/$ACCOUNT/data"
curl -sS -X DELETE "$BASE$PATH" \
  -H "X-HMAC-Signature: $(sign DELETE "$PATH")"
# -> 202 {"job_id":"7b3f...-...."}
```

## 5. Poll a purge job

`GET /admin/purge-jobs/{job_id}`

```bash
JOB="7b3f0000-0000-0000-0000-000000000000"
PATH="/admin/purge-jobs/$JOB"
curl -sS "$BASE$PATH" \
  -H "X-HMAC-Signature: $(sign GET "$PATH")"
# -> {"job_id":"7b3f...","account_id":"5FHneW...","state":"running",
#     "deleted_objects":18402,"deleted_bytes":90322113,"error":null}
```

`state` is one of `queued | running | done | failed`.

**What `done` means:** every row has been soft-deleted and every backend-unpin request has been
enqueued. `deleted_bytes` is *logical* bytes purged (sum of object-version sizes). The physical
delete from the storage backend and the disk-space reclaim happen asynchronously afterward
(they're handled by the unpinner and janitor). A `failed` job is safe to retry — just issue the
`DELETE .../data` again; soft-deletes are idempotent and the job resumes where data remains.

---

## Quick reference

| Method | Path | Purpose |
|---|---|---|
| POST | `/admin/accounts/{id}/suspend` | Block all (or read-only) access |
| POST | `/admin/accounts/{id}/reactivate` | Lift suspension (409 during purge) |
| GET | `/admin/accounts/{id}/status` | State + bucket/byte counts |
| DELETE | `/admin/accounts/{id}/data` | Start async purge → `202 {job_id}` |
| GET | `/admin/purge-jobs/{job_id}` | Poll purge progress |

Error responses are JSON `{"detail": {"code": ..., "message": ...}}` (or `{"detail": "..."}` for
the auth layer). Common ones: `401` missing signature, `403` bad signature / admin API disabled,
`400` malformed SS58 or job id, `404` unknown job, `409` reactivate during purge.
