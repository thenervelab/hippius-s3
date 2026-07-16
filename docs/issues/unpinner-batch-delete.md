# Unpinner batch delete via HCFS `POST /delete_files`

## Summary

The arion-unpinner deletes one chunk per HTTP call (`DELETE /delete/{ss58}/{file_id}`). At the
observed ~2s/call ceiling this cannot drain a 3.7M-file backlog for a single account in any
reasonable time. HCFS shipped a batch endpoint (`POST /delete_files`, up to 1000 file deletes per
call). This change reworks the unpinner to use it, **feature-flagged OFF by default**, with a
transparent per-file fallback so an old HCFS server (or a disabled flag) behaves exactly as today.

## The HCFS contract (treat as spec)

- Endpoint: `POST {arion_base}/delete_files`, same auth headers as the per-file delete
  (`X-API-Key`, `Authorization: Bearer …`, `X-Hippius-Bypass-Rate-Limiting`).
- Request JSON:
  ```json
  {"ss58_address": "5F..", "folder_hash": "<16-hex>", "file_ids": ["<64-hex path_hash>", ...≤1000], "quiet": false}
  ```
- Response is **HTTP 200 even on partial failure**:
  ```json
  {"Success": {
    "deleted": [{"file_id": "..", "status": "deleted" | "already_deleted"}],
    "errors":  [{"file_id": "..", "code": "invalid_file_id", "message": ".."}],
    "files_deleted": <int>
  }}
  ```
- Semantics:
  - Per-item `status` is mandatory. `already_deleted` is an **idempotent success** (mirrors the
    current 404-on-single-delete handling).
  - Partial failure → HTTP 200 with a populated `errors[]`.
  - In non-quiet mode `deleted ∪ errors` covers **every** id sent.
  - Whole-request failures are **non-200**: `400` (`batch_too_large` if >1000, or malformed),
    `401` (auth), `500` (DB).
  - The endpoint may `404` on an old server → the client must fall back to per-file.

## Open question / MERGE-BLOCKER: `folder_hash`

Our S3 objects are uploaded to HCFS with **no folder** (`POST /upload` sends only `account_ss58`),
and the current single-delete response returns `folder_hash: ""` (empty/root). We have **no
per-file folder_hash** in our schema. So:

- `folder_hash` defaults to `""` (empty = root), configurable via `HIPPIUS_ARION_FOLDER_HASH`.
- Batches are grouped by `(ss58, folder_hash)`; since `folder_hash` is a single config value it is
  effectively constant, so in practice all of one account's requests coalesce into one group.

**Before enabling in prod, HCFS MUST confirm `POST /delete_files` accepts an empty `folder_hash`
for our root-folder objects.** We deliberately do NOT invent a fake folder_hash. This is called out
in code (`ArionClient.unpin_files_batch` docstring, config comment) and is the gate on the rollout
plan below.

## Design: batch-of-requests

When `HIPPIUS_UNPINNER_BATCH_DELETE=true`:

1. **Assemble.** The loop dequeues one request (blocking), then non-blockingly drains more (short
   brpop timeout + a small wall-clock window), fetching each request's `chunk_backend` identifiers
   and grouping by `(address, folder_hash)`. A request with no rows keeps the existing
   retry-then-drop-after-6 behavior and is **not** batched.
2. **Coalesce.** Within a group we build `file_id → set(chunk_id)` (a `backend_identifier`/`file_id`
   can map to multiple chunk_ids and be contributed by multiple requests) and `request → set(file_id)`.
   The batch payload is the **deduped** set of file_ids.
3. **Delete.** `unpin_files_batch(deduped_file_ids, ss58, folder_hash)`, split into `≤1000` sub-batches
   (hard cap; the configured `HIPPIUS_UNPINNER_BATCH_MAX_FILES` is clamped down to 1000). Concurrent
   in-flight batches are bounded by `HIPPIUS_UNPINNER_MAX_INFLIGHT`; concurrent Arion calls by the
   shared per-pod `HIPPIUS_UNPINNER_PARALLELISM` semaphore (reused as-is).
4. **Fan-out (invariant A9 preserved).**
   - `deleted` / `already_deleted` item → soft-delete **all** of that file_id's chunk_ids, but only
     mark the file_id "cleared" if **every** soft-delete DB write succeeded.
   - `errors[]` item → **never** soft-delete its chunk_ids.
   - a sent file_id **absent from both** `deleted` and `errors` → treated as a **failure**
     (defensive; never soft-delete silently).
5. **Per-request routing.** A request is acked iff **all** its file_ids cleared both the backend
   delete and the soft-delete. Otherwise it is routed exactly as today:
   - permanent per-item code (`invalid_file_id` → classified permanent) dominates → **DLQ**;
   - otherwise (transient per-item error / missing id / soft-delete DB failure) → **retry** with
     backoff (or DLQ once `HIPPIUS_UNPINNER_MAX_ATTEMPTS` is exhausted).
   Requests whose file_ids all succeeded are **unaffected** by sibling requests' failures.
6. **Whole-batch failure** (non-200 500/timeout/network, after `retry_on_error` exhausts): **no
   soft-deletes**; every request in the sub-batch is routed by `classify_unpin_error` (500/timeout/
   network → transient → retry). `400 batch_too_large` is impossible (we cap ≤1000); if it ever
   surfaces it classifies permanent and lands loudly in the DLQ.
7. **404 (endpoint not deployed).** Fall back to the existing per-file path for **every** request in
   the group (`process_unpin_request` per request — full legacy DELETE + soft-delete + routing).

When the flag is **OFF**, behavior is byte-for-byte the existing per-file path; the old code is left
intact. Graceful shutdown still drains in-flight batch tasks **before** closing the shared client
(the client must outlive its in-flight HTTP calls), and the periodic retry-mover is preserved.

### A note on `401` classification

The task spec loosely groups `401` with "whole-batch → retry (transient)". We instead route
whole-batch exceptions through the existing `classify_unpin_error`, which classifies a `401`
(`HippiusAuthenticationError`) as **permanent** — consistent with how auth failures are treated
everywhere else in this codebase (retrying bad credentials is pointless). The tested whole-batch
cases (500 / timeout / network) all classify transient as required. Flag for reviewer confirmation.

## Config knobs

| Env var | Default | Meaning |
|---|---|---|
| `HIPPIUS_UNPINNER_BATCH_DELETE` | `false` | Feature flag. OFF = per-file (prod default). |
| `HIPPIUS_UNPINNER_BATCH_MAX_FILES` | `1000` | Max file_ids per batch call; hard-clamped ≤1000. |
| `HIPPIUS_ARION_FOLDER_HASH` | `""` | folder_hash sent to `/delete_files` (root by default). |

## Testing

`tests/unit/test_unpinner_batch.py` — adversarial, no live services. Covers: single-chunk batch,
multi-chunk request, request spanning two sub-batches, partial per-item failure isolation,
`already_deleted` success, `invalid_file_id` → DLQ, whole-batch failure → retry-all/no-soft-delete,
404 → per-file fallback, duplicate file_id dedup with multi-chunk/multi-request fan-out, missing
file_id in response, soft-delete DB failure → retry, grouping by ss58 (never mixed), `>1000` split
into `≤1000` calls, empty-identifier retry-then-drop, flag OFF, graceful-shutdown ordering, and
folder_hash threading. The existing `test_unpinner_loop.py` / `test_unpinner.py` per-file tests
remain green (their config fake now pins the flag OFF).

## Rollout plan

1. Merge with the flag **OFF** (prod stays per-file).
2. **Confirm with HCFS** that `/delete_files` accepts our empty `folder_hash` for root-folder
   objects, and that the endpoint is deployed on the target environment.
3. Enable `HIPPIUS_UNPINNER_BATCH_DELETE=true` on **staging**, watch the unpin DLQ, `files_deleted`
   counts, and Arion error rates.
4. Flip the flag on **prod**, ramp cautiously (watch Arion 429/5xx and the DLQ), then drain the
   backlog. Roll back instantly by setting the flag back to `false` (per-file fallback is always in
   the binary).
