# todo.md — hippius-s3 improvement backlog

Open backlog of pitfalls, optimizations, and onboarding tasks. **Pick one, propose a solution, open a PR.** If you start something, update the item with a PR link. When in doubt about scope or severity, ask Radu.

If you're new here, read [CLAUDE.md](CLAUDE.md) first for the architectural map. This file assumes you know what the gateway, API, writer, reader, cache, janitor, and workers are.

---

## 1. Current architecture in one screen

```
                                    ┌───────────────────────────────────────┐
  S3 client ──HTTPS+SigV4─────────▶ │  Gateway  (:8080)                    │
  (aws-cli, boto3, mc, s3cmd)       │  auth · ACL · rate-limit* · audit    │
                                    │  forward_service.py (streaming httpx)│
                                    └────────┬──────────────────────────────┘
                                             │ HTTP + X-Hippius-* trust headers
                                             ▼
                                    ┌───────────────────────────────────────┐
                                    │  API  (:8000)                         │
                                    │  parse_internal_headers · fs_cache_   │
                                    │  pressure · input_validation          │
                                    │                                       │
                                    │  writer/ (PUT)        reader/ (GET)   │
                                    │     └─── object_writer   └── streamer │
                                    │         └─ fs_store.set_chunk         │
                                    │         └─ WriteThroughPartsWriter    │
                                    │                                       │
                                    │  obj_cache: FS chunks + Redis pubsub  │
                                    └──┬─────────┬─────────────┬────────────┘
                                       │         │             │
          ┌────────────────────────────┘         │             └──────────────┐
          ▼                                      ▼                            ▼
  ┌──────────────┐              ┌─────────────────────────┐       ┌─────────────────────┐
  │ PostgreSQL   │              │ FileSystemPartsStore    │       │ Redis queues        │
  │ + keystore   │              │ /var/lib/hippius/       │       │ arion_upload_requests│
  │              │              │   object_cache/         │       │ arion_download_...  │
  └──────────────┘              │ + ChunkNotifier pubsub  │       │ unpin_requests      │
                                └────────┬────────────────┘       └─────────┬───────────┘
                                         │                                  │
                                         ▼                                  ▼
                                  ┌──────────────┐              ┌────────────────────────┐
                                  │ Janitor      │              │ Workers                │
                                  │ hot retention│              │ Arion uploader         │
                                  │ disk pressure│◀──chunk_backend──│ Arion downloader    │
                                  │ modes        │              │ Arion unpinner         │
                                  └──────────────┘              │ orphan_checker         │
                                                                │ account_cacher         │
                                                                └────────┬───────────────┘
                                                                         │
                                                                         ▼
                                                                ┌─────────────────────┐
                                                                │ Arion (storage)     │
                                                                │ Hippius chain (pub) │
                                                                └─────────────────────┘

* rate-limit + banhammer middleware currently not wired (gateway/main.py:94)
```

For detail: [CLAUDE.md section 3](CLAUDE.md) (request lifecycle).

---

## 2. Settled decisions — not open work

Audits keep re-raising these as findings. They are decided. Reopen only with new evidence, and loop in Radu.

### The Arion circuit breaker was removed on purpose

A working breaker shipped in [0de0fe9](https://github.com/thenervelab/hippius-s3/commit/0de0fe9) and was deleted again inside the same PR (#183) by [be8c76a](https://github.com/thenervelab/hippius-s3/commit/be8c76a) (radu.mutilica, 2026-06-10). Rationale, verbatim from the commit: *"The circuit breaker added state-machine complexity (half-open gating, failure classification, per-pod shared state) and several subtle edge cases for a secondary safety concern. The performance win is the bounded-dispatch loop + the single shared per-pod Arion semaphore + larger DB pool — those stay. … Transient Arion errors fall back to the existing per-request exponential-backoff retry; ramp concurrency cautiously instead."*

Honest nuance:

- The fallback that rationale leans on was **not actually working** when it was written. The deployed uploader budget was 2 attempts / ~300ms of backoff, so a brief Arion blip DLQ'd the in-flight backlog rather than riding it out. PR #288 raised it to 7 attempts / ~63s (`HIPPIUS_UPLOADER_MAX_ATTEMPTS: "7"`, base 500ms, max 60s in [k8s/base/configmap-defaults.yaml](k8s/base/configmap-defaults.yaml)). The reasoning holds better today than on the day it was recorded.
- Counter-argument, for whoever reopens this: 7 attempts × 10 replicas is more load on a degraded backend than 2 × 10 was. A breaker is the standard answer to that. Measure before arguing.
- A breaker would **not** have helped the 2026-07-19 incident. Those were 401s, which [arion_service.py:218-220](hippius_s3/services/arion_service.py) classifies as permanent — they bypass retry entirely and go straight to the DLQ. No amount of failure-rate tripping changes that path.

If it is ever revisited, recover from `be8c76a^` (= fc2068f), the last commit that still had the breaker: `git show be8c76a^:hippius_s3/services/circuit_breaker.py` (94 lines) and `git show be8c76a^:tests/unit/test_circuit_breaker.py` (168 lines). Not from 0de0fe9 — that holds the earlier 72/103-line draft, before fc2068f folded in the PR #183 review fixes.

### Sub-token scope enforcement is live, not dead code

Recurring audit finding: "`gateway/services/sub_token_scope.py` imports a nonexistent `TokenAcl` and is wired nowhere." Both halves are false.

- There is no `TokenAcl` symbol anywhere in the repo (`rg TokenAcl` hits only prose). The module imports `BucketScope`, `Op`, `Permission`, `SubTokenScope` from [hippius_s3/models/sub_token.py](hippius_s3/models/sub_token.py) — all real.
- It is on the live ACL path: [gateway/middlewares/acl.py:9-14](gateway/middlewares/acl.py) imports it and the sub-token branch at [acl.py:164](gateway/middlewares/acl.py) calls `evaluate` at [acl.py:190](gateway/middlewares/acl.py), backed by the `sub_token_scopes` table and its cache.

The claim propagates from [gateway/CLAUDE.md:75,82](gateway/CLAUDE.md) and [gateway/services/CLAUDE.md:53-68](gateway/services/CLAUDE.md), which are auto-loaded into every agent session — which is why it resurfaces every audit. **Fixing those two files is the actual fix**; correcting this backlog only stops the symptom.

---

## 3. Pitfalls & known rough edges

### P0 — Silent PUT connection-close on Arion placement stall (postmortem 2026-04-21)

**Reference**: https://s3.hippius.com/will/public/hippius-s3-put-degradation-2026-04-21.html (public postmortem by Will).

**What happens**: Between ~08:20 and 09:30 UTC on 2026-04-21, PUTs from one client intermittently hung for 20–30s then failed with `ConnectionClosedError` from the AWS SDK. GET/LIST/DELETE stayed healthy throughout. Same request size range succeeded on retry. Signature points at Arion placement-commit stalls; only the write path uses it.

**Why it matters**:
- The gateway returned no HTTP status at all — it dropped the TCP connection. The AWS SDK does **not** retry `ConnectionClosedError` by default, so automation silently drops writes and moves on. Obsidian-style sync tools report the file as "written" to the user when nothing was stored.
- Size-independent latency in the healthy case (100 B PUT ≈ 256 KB PUT ≈ 1.3s) means every small-object write eats ~1s of placement overhead. Fine-grained sync tooling takes heavy UX damage from this.

**Proposed hardening** (short-term):

1. **Return 503 + `Retry-After` instead of letting the socket idle-close.** In [gateway/services/forward_service.py:67](gateway/services/forward_service.py), the upstream httpx call uses `Timeout(300.0, connect=10.0)`. If the upstream API is stuck waiting on Arion, we need an earlier watchdog that emits a proper response instead of hitting uvicorn's transport timeout. Specifically: wrap the streaming pipe in a per-operation deadline (e.g. 45s for PUT) and, on deadline, return a synthesized 503 with `Retry-After: 30`. AWS SDKs retry 503 by default.
2. **Add a write-backpressure signal.** Export a gauge on `/metrics` (or a small `/health?component=write` endpoint) that reports the current p95 Arion upload latency — something a small-object sync tool or the status page can poll before hammering. This is the "expose a status-page `PUT success rate p1m`" item in the postmortem.
3. **Gateway worker-pool saturation counter.** Postmortem H2: if all httpx workers are stuck on a slow upstream, incoming PUTs queue and then drop silently. Today [gateway/services/forward_service.py:62](gateway/services/forward_service.py) uses `max_connections=100, max_keepalive_connections=20`. Add `queued_requests` / `active_connections` gauges so we can graph saturation next to PUT p99.

**Longer-term ideas** (open for discussion):
- Small-object fast-path: commit FS cache + DB row + queue, return 200 before Arion acks. Durability tradeoff; needs Radu's call.
- ~~Per-backend circuit breaker~~ — decided against, see [section 2](#2-settled-decisions--not-open-work).

Anchors to touch: [gateway/services/forward_service.py](gateway/services/forward_service.py), [hippius_s3/services/arion_service.py](hippius_s3/services/arion_service.py), [hippius_s3/monitoring.py](hippius_s3/monitoring.py), [gateway/main.py:94](gateway/main.py) (currently-disabled rate limit — may tie in).

### P0 — Broken v5 `object_versions` rows (NULL envelope)

**Status**: partially fixed on the write path. Legacy orphan rows remain in prod (200k+ at last count — see [analysis.md](analysis.md)). Every GetObject / HeadObject / CopyObject against a broken row currently 500s.

**Fix state**:
- Write side now writes `kek_id`/`wrapped_dek` **immediately** after `upsert_object_basic` ([hippius_s3/writer/object_writer.py:244-261](hippius_s3/writer/object_writer.py)), preventing new broken rows under normal operation. Previously these ran after stream completion, creating a gap where client disconnect left NULL envelope. Confirm with prod metrics that the rate of new broken rows is zero.
- Read side has a fallback to version-1 ([hippius_s3/services/object_reader.py:177-220](hippius_s3/services/object_reader.py)) — mitigates the read race on concurrent overwrite but doesn't help the genuinely-orphaned rows (no valid previous version).

**Still to ship** (from [analysis.md](analysis.md)):
1. **Read-time filter** on ListObjects / GetObject / HeadObject / CopyObject-source queries:
   ```sql
   AND NOT (ov.storage_version >= 5 AND (ov.kek_id IS NULL OR ov.wrapped_dek IS NULL))
   ```
   Makes broken rows return 404 NoSuchKey instead of 500. Low-risk short-term patch.
2. **Compensating cleanup** on writer exceptions — DELETE the reserved version and revert `current_object_version` on failure.
3. **Janitor worker sweep** for broken rows older than 30m to catch pod/OOM kills.
4. **DB CHECK constraint** once the backlog is drained:
   ```sql
   CHECK (storage_version < 5 OR (kek_id IS NOT NULL AND wrapped_dek IS NOT NULL))
   ```

### RESOLVED — Double FS writes on every upload

**Was**: the upload path was believed to write every chunk to FS twice — once directly via
`fs_store.set_chunk` and again via an `obj_cache.set_chunks` mirror.

**Status (2026-07-17)**: fixed. The second write is gone — `put_simple_stream_full` and
`mpu_upload_part_stream` write each chunk exactly once (guarded by
`tests/unit/test_put_simple_stream_full.py`), and `RedisObjectPartsCache.set_chunks` had zero
production callers and has been deleted. `WriteThroughPartsWriter.write_chunks` writes via
`fs_store.set_chunk` only.

**Follow-up (minor cleanup)**: `WriteThroughPartsWriter.__init__` still accepts a `redis_cache`
param that is stored but never read (`hippius_s3/writer/write_through_writer.py:17-26`). Drop it and
the `ExplodingCache` arg in `tests/unit/test_write_through_writer.py` in a dedicated cleanup;
consider renaming the class to `FSPartsWriter`.

### P1 — Accepted SSD leak from the UploadPart cleanup-race fix (unbounded until ov-row cleanup lands)

**Context**: branch `fix/upload-part-cleanup-race` stopped failure-path cleanups from deleting shared FS part data — a duplicate UploadPart's loser used to destroy the winner's *published* chunks (drain then wrote the part off as unrecoverable). The fix deletes nothing on failure and enforces the exact chunk set at publish time instead ([hippius_s3/writer/object_writer.py:893](hippius_s3/writer/object_writer.py), [hippius_s3/cache/fs_store.py:359 `trim_chunks_from`](hippius_s3/cache/fs_store.py)).

**The accepted cost** — two leak shapes now accumulate on SSD:

1. **Meta-less MPU orphan dirs**: a failed UploadPart attempt that never published leaves its chunk files behind with no `meta.json`.
2. **Append CAS-loser chunk-only dirs**: the CAS loser is un-published (`meta.json` deleted) but its chunks are deliberately kept.

Both shapes have no cephor row, or a failed one. Post-PR-#359 reclaim semantics HOLD any part whose `object_versions` row exists, and **nothing today deletes an abandoned version's `object_versions` row** — so the leak is unbounded, not self-limiting.

**Follow-up that closes it**: a reaper that deletes the abandoned version's `object_versions` row once its parts are dropped. That is the unlock — with the ov row gone, #359's absence-proof reclaim takes over and the leaked dirs become reclaimable instead of held forever.

### Follow-up — janitor exact-set sweep for stale chunk tails

The publish-time trim only covers parts published after the fix. For inventoried parts with meta present, a janitor sweep should delete `chunk_i` where `i >= meta.num_chunks`. This retro-heals the pre-existing stranded stale-tail population and self-heals publish-time trim failures (trim is best-effort by contract — ERROR-logged, never raised). It matters because the drain gate ([crates/hippius-drain-core/src/partdrain.rs:445-461](crates/hippius-drain-core/src/partdrain.rs)) requires the on-disk set to be EXACTLY `{0..num_chunks-1}` — extras mean permanent IncompleteSource deferral: never replicated, never evicted.

### Follow-up — hippius-otel alert on servable write-offs

Add `increase(drain_parts_written_off_servable_total[1h]) > 0` to hippius-otel. A servable write-off is data loss for a part a client could still read — always operator-worthy, and the counter exists precisely so this alert can be cheap.

### P1 — Meta.json rewrites on concurrent upload + download

**What**: Upload writes `meta.json` once after all chunks ([object_writer.py:420](hippius_s3/writer/object_writer.py), [object_writer.py:~865 `mpu_upload_part_stream`](hippius_s3/writer/object_writer.py)). Downloader writes `meta.json` **eagerly** per part at the start of processing ([hippius_s3/workers/downloader.py:49-91](hippius_s3/workers/downloader.py)). For an object that was just uploaded and is immediately read via a cold-miss downloader path, the meta is written twice with identical content.

**Why it's safe today**: atomic rename, identical payload, last-write-wins.

**Why it matters**: extra FS ops on hot read paths. Not huge but easy to avoid.

**Proposed**: in `downloader._write_part_meta_from_db`, `await fs_store.get_meta(...)` first and skip the write if present (already done in [downloader.py:256-269](hippius_s3/workers/downloader.py) — good). The only rewrite window is if the meta is *missing* — which is the only time we want it. So: probably a non-issue in the current code, but worth verifying with a trace/counter that rewrites are ≈0 in prod.

### P1 — `execute_v5_fast_path_copy` latent risk

**File**: [hippius_s3/services/copy_service_v5.py:24](hippius_s3/services/copy_service_v5.py).

Fast-path copy: rewraps the DEK under the destination's AAD, copies `chunk_backend` rows pointing to the same backend object, skips the byte copy. Currently enabled only for single-part (non-MPU) v5 objects. For MPU, [copy_object_endpoint.py](hippius_s3/api/s3/objects/copy_object_endpoint.py) falls back to streaming copy because v5 crypto binds per-object-id AAD.

**Latent risk**: if someone re-enables this for MPU without also writing FS cache entries for the new `object_id`, streamer reads on the destination will hang on `wait_for_chunk` (chunks are on Arion under the source's identifier but `fs.chunk_exists` returns False for destination). The downloader would eventually fill it, but only if `chunk_backend` rows correctly point at the same Arion identifier under the new object_id — check carefully.

**Proposed**: add a prominent comment block at [copy_service_v5.py:24](hippius_s3/services/copy_service_v5.py) documenting the invariant ("fast path requires either (a) non-MPU single-part object OR (b) explicit FS backfill of all chunks into the destination object_id path"). Consider a feature flag before re-enabling for MPU.

### P2 — Gateway → API streaming hop

**What**: Every request streams through `gateway → forward_service → httpx → api`. Client body is read once via `request.stream()`, forwarded via `httpx.AsyncClient.stream()`, and the API response is re-streamed to the client via `StreamingResponse`. No buffering. ([gateway/services/forward_service.py:113-170](gateway/services/forward_service.py)).

**Cost**: per-byte NIC traversal doubles (client→gw + gw→api), and tail latency doubles for any request where the API is slow. On large GETs this is meaningful. Keep-alive pool is shared (100 connections, 20 keepalive).

**Why we can't just 307-redirect**:
- Gateway is the only component that can verify SigV4 against the DB/Arion-backed access keys and evaluate ACL. Clients don't have auth tokens for the internal API.
- The internal API listens inside the cluster; exposing it publicly would duplicate the auth surface.

**Ideas worth exploring** (pick one, measure):

1. **Merge gateway + API in the same pod.** Run one ASGI app with a "role" config: gateway role mounts the auth/ACL/forward chain and routes to local endpoints; api role exposes just `/s3`. Keeps the layering as middleware ordering rather than network hops. Tradeoff: harder to scale the two independently, but today they scale together anyway.
2. **HTTP/2 + HPACK** between gateway and API. Cuts per-request header overhead; no behavior change required. Verify it doesn't break streaming upload chunked framing.
3. **Dedicated cache-hit fast-path** for GETs that resolve entirely from FS cache: a lightweight sub-app mounted on the gateway that skips the httpx trip when `fs_cache_pressure` and chunks_exist_batch both say "all green". Obviously needs the gateway pod to have the FS cache volume mounted — requires redesign.
4. **Measure first.** Add a gauge of gateway→API latency (we already emit `X-Hippius-Gateway-Time-Ms` on the forward). If the extra hop isn't actually dominating any SLO, leave it alone.

No strong opinion here — needs a benchmark before we touch anything.

### P2 — `CreateBucket` lifecycle XML is parsed then discarded

**File**: [hippius_s3/api/s3/buckets/bucket_create_endpoint.py:78](hippius_s3/api/s3/buckets/bucket_create_endpoint.py). Explicit `# todo: For now, just acknowledge receipt` — the endpoint parses the XML and logs the rule IDs but doesn't persist them. Client sees 200 OK and believes its lifecycle config was saved.

**Proposed**: either store into a `bucket_lifecycles` table (and have the janitor/orphan worker respect it for expiry), or reject with 501 NotImplemented until we're ready. Silent acknowledgement is worse than either.

Same file, line 177-179: CORS configuration is similarly acknowledged and ignored. Intentional per the commit history, but worth flagging: if we ever do want to surface bucket-level CORS, this is where it lives today.

### P2 — Pre-check race on PUT overwrite object_id selection

**File**: [hippius_s3/api/s3/objects/put_object_endpoint.py:105-108](hippius_s3/api/s3/objects/put_object_endpoint.py):

```python
# TODO: Make object identity/version allocation fully DB-atomic by removing this
#       pre-check and always passing a generated candidate UUID. The writer already
#       treats the DB-returned object_id/object_version as authoritative.
```

Two concurrent PUTs to the same (bucket, key) today both fetch the existing object ID, then both call `upsert_object_basic`. The upsert is atomic, so only one's `object_version` wins — but both initially reserved the *same* object_id. The writer already re-reads `object_id` from the DB response ([object_writer.py:222-227](hippius_s3/writer/object_writer.py)), so this is defensive.

**Proposed**: always pass `uuid.uuid4()` as the candidate, let the DB override on conflict. Removes the read-before-write and simplifies the endpoint. Low risk.

### P1 — Path normalization: one view, set once

**Seam**: [gateway/main.py:231](gateway/main.py) (the `# TODO:` line next to `ray_id_middleware`, second-outermost).

The gateway judges paths in two views and the api receives a third-party rewrite of one of them. `ForwardService` interpolates the decoded `scope["path"]` into a URL *string* ([forward_service.py:135](gateway/services/forward_service.py)) and hands it to httpx, which then **collapses `.`/`..`** and **truncates at the first `#`/`?`** before the request goes out; the api's uvicorn then **decodes percent-escapes a second time**. So "the path" is ambiguous at every layer, and each layer that picks the wrong view is deciding about a request that is never sent.

This has now failed three separate times, each fixed in isolation:

1. **2026-08-03 (prod)** — `auth_router` matched exempt paths with `startswith()`, so bucket `docs2` skipped authentication entirely and landed as an anonymous-owned bucket.
2. **PR #401** — `/anybucket/../internal/parts/...` passed `input_validation`'s first-segment denylist and reached the api as `/internal/parts/...`, because the denylist judged the uncollapsed path. Fixed by introducing `forwarded_path`.
3. **PR #401 (later)** — `/internal%23x/parts/1` and `/internal%3Fx/parts/1` passed the same denylist and arrived as `GET /internal`, because `forwarded_path` modelled only dot segments and not the `#`/`?` truncation. Fixed by truncating. Separately, `auth_router`/`acl`/`account`/`frontend_hmac` were still judging the uncollapsed path — a candidate auth bypass (`GET /docs/../anybucket/key` skipped auth AND had ACL evaluated against the ownerless `docs` bucket).

Each fix was correct and each left the *class* open, because the invariant lives in no single place: it is re-asserted by every caller that remembers to ask the right question. `gateway/utils/paths.py::routing_path` is now the one right answer, but nothing forces a new middleware to use it — and a plausible-sounding alternative (`request.url.path`) is one attribute access away.

**Proposed**: rewrite `scope["path"]` to `routing_path(request)` once, in a middleware at the seam above, and let every downstream layer read `request.url.path` freely — because it would then already BE the forwarded view. `ForwardService` keeps interpolating `scope["path"]`, which is now idempotent under httpx's rewrites (dot segments already collapsed, nothing after a `#`/`?` left to truncate), so the gateway and the api agree by construction rather than by discipline.

Deliberately not done as part of #401: it changes the path every middleware sees, so it needs its own PR with e2e coverage of the exempt routes, the `/user/` HMAC arm, presigned-URL canonicalization (**note**: `sigv4.py` must keep signing over `raw_path` — the client signed what it sent, not what we forward), and the ATS cache-key/purge paths.

**Still judging `request.url.path` today** (each is the same class; none is known-exploitable, listed so the sweep is mechanical rather than archaeological):

| File | Decision | Consequence of the wrong view |
|---|---|---|
| [cache_control.py:64](gateway/middlewares/cache_control.py) | ATS cache key (bucket/key) | Cache key disagrees with the object served — stale or cross-key hits |
| [ats_purge.py:31](gateway/middlewares/ats_purge.py) | Which key to invalidate on write | A write invalidates the wrong key; readers keep the stale copy |
| [rate_limit.py:66-70](gateway/middlewares/rate_limit.py) | `skip_paths`, `/user/` skip | Rate limiting dodgeable via `/health/../bucket/key` |
| [read_only.py:21](gateway/middlewares/read_only.py) | `ALWAYS_ALLOWED_PATHS` | Exact-match only, so it fails safe — writes stay blocked |
| [audit_log.py:19](gateway/middlewares/audit_log.py), [tracing.py:22](gateway/middlewares/tracing.py) | Log/span attributes | Observability only: audit lines name a path the api never saw |

**Also worth one query before the next release**: the new `%`-in-first-segment rejection ([input_validation.py](gateway/middlewares/input_validation.py)) makes any pre-existing bucket whose name contains `%` unreachable. Such a name cannot be created through the gateway (`BUCKET_NAME_PATTERN` refuses it), but confirm none exists: `SELECT bucket_name FROM buckets WHERE bucket_name LIKE '%\%%' ESCAPE '\';`

### P2 — Rate limiting and banhammer disabled at gateway

**File**: [gateway/main.py:94](gateway/main.py) logs `"Rate limiting and banhammer disabled"`. The middleware modules exist ([gateway/middlewares/rate_limit.py](gateway/middlewares/rate_limit.py), [gateway/middlewares/banhammer.py](gateway/middlewares/banhammer.py)) but are not registered in the middleware chain. Reasons unknown from code alone — maybe performance, maybe maturity.

**Ask Radu**: is this intentional for now, or should we re-enable? If enabling, verify `redis-rate-limiting` and `redis-acl` capacity/perf at current RPS.

### P2 — Seed phrase auth failure messages

**File**: [gateway/middlewares/sigv4.py](gateway/middlewares/sigv4.py). Base64 decode failures on the seed phrase return a bare 403 InvalidAccessKeyId with no diagnostic. Users who've fat-fingered their access key see no hint. Minor UX win to return `"Malformed seed encoding"` in non-prod.

---

## 4. Download / range / bandwidth optimizations

### 4.1 How the FS cache works today (recap)

- **Storage**: one file per chunk on the shared cache volume (`/var/lib/hippius/object_cache/<object_id>/v<ver>/part_<n>/chunk_<i>.bin`), plus a `meta.json` per part that acts as the "this part is known" signal. See [hippius_s3/cache/fs_store.py](hippius_s3/cache/fs_store.py).
- **Atomicity**: writes go to `.tmp.<uuid>` and `os.replace` to final ([fs_store.py:92, 123-131](hippius_s3/cache/fs_store.py)). No locks needed; content is deterministic.
- **Readiness**: `get_chunk` returns None unless `meta.json` exists AND the chunk file exists ([fs_store.py:168-173](hippius_s3/cache/fs_store.py)). Eager meta from the downloader ([workers/downloader.py:49-91](hippius_s3/workers/downloader.py)) enables per-chunk visibility during partial fills.
- **Hot retention**: every successful read calls `os.utime` on the chunk and the meta ([fs_store.py:183-186](hippius_s3/cache/fs_store.py)). Janitor's hot-retention check reads those mtimes.
- **Coordination**: `ChunkNotifier` ([hippius_s3/cache/notifier.py](hippius_s3/cache/notifier.py)) publishes `notify:{chunk_key}` on `redis-queues` when a downloader lands a chunk. Streamers subscribe + re-check on each notification. Fast-path (FS hit) bypasses pub/sub entirely.
- **Coalescing**: `build_stream_context` ([hippius_s3/services/object_reader.py:77-104](hippius_s3/services/object_reader.py)) uses `SET NX EX <DOWNLOAD_COALESCE_LOCK_TTL>` (default 600) on `download_in_progress:{object_id}:v:{ov}:part:{pn}` so N simultaneous readers of a cold object only cause one backend fetch. Lock is released by the downloader when the part lands ([workers/downloader.py:286-292](hippius_s3/workers/downloader.py)). TTL covers crashed-downloader case.

### 4.2 How the janitor works today (high level)

- Scans `<root>/<object_id>/v<ver>/part_<n>/` trees periodically.
- Classifies each part by age → bucket gauge.
- Replication check via `chunk_backend` rows: only delete if fully replicated to every required backend (`upload_backends` ∪ `backup_backends`).
- Honors hot retention (parts read in the last `HIPPIUS_FS_CACHE_HOT_RETENTION_SECONDS`).
- Disk-pressure modes (normal / elevated / critical) adjust the hot window, never the replication gate.
- Under critical pressure with nothing replicated, logs ERROR and refuses — intentional deadlock-detection over silent data loss. See [workers/run_janitor_in_loop.py:1-22](workers/run_janitor_in_loop.py).
- Dedicated cleanup for orphan `.tmp.*` files older than 1h.

### 4.3 Cold-download bandwidth ideas

- **Range-aware backend fetch.** Client requests bytes 100–200 of a 1 GiB object. Planner picks one chunk; downloader fetches the **whole 4 MiB chunk** from Arion. For small Range reads this burns 4 MiB of backend bandwidth per 100 B of user data. Idea: add `BackendClient.download_range(identifier, offset, length)` and have the downloader use it when the plan specifies a single-chunk slice smaller than, say, 64 KiB. Risk: chunks are ciphertext and the streamer expects full ciphertext for AEAD verification — **this doesn't work** unless we also shift to per-sub-chunk nonces or accept that range-fetched bytes are verified during re-download. Start with a design doc before writing code.
- **Stream-through for one-shot Range.** For Range reads tagged with `x-hippius-no-cache`, fetch Arion → decrypt → stream → do NOT persist to FS. Trades cold-re-read latency for NVMe preservation on rarely-revisited data.
- **Lookahead prefetch.** `stream_plan` prefetches in-flight up to `prefetch_chunks`. The streamer function-parameter fallback is 0, but the wired runtime default is **16** (`HTTP_STREAM_PREFETCH_CHUNKS`, [config.py:296](hippius_s3/config.py)) — so prod already runs the pipelined branch ([streamer.py:82-88](hippius_s3/reader/streamer.py)). Note (RD-2): prefetch currently overlaps only the FS/Arion fetch, not the on-loop decrypt, so deep prefetch mostly buffers memory until decrypt is offloaded.
- **Keep-alive to Arion.** Confirm `arion_service.py` uses a shared `httpx.AsyncClient` per worker pod, not one-per-request. TCP setup dominates small-object latency per the postmortem (100 B PUT = 256 KB PUT ≈ 1.3 s). If already shared, good; otherwise that's a one-line fix.
- **Regional cache.** [deploy-cache-production](.github/workflows/deploy.yaml) is disabled (`if: false` at line 373). Re-enabling would put a FS-cache-backed gateway/API/downloader stack in `us-0`/`eu-0`, cutting cold-read latency for geographically distributed clients. Blocker: NVMe PVC provisioning per region + deciding how cross-region invalidation works (probably: it doesn't; each region is an independent cache).

---

## 5. Cache invalidation proposals

Current state: the FS cache is content-addressed by `(object_id, version, part, chunk_index)`. Writes are immutable per that key. "Invalidation" only happens implicitly via:
- Janitor age-out.
- Explicit `fs_store.delete_part` / `delete_object` on object deletion (soft delete flow).

Known gaps:

### 5.1 Object overwrite doesn't free old version's FS bytes

Overwriting an object creates a new `object_version` with a new DEK, a new object_id is NOT created (same object_id, higher version). The old version's chunks stay on FS under `v<old>/` until janitor ages them out. For large, frequently-overwritten objects this wastes disk.

**Proposed**: on successful `update_object_version_metadata` for a new version (see [hippius_s3/writer/db.py](hippius_s3/writer/db.py)), schedule a targeted `fs_store.delete_object(object_id, object_version=prev_version)` once we've confirmed the new version is serving. Has to wait for any in-flight reads against the old version (check by stat-ing atime recency, or just let janitor handle the wind-down).

### 5.2 Soft-deleted objects linger in cache

`chunk_backend.deleted_at` is set on DELETE but the FS copy stays. The ACL/existence check in the read path (`get_object_for_download_with_permissions`) should filter these out before reaching the streamer, but if any legacy path doesn't filter, a stale GET could 200 on data the user believes they deleted.

**Action**: audit every SQL query that feeds `build_stream_context` for a `deleted_at IS NULL` predicate. Anchor: [hippius_s3/sql/queries/](hippius_s3/sql/queries/).

### 5.3 Partial-fill meta consistency

The downloader writes `meta.json` before the chunks land ([downloader.py:49-91](hippius_s3/workers/downloader.py)). A reader seeing `meta.json` cannot assume "part is complete" on the download path — only on the upload path. `wait_for_chunk` ([cache/notifier.py:61](hippius_s3/cache/notifier.py)) handles this correctly by re-checking after subscribe, but any new code should **not** gate on `meta.json` presence alone.

**Proposed**: document this invariant explicitly in [hippius_s3/cache/CLAUDE.md](hippius_s3/cache/CLAUDE.md) (already in the rewrite list). Consider a small marker like `.complete` for the upload path only, so a downstream tool can distinguish.

### 5.4 Mixed-deploy window after a cache refactor

During any future change to the cache layout (not planned right now, but e.g. if we add a content-hash index), a rolling deploy will have old pods reading old layout and new pods reading new. Janitor races especially bad. Checklist: always provide a read-fallback (see `DualFileSystemPartsStore` at [hippius_s3/cache/dual_fs_store.py](hippius_s3/cache/dual_fs_store.py)) and migrate with a flag-gated single rollout.

---

## 6. Retry-mechanism hardening (postmortem followups)

Checklist derived from the 2026-04-21 postmortem. Each item is a small-medium PR.

1. **503 + Retry-After on upstream timeout in forward_service.** Today the gateway closes the connection after ~300s if the upstream API never responds ([forward_service.py:61](gateway/services/forward_service.py)). A deadline of ~45s with a synthesized 503 response would let AWS SDKs retry. Make the deadline configurable per-method (PUT tighter than GET).
2. **Surface Retry-After from internal API to client.** If the API itself returns 503 with Retry-After (e.g. `fs_cache_pressure_middleware` already does), the gateway must forward that header unmodified. Verify in [forward_service.py:130-146](gateway/services/forward_service.py).
3. **Small-object fast-path** (postmortem §6 long-term). Return 200 after FS write + queue, before Arion ack. Durability tradeoff; needs Radu's decision.
4. **Status-page PUT success gauge**. Expose p1m PUT success rate on `/metrics` and ship a Cachet component for it. The postmortem explicitly calls this out as a user-facing need.
5. **DLQ retry ergonomics**. The DLQ exists ([hippius_s3/dlq/base.py](hippius_s3/dlq/base.py)) and `scripts/dlq_requeue.py` works, but there's no dashboard panel for DLQ depth per-backend. Add one.

---

## 7. Dead code and cleanup candidates

Low-risk deletions; each one should be a one-PR cleanup:

1. **[hippius_s3/writer/cache_writer.py](hippius_s3/writer/cache_writer.py)** — `CacheWriter` class. Not referenced anywhere in the main code graph except the module itself. `WriteThroughPartsWriter` superseded it. Confirm with `rg '\bcache_writer\b|\bCacheWriter\b'` (only self-reference expected) and delete.
2. **Redis download-cache residue**. Grep for `REDIS_DOWNLOAD_CACHE_URL`, `redis_download_cache_url`, `DOWNLOAD_CACHE_TTL`, `redis-download-cache`. Should all be gone after the FS migration. Patch any stragglers in docker-compose files and k8s manifests.
3. **`set_download_chunk`** shim in [hippius_s3/cache/object_parts.py](hippius_s3/cache/object_parts.py) — if still present (prior memory says it was removed), verify. Old download-cache API.
4. **Any references to `manifest_cid` or `manifest_service`**. Replaced by `chunk_backend` tracking long ago.
5. **[hippius_s3/workers/fs_cleanup.py](hippius_s3/workers/fs_cleanup.py)** — zero importers (`rg fs_cleanup` hits only the module itself; no entry point in `workers/` runs it). Confirm and delete.

---

## 8. Getting started as a new contributor

1. Clone the repo. You need Python 3.10+, Docker (with compose v2), and `uv`.
2. Create a venv: `python3 -m venv .venv && source .venv/bin/activate && uv pip install -e ".[dev]"`.
3. Copy `.env.example` → `.env`, fill in the required vars from [CLAUDE.md section 8](CLAUDE.md).
4. `docker compose up -d`. Watch `docker compose logs -f api`.
5. Run unit tests: `pytest tests/unit -v`. All should pass.
6. Read [CLAUDE.md](CLAUDE.md) if you haven't, then skim the subsystem `CLAUDE.md` files for the area you're touching.
7. Pick a P2 from this file. Ask Radu before picking a P0/P1 — those usually have coordination requirements.
8. Write tests alongside the change (unit + e2e where applicable).
9. Open a PR; CI runs lint + unit + integration.
10. Never commit to `main` directly. Branch names: `fix/...`, `feat/...`, `refactor/...`.

Questions go in the PR or directly to Radu (`radu.mutilica`). Don't guess — this system has enough subtle invariants (crypto binding, janitor replication gate, writer visibility gating) that a question is usually cheaper than a rollback.
