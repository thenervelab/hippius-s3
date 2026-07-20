# Upload/Download Performance — Implementation Plan

**Date:** 2026-07-17 · **Source:** `upload-download-perf-review-2026-07-17.md` (all 67 findings adversarially
re-verified against live source; 59 confirmed, 8 corrected, 0 refuted) · **Grounding:** six domain audits of
blast radius + breaking-change risk + test coverage against the code on branch `staging`.

This plan is the bridge from *findings* to *safe change*. It is written for critical infrastructure, so every
item carries three mandatory gates before it can merge:

1. **Blast radius** — every caller/dependent/contract the change can perturb.
2. **Breaking-change verdict** — YES/NO with the reason; anything YES ships behind an env flag with a named
   rollback.
3. **Test requirement** — the existing gating test, the new test, and whether an **e2e** is *required* (the
   change touches real streaming / crypto / DB / client-visible contract semantics) or merely recommended.

---

## 0. Standing engineering rules for this whole effort

These are non-negotiable and apply to every wave below.

- **Never mutate a shared SQL file to batch one caller.** `soft_delete_object.sql`, `list_parts_for_version.sql`,
  `get_object_by_path.sql`, `get_chunk_backend_identifiers.sql`, `get_chunk_backend_identifier.sql` each have
  multiple consumers. Every batching change **adds a new query file** (e.g. `..._batch.sql`, `..._paged.sql`,
  `..._by_part.sql`) and repoints only the intended caller. This one rule prevents the majority of the
  breaking-change surface across §Wave 1 and §Wave 6.
- **One shared, dedicated crypto executor** for RD-2 + WU-1 (and optionally KM-5). A single lazy
  `ThreadPoolExecutor` (new `HIPPIUS_CRYPTO_POOL_WORKERS`, default 4), created in the API lifespan and closed on
  shutdown, kept **separate** from asyncio's default pool (which already carries FS md5 offload and blocking FS
  work). `cryptography`'s AES-GCM releases the GIL, so this yields real cross-core parallelism.
- **Two silent-corruption hazards gate the crypto wave.** Both return HTTP 200/206 with *wrong bytes* — no
  error — so they must be gated behind byte-exact concurrent round-trip e2e **and** property tests, with a
  break-the-code/confirm-the-test-fails step:
  - RD-2: decrypt tasks may *compute* out of order but must be *awaited/yielded* in plan order.
  - WU-1: `chunk_cipher_sizes` must be recorded **by chunk index**, never in encrypt-completion order; md5
    `hasher.update` must stay **inline and sequential** in plaintext order.
- **Every behavioral/capacity change ships with (a) an env knob defaulted to today's value and (b) a named
  metric to watch.** Reversible-by-config, not reversible-by-redeploy.
- **CI is the gate, not local.** Local Python is 3.9; tests/e2e and the Rust gate need CI. Build, push, watch CI
  — do not assume green from local `ruff`/`py_compile`.

---

## 1. Corrections that reshape the report (read before planning work)

The re-verification changed the shape of six items. Plan against these, not the report headline.

| Finding | Report framing | Corrected reality → planning consequence |
|---|---|---|
| **GW-1** | "Pipeline 3-5 serial Redis RTs into MGET + gather" | **Not fully achievable.** auth→acl→account is a hard data dependency (sub-scope needs `token_type` from the auth GET; account/acl need the authenticated principal). Only a **speculative MGET of the two `redis-acl` lookups** inside `acl_middleware` is viable; the biggest real win is **GW-4** (drop the account GET on reads). Do **not** merge the three middlewares to force a gather. |
| **HD-3 vs HD-4** | Two separate fixes | **Alternatives — do one.** HD-4 (point HEAD at a light `bucket_name`-keyed `get_object_by_path` variant) subsumes HD-3 (it already returns `append_version`) and additionally drops the `JSON_AGG`/mpu subquery. HD-5 folds into HD-4's query. Ship **HD-4+HD-5 as one query**, skip HD-3. |
| **MPU-1** | P1, "FOR UPDATE serializes UploadParts" | **P2.** The lock covers only the metadata phase (releases before the MB-scale write) and is a **required** correctness guard for lazy DEK creation (envelope is NULL at MPU initiate). Fix = non-locking read first, escalate to `FOR UPDATE` only when envelope NULL or `rotate=True`. **Never remove the lock.** |
| **DR-5** | P2, "combine the two commit UPDATEs" | **Combine is INVALID** — `mark_replicated`→`mark_upload_enqueued` are intentionally separate so in-flight MPU parts commit `replicated` with `upload_enqueued_at` NULL for the sweep. Ship **only** the drop of the near-dead `status` SELECT. |
| **DR-2** | P1, "SHA serialized on one task" | **P3, by design.** The single-task `FuturesUnordered` overlaps commit fsyncs (not CPU); at the default 100 MB/s drain rate doubled SHA-256 is a fraction of one SHA-NI core. **Defer** — profile-gated only; confirm `sha2` `asm` feature is on first. |
| **GW-3 / DB-1 / RQ-1 / LS-2** | P1/P2 | Severity-deflated (see §Wave rows): GW-3 is a no-op in default config and its result is *used*; DB-1's KEK-pool premise is false (downloader opens no KEK pool); RQ-1 fires only on cold chunks; LS-2's regression is runtime-planner-dependent. Keep as P3/verify. |

---

## 2. E2E gaps to close FIRST (from the coverage map)

Several changes below are **required** to add e2e, but the harness gaps mean some protection must be built
before the refactor, not after. Priority order:

1. **Multi-part (>1 part) MPU assembly e2e** — every `tests/e2e/test_*Multipart*.py` is a single trivial
   happy-path; there is **no** e2e asserting multi-part assembly, ETag combination, or large parts. MPU-1/2/3 and
   WU-1 all touch this. **Build this first.**
2. **Live pub/sub key-contract test** — the `notify:{chunk_key}` channel-key equality between streamer (coalesce)
   and downloader (release) is asserted nowhere against a real `redis-queues`. RQ-1/RQ-4 rework this exact key.
   **Build before RQ-1.**
3. **Prefetch>0 streaming correctness** — no test exercises the pipelined branch (`prefetch>0`), which is the
   actual prod path (default 16). RD-2 and CF-3 both change it. **Build before RD-2.**
4. **ListObjectsV2 pagination/delimiter e2e** — only one thin ListObjects e2e exists; LS-1 rewrites the delimiter
   rollup in SQL. **Build the differential harness before LS-1.**
5. **Concurrent-cold-GET + concurrent-append e2e** — for KM-1 (cold-KEK fan-out) and AP-1 (append CAS races).

Harness: `docker-compose.e2e.yml` (mock-arion/mock-kms/mock-hippius-api/toxiproxy, now incl. drain-agent/-allocator);
fixtures in `tests/e2e/conftest.py`; introspection in `tests/e2e/support/{db,cache,chunks,manifest}.py`; fault
injection in `tests/e2e/mock_faults.py` + `support/compose.py`.

---

## 3. Sequenced waves

Each wave is a mergeable PR (or small PR set). Within a row: **Change** / **Blast** / **Break** / **Test**.
"e2e: REQUIRED" means the PR is not mergeable without it.

### Wave 0 — Measurement + docs + dead code (do first; zero runtime risk except GW-2 metric step)

- **GW-2** (do first — enables measuring every other gateway fix). *Change:* move `ray_id_middleware` from
  second-innermost (`gateway/main.py:204`) to **outermost** (after `cors`, line 222) so `gateway_start_time`/`ray_id`
  are stamped before cors/input_validation/auth/account/acl; keep `auth_probe` innermost. *Blast:* observability only —
  `gateway_overhead_ms` (`main.py:187`) and forwarded `X-Hippius-Gateway-Time-Ms` (`forward_service.py:117`) grow to
  reflect true overhead; upstream logs stop falling back to `"no-ray-id"`. *Break:* NO for authz; **YES (measurement)** —
  overhead dashboards/alerts step upward (they were undercounting) → re-baseline. Verify `X-Hippius-Ray-ID` still set on
  CORS-error/OPTIONS unwind. *Test:* unit (outer middleware sees a real ray_id; overhead ≥ auth+acl time); e2e RECOMMENDED
  (SigV4 round trip asserts both headers). Also correct the stale middleware-order table in `gateway/CLAUDE.md` to match
  `main.py:203-222`.
- **DOC-1** — delete dead `RedisObjectPartsCache.set_chunks` (`hippius_s3/cache/object_parts.py:148`, zero prod
  callers; the "double FS write" is already fixed via `write_through_writer.py:74`). Rewrite `todo.md:114-124`
  from live-P1 to fixed. Flag the now-inert `WriteThroughPartsWriter.redis_cache` param for follow-up removal.
  *Break:* NO (dead code). *Test:* run `tests/unit/test_put_simple_stream_full.py test_write_through_writer.py`.
- **DOC-2 / DOC-3** — correct `CLAUDE.md`/`todo.md`: prefetch default is **16** not 0 (`config.py:296`); coalesce
  lock TTL default is **600** not 120 (`config.py:257`). *Break:* NO. Add two config-default assertions so the
  docs can't drift again.
- **KM-4** — memoize `get_config()` (`config.py:383-435`; called ~5×/PUT in the KEK layer). *Change:*
  `functools.lru_cache(maxsize=1)` or module singleton (the three `object.__setattr__` fixups happen before
  return, so a memoized instance is fully formed). *Blast:* every request path. *Break:* NO for prod; **YES for
  the test harness** — env-mutating gateway tests need a `reset_config()`/`cache_clear()` hook wired into
  fixtures. Ship both together. *Test:* same-object on repeat; new value after reset+setenv; full
  `tests/unit/gateway/` suite green.

### Wave 1 — Verified low-risk metadata/N+1 wins (the report's "PR A", corrected)

All add-a-new-query-file per §0. All P1/P2, all breaking-change **NO** unless noted.

- **RD-1** — batch the downloader per-chunk identifier lookup. *Change:* new
  `get_chunk_backend_identifiers_by_part.sql` returning `(part_number, chunk_index, backend_identifier)`; run once
  per DCR into a dict; on **dict-miss keep `return True` skip AND fall back to the singular
  `get_chunk_backend_identifier`** to close the mid-upload race. *Blast:* `downloader.py` only; singular query
  still used by fallback + HEAD; **do not touch** the plural `get_chunk_backend_identifiers.sql` (unpinner).
  *Test:* unit (one batch query; map hit → fetch; miss → singular fallback → NULL → skip). e2e: not required;
  run `test_GetObject_Range.py` as regression.
- **RD-3** — thread the parts list through the GET path (3 reads → 1). *Change:* add `chunk_size_bytes` **last** in
  `build_initial_download_chunks` SELECT (positional reads stay valid); pass the endpoint's built list into
  `build_stream_context` (skip `read_parts_list`); pass sizes into `build_chunk_plan` (drop `planner.py:34`
  query); **keep the DB path as fallback** for the envelope-race branch and copy/UploadPartCopy callers.
  *Break:* NO for response. *Test:* extend `test_stream_context_batch.py` (zero size-queries when parts supplied);
  **e2e: REQUIRED** (multipart + range byte-identical).
- **WU-2** — prefetch the `part_chunks (chunk_index→id)` map in one query before the gather; **dict-miss must raise
  the identical `part_chunk_row_missing`** (requeue classifier depends on the string). Keep `insert_chunk_backend`
  per-chunk (needs the post-POST `file_hash`; UNNEST batch widens the crash window — defer). *Test:* unit (one
  SELECT/part; missing index → `part_chunk_row_missing`). e2e: not required.
- **WU-3** — drop the `get_object_by_path` pre-check on every PUT (`put_object_endpoint.py:138-155`); always pass a
  fresh UUID, trust `upsert_object_basic ... RETURNING object_id`. *Break:* NO — **clean GO** per VER-4; also
  closes a TOCTOU window. Append/MPU unaffected. *Test:* existing `test_put_object_endpoint.py`.
- **MPU-2** — use `bucket_name`/`bucket_id` from the already-fetched `get_multipart_upload` row; delete the
  `multipart.py:585-602` JOIN and the `object_writer.py:660-671` JOIN + dead `storage_version`. Add `bucket_id`
  param to `mpu_upload_part_stream` (thread from `append_stream` too). *Test:* unit (no bucket/version query when
  passed); deleted-bucket UploadPart still 404s.
- **MPU-3** — pass the endpoint's already-fetched `db_parts` into `mpu_complete`; compute ETag/size from it; drop
  the two internal re-reads. Keep `mpu_complete`'s own UPDATE txn. *Test:* unit (identical `final_md5`/`total_size`
  incl. strict subset); **e2e: REQUIRED** (ETag byte-identical — needs the multi-part MPU e2e from §2).
- **MPU-4** — abort: compute chunk keys from `num_chunks` + pipelined `UNLINK` instead of per-part `scan_iter`;
  keep `scan_iter` fallback when meta absent (variable-chunk legacy). *Test:* unit (UNLINK exact keyset, zero
  scans when meta present).
- **MPU-56** — ListMPU: single `MGET` of `aborted_mpu:*`. ListParts: **new** `list_parts_for_version_paged.sql`
  (`part_number > $ ORDER BY LIMIT max+1` probe) — **do not** add LIMIT to the shared query (abort/complete need
  the full set). *Break:* ListParts `IsTruncated`/`NextPartNumberMarker` is a client contract — reproduce exactly.
  *Test:* unit (max-parts=2 over 5 → truncated, correct marker, resume); e2e RECOMMENDED (SDK pagination).
- **HD-2** — stop the write-on-read: HEAD must not call `ensure_by_main_account` (INSERT-on-conflict). Gate like
  GET (`account.main_account != "anonymous"`) → read-only `get_user` with create-on-miss, or drop entirely (the
  gateway already authenticated; no FK dependency on `users`). *Blast:* change **only the HEAD callsite**, not the
  shared query. *Test:* unit (no INSERT on existing/anonymous HEAD); new-account HEAD still 200.
- **HD-4 (+HD-5, subsumes HD-3)** — new `bucket_name`-keyed light `get_object_by_path` variant returning
  size/md5/content-type/metadata/version/`append_version`/multipart (+ `LEFT JOIN LATERAL` for the Arion file
  hash, HD-5); repoint HEAD only. Drops the `JSON_AGG download_chunks` + mpu subquery + the always-firing
  append-version fallback JOIN. *Blast:* **do not repoint GET** (needs `download_chunks`/`is_public`). Replicate
  the incomplete-multipart-placeholder skip predicate. *Break:* LOW — HEAD headers must be byte-identical.
  *Test:* header-diff (Content-Length/ETag/Last-Modified/x-amz-meta-*/append-version) for simple+multipart+appended;
  **e2e: REQUIRED** (HEAD header contract).
- **HD-78** — drop the DeleteObject pre-check (`soft_delete_object` RETURNING already distinguishes → 204); flip
  DeleteObjects/ListBuckets `pretty_print=False`. *Break:* NO functionally; grep `tests/` for exact-XML golden
  files and update whitespace.
- **RD-5** — one `chunks_exist_batch` per part (off-loop, already `to_thread`'d) instead of per-chunk
  synchronous `chunk_exists` on the loop (`downloader.py:151`). *Test:* unit (only missing chunks fetched).
- **RD-6 / RQ-3** — pipeline the per-part coalesce `SET NX`; **skip CID resolution entirely for Arion/v5**
  (downloader reads only `spec.index`, never `spec.cid`/`cipher_size_bytes`). Gate the skip on the **resolved
  backend set**, not a global flag, so a real IPFS backend still resolves CIDs. *Break:* LOW (mixed-backend
  guard). *Test:* unit (Arion object → `cid=None` specs, zero `get_part_chunks_by_object_and_number`; IPFS still
  resolves); e2e RECOMMENDED (cold Arion GET streams correct bytes indices-only).
- **LS-45** — wrap `_collect_page` in one `pool.acquire()`; `list_buckets` `pretty_print=False`.
- **AP-2** — gate `set_object_version_address` behind `address IS NULL` at the **callsite** (append.py:151 /
  multipart.py:1132), not the shared query; fold the append reservation's `MAX(part_number)+1` + upload-row lookup
  into the `FOR UPDATE` txn. *Blast:* **the Rust drain reads `object_versions.address`** — the NULL-guard writes
  exactly when needed, but prove (a) address is monotonic and (b) every drainable version gets a non-NULL address
  on first write, against the drain's read query. *Break:* MEDIUM (external drain consumer). *Test:* integration
  (address UPDATE at most once; legacy NULL still filled); **e2e: REQUIRED** (append + drain pickup — coordinate
  with the drain repo per the `drain↔api storage contract`).

### Wave 2 — KEK singleflight + MPU lock (coalescing; correctness-sensitive but low-risk fixes)

- **KM-1** (P1) — mirror the A14 singleflight into `get_bucket_kek_bytes` (`kek_service.py:524-562`): after the DB
  fetch, `async with _kek_unwrap_lock(bucket_id, kek_id):` and **re-check `_get_cached_kek` inside the lock** before
  `_unwrap_kek`. ~6 lines; helper + cache already exist. *Blast:* every GET/HEAD/Range/streaming-copy DEK unwrap.
  Reduces (does not eliminate) the KMS-blackhole keystore-pool pinning. *Break:* NO. *Test:* unit (N cold concurrent
  gets → `_unwrap_kek` called once; two distinct kek_ids → two unwraps — no collapse). Property-test: unwrap count ≤
  distinct cold `(bucket,kek)`.
- **KM-2** (P2) — per-bucket `asyncio.Lock` around fetch-or-create in `get_or_create_active_bucket_kek`
  (`:482-521`), re-check cache inside; **keep the `23505` unique-index handler** as the cross-pod backstop. *Test:*
  unit (N first-PUTs → one `_create_wrapped_kek`; cross-pod 23505 → re-fetch winner).
- **KM-3** (P3) — make `_get_cached_active_kek_id` sliding (or fold id+plaintext into one cache entry). *Break:* NO
  functionally; **behavioral note** — sliding lengthens rotation propagation on always-hot buckets. Reads are always
  correct (per-version stored `kek_id`); only new writes bind the active KEK. If rotation is ever implemented, add a
  cache-invalidation hook. *Test:* unit (repeat within TTL keeps entry alive; genuinely-expired not resurrected).
- **MPU-1** (P2, corrected) — non-locking `SELECT storage_version, kek_id, wrapped_dek` first in
  `_ensure_and_get_v5_dek` (`:513`); return unwrapped DEK when envelope present and `rotate=False`. Escalate to the
  `FOR UPDATE` txn **only** when `kek_id`/`wrapped_dek` NULL or `rotate=True`, keeping the in-lock re-check
  (`:532`) so the first-part race converges on one DEK. *Blast:* MPU parts + S4 append. *Break:* NO — behavior
  identical for correctness; removing the lock on the NULL/rotate path would cause two-DEK data loss (do not).
  *Test:* integration (N concurrent parts → exactly one DEK envelope, all decrypt; first-part race → single DEK;
  parts 2..N take the lock-free path); **e2e: REQUIRED** (parallel-part MPU round-trip).
- **RQ-4** (P3) — on first-chunk-timeout (`object_reader.py:349`), compare-and-delete the coalesce lock keyed on
  the **same ray token** (mirror `downloader.py:310`) per part, so the next GET re-enqueues instead of waiting out
  the 600 s TTL. *Blast:* must match the downloader's lock-key format exactly. *Break:* NO (CAD only deletes the
  timing-out streamer's own lock). *Test:* unit (timeout → own lock CAD-deleted → re-enqueue; foreign-token lock
  untouched). Needs the live pub/sub key-contract test from §2.

### Wave 3 — Crypto offload + pub/sub coalescing (highest correctness care; §0 hazards apply)

- **RD-2** (P1) — offload AES-GCM decrypt to the shared crypto executor; fold decrypt into the pipelined `_fetch`
  task so decrypt(N+1) overlaps send(N); **await/yield in plan order**; bound in-flight to `prefetch+1`. Keep the
  guards (`require_supported_storage_version`, `key_bytes is None` legacy passthrough) on the loop. Auth-tag failure
  must still propagate and break the stream; first-chunk timeout must still map to 503. *Break:* NO (wire format
  unchanged). *Test:* unit (slow early-chunk decrypt still yields in order; decrypt raise on chunk k aborts, no
  k+1); loop-not-blocked (N concurrent GETs wall-time << N×single-decrypt). **e2e: REQUIRED** (parallel large-object
  GET + range fan, byte-exact). **Property-test:** `GET(range) == plaintext[start:end]` for random sizes/ranges at
  prefetch 0/1/4. Needs the prefetch>0 e2e from §2.
- **WU-1** (P1) — offload AES-GCM encrypt to the shared crypto executor in both streaming write paths
  (`put_simple_stream_full`, `mpu_upload_part_stream`). **md5 stays inline+sequential** (running hash — ETag
  correctness). **`chunk_cipher_sizes` recorded by index** (pre-size the list / carry `(index, ct)`), never
  append-order. Encrypt uses the global `next_chunk_index` assigned on the loop before dispatch. Bound in-flight to
  ~16. An encrypt-task exception is fatal (no partial object finalized). *Break:* NO (stored ciphertext identical).
  *Test:* unit (reordered encrypt completion → correct per-index sizes + md5==full-plaintext md5; mid-stream raise →
  no serveable version). **e2e: REQUIRED** (PUT→GET + parallel-part MPU→Complete→GET byte-exact, ETag==md5).
  **Property-test:** random sizes (chunk multiples, off-by-one, empty final) → PUT/GET identity, ETag==md5.
- **RQ-1** (P1 mechanism; impact P3) — one subscription per stream/part instead of per cold chunk
  (`notifier.py:64-72`). `psubscribe notify:...:part:{pn}:chunk:*` (or subscribe planned channels up front) and
  demux to per-chunk `asyncio.Event`s; **preserve the post-subscribe FS re-check race guard** and the transient-miss
  retry. *Break:* **YES (correctness-sensitive)** — the demux/race guard is the subtlest change in the whole plan; a
  bug hangs a stream (missed wakeup) or leaks subscriptions. Ship behind `HIPPIUS_STREAM_SINGLE_SUBSCRIPTION` with
  fallback to per-chunk. *Test:* unit (one subscription serves N out-of-order chunks; chunk lands between subscribe
  and wait still resolves; terminal miss still raises); **e2e: REQUIRED** (`test_GetObject_Range.py` cold). Needs the
  live pub/sub key-contract test from §2.
- **RQ-2** — folds into RQ-1 (17-deep prefetch now costs 1 subscription). Independently, reconcile the prefetch
  default with intent (see CF-3). *Test:* pin the effective prefetch value passed into `stream_plan`.
- **KM-5** (P3) — only if RD-2/WU-1 already touch this region: resolve the adapter once per object and reuse
  `AESGCM(key)` across an object's chunks (key is constant per version). **Do not precompute nonces.** *Test:*
  micro-assert reused-cipher ciphertext == per-chunk-constructed ciphertext.

### Wave 4 — Connection/pool/config tuning (capacity-gated; not CI-green-sufficient)

**These require an Arion capacity check and/or a load benchmark before prod, not just green CI.** Size the joint
clusters together (see §4).

- **RD-4 + NET-1** — hoist a **single per-pod** downloader semaphore (`downloader.py:133` is per-DCR today → up to
  10×20=200 vs a 100-conn pool) sized to the download client's `max_connections`; give `ArionClient` **per-role**
  `httpx.Limits` (download `max_connections≥256, max_keepalive=128, keepalive_expiry=30`; upload/unpin ~16; the
  client is shared by 4 roles — **parameterize, don't hardcode**). Consider a two-level cap so one large sequential
  DCR can't starve range DCRs. *Break:* **YES (behavioral/capacity)** — a semaphore *smaller* than today regresses
  throughput; *larger* than the pool triggers 60 s PoolTimeout; a 256-wide pool × replicas can overwhelm Arion.
  *Test:* unit (one semaphore per pod; concurrency ≤ cap; per-role Limits). **Load benchmark + Arion capacity check
  REQUIRED** (range-heavy mix + one large DCR; no starvation, no PoolTimeout). Env: `DOWNLOADER_GLOBAL_FETCH_CONCURRENCY`
  (start ~200 = current), per-role `Limits` envs. Watch: httpx pool-timeout counter, Arion 429/5xx, `ttfb_ms`.
- **NET-2** — add `httpx[http2]` (pin exact; `pip-audit`); enable `http2=True` on **KMS + gateway-forward +
  Hippius-API first**; **benchmark Arion before enabling there** (H2 can hurt large sequential/lossy transfer).
  *Break:* YES (new dep + protocol) → per-client env flags, Arion default off. *Test:* e2e green with H2 on
  gateway/KMS/API; **Arion H2 benchmark REQUIRED**.
- **NET-3** — KMS `limits=httpx.Limits(max_keepalive_connections=10, keepalive_expiry=300)` (mTLS handshake is the
  #1 cold-path cost). *Break:* NO — verify OVH KMS tolerates long-idle mTLS keepalive. *Test:* unit (limits set);
  staging cold-vs-warm KMS latency check.
- **NET-4** — gateway forward `max_keepalive_connections=100` (=`max_connections`), `keepalive_expiry=30-60`.
  *Break:* NO. *Test:* unit + gateway→API latency benchmark (recommended).
- **NET-5** — hold one shared `HippiusApiClient` in gateway `app.state` (like `arion_client`); pass into
  `cached_auth`; close on shutdown (stop building a client per auth-cache miss). *Break:* NO (lifecycle). *Test:*
  unit (no new client constructed on miss).
- **WU-4 / NET-6** — exponential backoff + jitter in both `retry_on_error` decorators (reuse the existing
  `workers/errors.compute_backoff_ms`); **release `_put_semaphore` across retry sleeps** (acquire→one POST→release→
  sleep→re-acquire). **Note:** the decorator params are hardcoded and *separate* from the deployed configmap
  `MAX_ATTEMPTS=2/BASE_MS=100/MAX_MS=500` (that governs the request-level retry) — config-drive the decorator and
  reconcile so the two layers don't compound. *Break:* YES (mild behavioral) — idempotency holds (content-addressed
  CID, `ON CONFLICT` insert). *Test:* unit (exponential+jittered sequence; semaphore released during sleep);
  toxiproxy e2e recommended (slot not frozen).
- **DB-1** (P3, corrected) — make the downloader pool config-driven (`downloader.py:389` hardcoded min=2/max=20);
  the downloader opens **no** KEK pool (the report's exhaustion math was inflated). Document the aggregate
  `Σ(replicas × pool_max)` budget vs Postgres `max_connections` (prior "+96 over max" incident). *Break:* YES
  (latent capacity) → env `HIPPIUS_DOWNLOADER_DB_POOL_MAX` default 20 = current. *Test:* unit (pool created with
  config values).
- **CF-1** — default chunk size 4 MiB → 8 MiB (`config.py:227`). Per-write-safe (`parts.chunk_size_bytes` is
  per-part; old objects keep 4 MiB). *Break:* **YES (memory/capacity)** — buffers scale linearly at every stage;
  with prefetch 16 that's ~136 MiB/stream → **pair with CF-3** (drop prefetch) in the same rollout; range reads
  over-fetch a full 8 MiB chunk. *Test:* round-trip at 8 MiB + mixed-version object; **benchmark REQUIRED** (throughput
  + api-pod RSS + Arion egress). Env `HIPPIUS_CHUNK_SIZE_BYTES`.
- **CF-2** — raise `UPLOADER_MAX_INFLIGHT` 4→~8 **jointly** with `arion_upload_concurrency` (8→8-16) and
  `uploader_db_pool_max` (12→12-16) — raising one alone just queues. *Break:* YES (capacity) → ramp; check
  `pods × arion_upload_concurrency` vs Arion and `pods × db_pool` vs Postgres. Depends on **WU-4** (semaphore
  release makes the added concurrency real). *Test:* unit (pool clamp); load benchmark REQUIRED.
- **CF-3** — lower prefetch default 16→4-8 until RD-2 lands; expose the hardcoded writer queue
  `maxsize=16` (`object_writer.py:301` **and** `:698`) as `HIPPIUS_WRITE_QUEUE_MAXSIZE`. *Break:* behavioral
  (throughput vs memory). Ship with CF-1.
- **RQ-5** (P3) — **defer.** Single-item `brpop` is almost certainly not the bottleneck (workers are Arion-bound).
  Only batch via `LMPOP` if a metric proves queue-dequeue is the constraint. Per "no speculative features."

### Wave 5 — Gateway fixed floor (after GW-2 makes it measurable)

- **GW-4** (biggest clean win) — skip `fetch_account_by_main_address` for GET/HEAD on the access-key path; still
  set `request.state.account_id` + a lightweight `account`. **Confirmed safe:** `has_credits`/`upload`/`delete` are
  set in `parse_internal_headers` but **never read** anywhere on any method; credit gating only runs for
  PUT/POST/DELETE (which still fetch). *Blast:* access-key GET/HEAD; audit log will show the sub-account instead of
  resolved main account on reads (acceptable, note it). *Break:* NO (no read has a credit gate). *Test:* unit (no
  fetch on GET/HEAD, fetch on mutating; account_id still set); **e2e: REQUIRED** (credit-gated PUT still 402s;
  authenticated GET succeeds with redis-accounts forced to error).
- **GW-1** (constrained) — within `acl_middleware`, a **speculative MGET** of the two `redis-acl` lookups
  (bucket-meta + bucket-ACL grants), discarding grants on master-bypass hits; combine with GW-4's dropped account
  GET. **Do not** attempt the auth+sub-scope MGET or merge middlewares (breaks the principal-before-decision
  invariant). *Break:* NO iff the discarded-grant path is provably decision-neutral. *Test:* unit (master-bypass
  decision identical with/without the speculative fetch; redis-acl outage still falls through to DB); **e2e:
  REQUIRED** (owner GRANTED, master bypass, cross-account grant, DENIED 403); add a per-instance Redis-RT metric.
- **GW-5** (P3) — in-process TTL LRU (keyed by access_key, TTL ≤ `AUTH_CACHE_TTL_SECONDS`) caching the *decrypted
  secret* only. **Signature verification stays mandatory** (canonical request + HMAC chain + `compare_digest` every
  request). *Break:* NO. Tradeoff: plaintext secret in memory for ≤ TTL — bounded LRU, never logged. *Test:* unit
  (valid verifies on warm cache; tampered still 403; entry expires within auth TTL).
- **GW-3** (P3, corrected) — optional. Reuse the effective ACL from the real `check_permission` instead of a
  separate `check_permission(None)`; **do not** simply move it after the master bypass (the pre-bypass ordering is
  intentional so master/owner reads still warm the ATS cache for public objects). No-op in default config
  (`ats_cache_endpoints` empty). Low priority.
- **GW-67** (P3) — `model_construct()` for **gateway-private** trusted cache payloads (auth/ACL/bucket-meta only —
  never client-influenced); guard `grants_summary` behind `logger.isEnabledFor(INFO)`; parse query params once onto
  `request.state`; reuse the ray-id logger adapter. *Break:* NO for authz; **minor observability** — confirm no Loki
  dashboard parses the `"ACL check: … result="` INFO line before gating it. *Test:* unit (construct==validate for
  representative payloads; ACL decision unchanged with the log guard).

### Wave 6 — ListObjects skip-scan + index (medium risk; differential-test-gated)

- **LS-3** (do first — helps LS-1) — `CREATE INDEX CONCURRENTLY idx_objects_bucket_prefix_active ON
  objects(bucket_id, object_key) WHERE deleted_at IS NULL;` then `DROP INDEX CONCURRENTLY` the non-partial one.
  *Migration:* **must be CONCURRENTLY, out-of-band** (can't run in a txn) — follow the established pattern
  (`k8s/cleanup-indexes-staging-apply.yaml` seeds `schema_migrations` so dbmate skips it; a standalone job runs the
  DDL). Verify `pg_index.indisvalid` before dropping. Rollback: recreate non-partial CONCURRENTLY. *Blast:* audit
  `console_list_objects.sql`'s predicate before dropping the full index (all main list/get queries filter
  `deleted_at IS NULL`). Zero-downtime.
- **LS-2** — pass an explicit successor param (`_prefix_resume(prefix)` exists) and add `AND ($5::text IS NULL OR
  o.object_key < $5::text)` to `list_objects.sql`. *Break:* NO (byte-collation-safe tightening). Verify with
  `EXPLAIN (ANALYZE)` under bound params that both index bounds are used. **Do not regress** the load-bearing
  LATERAL/no-`COLLATE "C"` shape (prior 92× prod fix).
- **LS-1** (highest complexity here) — push the distinct-common-prefix rollup into SQL (loose index skip-scan /
  recursive CTE or `DISTINCT ON`) in a **new** `list_objects_delimited.sql`; stop fetching-then-discarding content
  rows in Python. *Break:* **MEDIUM** — `CommonPrefixes`/`Contents`/`IsTruncated`/`NextContinuationToken`/`KeyCount`
  and StartAfter `cp_floor` suppression are a strict client contract; any successor off-by-one corrupts pagination.
  *Test:* **differential/property tests** — byte-identical XML vs the current implementation across randomized
  `prefix`/`delimiter`/`max-keys`/`start-after`/continuation sequences; **e2e: REQUIRED** (`aws s3 ls` with `/`).
  Build the differential harness from §2 first.

### Wave 7 — Rust drain (owner-coordinated; durability-critical subset needs sign-off)

Low-risk, no sign-off:

- **DR-3** — raise `HASH_BUF_BYTES` 64 KiB → 512 KiB-1 MiB (`localfs.rs:33`); cuts blocking-pool dispatches
  8-16×. *Break:* NO. *Test:* add a chunk-larger-than-buffer case (current tests never loop the buffer).
- **DR-5** (corrected) — delete **only** the near-dead `status` SELECT fast-path (`partdrain.rs:437-443`);
  `claim_part` gating makes the `==Replicated` branch unreachable. **Keep both UPDATEs** (commit-before-enqueue is
  load-bearing for in-flight MPU parts). Retain the `AlreadyReplicated` enum variant or update its tests. *Break:*
  NO. *Test:* keep the two `worker.rs` decoupling tests green.
- **DR-6** — fix the stale `chunk_landed` doc now (`config.rs:16-17` vs `runtime.rs:175`). Optionally add a
  best-effort redis-queues wake key (API `SET`/`PUBLISH` at part-land; `run_drain` `select!`s on it with the 5 s
  poll as backstop) — off the read path, ≤5 s latency only, fire-and-forget so it never blocks a PUT. *Break:* NO
  (poll backstop). *Test:* unit (wake breaks the sleep; dropped wake still drains via poll).
- **DR-78** — batch the post-MPU enqueue sweep (`runtime.rs:381`): cache `UploadContext` per `(object_id,
  version)` (siblings differ only by `part_number`), pipeline the LPUSHes, `buffer_unordered` the per-part
  enqueue+stamp. **Preserve stamp-after-successful-push per part** (batching the stamp before confirming each LPUSH
  = silent non-upload loss, same class as the rejected DR-5 merge). *Break:* NO iff invariant held. *Test:* one
  context load for N same-object parts; mid-batch failure leaves exactly the failed part unstamped; golden-wire test
  green.

Durability-critical — **DR-1 owner sign-off + staging CephFS integration test mandatory:**

- **DR-1** — the readback re-hash is the **only** torn-write detector (copy_hash digests the SSD source; pool_hash
  digests the written Ceph copy). Sampling **weakens** detection by design. Ship **Option A** (always readback
  endpoints chunk 0 + last; interior `i % N == 0`) **dark, default stride 1** (behavior unchanged) behind
  `CEPHOR_READBACK_SAMPLE_STRIDE`, so the path exists and is tested but detection is unchanged until an operator
  opts in with eyes open. *Test:* endpoints always read back regardless of stride; a corrupt *sampled* chunk is
  caught, a corrupt *un-sampled* chunk is (documentedly) missed at stride>1 (encode the weakened-guarantee
  contract); **staging CephFS: inject a real torn write, confirm detection at stride 1**. **DR-1 durability-posture
  sign-off MANDATORY.**
- **DR-4** — `finalize_part` fsyncs the shared pool **root inode** on every part (`localfs.rs:494-509`) — N MDS
  round-trips on one hot inode. Fix: track created ancestor dirs, fsync only those + the part dir; sync root once at
  startup / on new top-level object dir. *Break:* **YES (crash-consistency) if done wrong** — a missed
  newly-created ancestor orphans a part on crash. Be conservative: when unsure an ancestor is new, fsync it. *Test:*
  extend `finalize_part_fsyncs_*`; two parts of one object → only part dirs fsynced (fsync counter); **staging
  CephFS: `kill -9` mid-drain of a new object → object reachable after remount** (the network-mount dir-fsync
  caveat is exactly why staging is mandatory). Durability sign-off.

Deferred:

- **DR-2** — by design; profile-gated only. Confirm `sha2` `asm` feature is enabled first (cheaper lever). Revisit
  only if a flamegraph shows the drain task CPU-bound above the default rate.

### Design track (needs a decision before implementation)

- **CP-1 → v6 (Structural, P1 impact)** — the copy fast path is dead because per-chunk AAD binds `object_id`, so
  CID-reuse copy isn't decryptable at the dest; **every copy is O(size)**. Fix = a **v6 suite** binding per-chunk
  AAD/nonce to a **copy-stable `content_key_id`** (drop `object_id`), so copy = duplicate `chunk_backend`/`part_chunks`
  rows + rewrap the DEK, zero byte copy. **Critical crypto invariant:** copied ciphertext reuses `(DEK, nonce)`
  across source and dest — **safe only because the plaintext is identical**; therefore copied ciphertext must be
  **frozen immutable**, and any append/overwrite must allocate a **new DEK/version** (else nonce reuse across
  differing plaintexts = catastrophic GCM break). Reader selects binding by `enc_suite_id` (v5 keeps object_id
  binding, still readable — no rewrite). Needs: `object_versions.content_key_id` column, storage-version policy,
  unpin/janitor shared-CID dedup (don't delete a CID still referenced by the source), and the FS-backfill guard
  (dest has `chunk_backend` rows but no FS entries → cold-fill or defined 503, never hang). **CP-3** (batch the dead
  `copy_chunk_cids` per-chunk upsert into `INSERT...SELECT`; pass `dest_part_id` through) lands **with** CP-1.
  *Test:* **e2e REQUIRED** (v6 PUT→copy→GET byte-exact across object_ids; copied-then-appended allocates a new DEK;
  source-unpin then dest-GET is defined). **Wycheproof-style property test:** no `(key, nonce)` pair ever encrypts
  two different plaintexts across any copy+mutation sequence. **Design sign-off MANDATORY** (new storage/suite
  version + schema + crypto invariant).
- **AP-1 (P2)** — append CAS validates only at finalize, so K concurrent same-version appends each do the full
  encrypt+write and K-1 lose. **Recommended ship = the cheap interim** (pre-write `append_version` re-check) — it is
  breaking-change NO. The **optimistic reserve-and-bump** is breaking-change YES: moving the bump into the
  reservation txn **splits** `append_version` from the `size_bytes`/`md5`/`etag` update that must stay atomic with
  it, and strands the counter on a crash-between-reserve-and-finalize → needs a reaper for orphaned reserved parts.
  Only pursue the optimistic path as a deliberate design item. **Adjacent (flag, don't deepen):** `append_stream`
  never validates `objects.current_object_version`, so a concurrent plain PUT can silently supersede an append —
  ideally validate `cov` in the same reservation txn. *Test:* two concurrent same-version appends → one 200/one 412,
  metadata consistent; **e2e REQUIRED** (concurrent append fan, GET byte-exact, ETag consistent).

---

## 4. Joint-sizing clusters (never tune one in isolation)

1. **Download concurrency** — RD-4 semaphore ≤ NET-1 download `max_connections`; `downloader_max_inflight` stops
   being a concurrency multiplier. One benchmark + Arion capacity check covers all three.
2. **Upload throughput** — CF-2 (`uploader_max_inflight` + `arion_upload_concurrency` + `uploader_db_pool_max`)
   move together; WU-4's semaphore-release makes the added concurrency real; DB-1's aggregate audit bounds Postgres
   connections.
3. **Memory** — per-stream = `chunk_size × (prefetch+1)`; per-PUT = `chunk_size × write_queue_maxsize`. CF-1 (8 MiB)
   **must** ship with CF-3 (lower prefetch) and RQ-1 (fold the 17× pub/sub cost).
4. **HTTP/2** — NET-2 changes what keepalive counts mean (multiplexing); reason about it with NET-1/3/4 per client;
   Arion H2 stays flag+benchmark-gated.
5. **Coalescing/pub-sub** — RQ-1/RQ-2/RQ-4/RD-6 are all part-granular; keep the part-key semantics identical across
   streamer coalesce and downloader release.

---

## 5. Merge gates summary

- **e2e REQUIRED** before merge: RD-3, MPU-3, HD-4, AP-2, MPU-1, RD-2, WU-1, RQ-1, GW-4, GW-1, LS-1, CP-1, AP-1
  (plus the §2 harness additions those depend on).
- **Load benchmark / Arion capacity check** before prod (CI-green insufficient): RD-4, NET-1, NET-2 (Arion), WU-4,
  CF-1, CF-2.
- **Staging CephFS integration test + owner sign-off**: DR-1, DR-4.
- **Design sign-off**: CP-1 (v6 crypto/schema), AP-1 optimistic variant.
- **Breaking-change = YES → env-flagged with named rollback**: RD-4/NET-1, NET-2, WU-4, DB-1, CF-1/2/3, RQ-1
  (`HIPPIUS_STREAM_SINGLE_SUBSCRIPTION`), AP-2, GW-2 (dashboard re-baseline), CP-1, AP-1.

---

## 6. Recommended merge order

Wave 0 → Wave 1 → Wave 2 → (Wave 5 gateway in parallel, once GW-2 lands) → Wave 3 (crypto, after the §2 e2e
harness) → Wave 6 (list, after the differential harness) → Wave 4 (capacity tuning, benchmark-gated) → Wave 7
(drain, owner-coordinated) → Design track (CP-1/AP-1) last. Waves 1/2/5 are independent and can run concurrently
across contributors; Wave 3 and Wave 4 memory items are coupled (CF-1↔CF-3↔RQ-1) and must be coordinated.
