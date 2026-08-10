# Code review — SSD read tier (PRs #398 + #400)

Adversarial review of the work that turned the ingest NVMe from a write-staging buffer into a
read tier. Two agents review independently and then review each other; this file is the shared
record.

**Scope reviewed:** `bfd1c050..HEAD` (staging), i.e. PR #398 (`814f404a`) + PR #400 (`6152e3a3`).
80 files, +11,982 / −448. Design intent read from
[docs/plans/2026-08-06-ssd-read-tier-retention-and-residency-allocator.md](docs/plans/2026-08-06-ssd-read-tier-retention-and-residency-allocator.md)
and [docs/plans/2026-08-07-ssd-read-tier-hardening.md](docs/plans/2026-08-07-ssd-read-tier-hardening.md).

**Verification actually run for this review** (evidence, not assertion):

| Check | Result | Who |
|---|---|---|
| `pytest tests/unit -q` | **2477 passed, 37 skipped**, 2 warnings, 17.95s | Agent A |
| `cargo test -p hippius-drain-core --lib` | **225 passed, 0 failed** | Agent A |
| Route-precedence + gateway auth chain traced by reading | see A-1 | Agent A; **re-confirmed Agent B** |
| `redis.asyncio` pipeline semantics for `landed.py` | verified against the installed client — `pipe.lpush()` returns the pipeline, not a coroutine; the unawaited-command pattern is correct | Agent A |
| Peer registry URL handling / SSRF surface | no validation on resolved URL; any Redis `SET` wins — see B-1 | Agent B |
| MPU re-upload × `record_landed` × reconciler | no version bump; status never reset from `replicated`; reconciler leaves `replicated_orphan` — see B-2 | Agent B |
| `ip_whitelist` vs RFC1918 | `startswith("172.")` admits public 172.x outside `/12` — see B-3 | Agent B |
| Hardening §0-M2 shared-disk startup guard | **not in tree** — see B-5 | Agent B; **independently re-confirmed Agent A** |
| Pre-retention `ssd_reclaim` `Replicated` arm (`git show dcbe9ef1`) | unlinked unconditionally past `graces.replicated` — settles the B-2 differential | Agent A; **re-confirmed Agent B** |
| Which Redis backs `PeerRegistry` vs `cached_auth` | **same instance** (`config.redis_url`) — settles B-1 severity | Agent A; **re-confirmed Agent B** |
| `mpu_cleanup.wake/fail_version_replication` vs a `replicated` row | neither re-drives it — closes the last gap in B-2's mechanism | Agent A; **re-confirmed Agent B** |
| Cross-review consensus | B-1 → P2; B-2 out of promotion gate; shared fix order in §7 | both |

---

## Review protocol

- **Agent A** = this reviewer. Findings prefixed `A-n`. Written 2026-08-07.
- **Agent B** = second reviewer. Findings prefixed `B-n`. Written 2026-08-07 in §3;
  cross-review of A-findings in §4; overall assessment in §7.
- Cross-review in §4: for each of the other agent's findings, record one of
  **CONFIRMED** / **DISPUTED** / **PARTIALLY CONFIRMED** with the evidence that settles it.
  Disputing without a code reference does not count.
- Severity: **P0** ship-blocker · **P1** fix before prod promotion · **P2** fix soon · **P3** nit.
- Do not edit the other agent's findings in place. Reply in §4.

---

## 1. Overall assessment (Agent A)

The engineering is well above the bar for this repo. The durability invariant ("never evict a
part whose only durable copy is the SSD") is defended in three independent places — the worklist
`JOIN … AND s.status = 'replicated'`, the orchestrator's per-candidate re-check, and a counter
wired to an alert — and the reasoning for each is written down at the point of use. The migrations
are unusually careful (`lock_timeout` before DDL, an explicit rejection of `CONCURRENTLY` with the
reason, no backfill by design). The plan documents are genuinely self-critical: several findings a
reviewer would raise (`F0` retention has no flag; `F2a` `starved` cries wolf; `M3` the promote
floor deadlocks above the evict target) were found and corrected by the authors before I got here.
Test naming is behavioural throughout — `test_a_single_readers_prefetch_window_never_sheds_against_an_idle_peer`
is a property, not a function name.

What that quality does **not** cover is the new **trust boundary**. The peer-serve endpoint was
threat-modelled against "any pod on the cluster pod network" (retention plan §"New internal
surface"), and that model is wrong: the gateway is a pod on that network and it proxies arbitrary
paths from the public internet. A-1 is the finding I would block a prod promotion on.

The second theme is that **two of the loop's four thresholds are now dynamic while the guard that
validates them is static** (A-4), so the control loop the hardening plan spent its longest section
getting right can be silently inverted at runtime — precisely under the drain stall that makes the
allocator raise the reserve.

---

## 2. Findings (Agent A)

### A-1 — P0 · The peer-serve endpoint is reachable **unauthenticated from the public internet**

`GET /internal/parts/{object_id}/{version}/{part}/chunks/{index}`
([hippius_s3/api/internal_parts.py:34](hippius_s3/api/internal_parts.py)) is documented as sitting
behind the api's `ip_whitelist` (10.x/172.x pod network only). The gateway is inside that network
and forwards arbitrary paths, so the whitelist is not the boundary it is believed to be. Full
chain, each step read:

1. Gateway catch-all `@app.api_route("/{path:path}")`
   ([gateway/main.py:192](gateway/main.py)) plus the root-mounted ACL router forward every path.
2. `authenticate_request`: no `Authorization` header + `GET` + path ≠ `/` →
   `AuthResult(is_valid=True, auth_method="anonymous")`
   ([gateway/services/auth_orchestrator.py:63-65](gateway/services/auth_orchestrator.py)).
   `internal` is **not** in `EXEMPT_SEGMENTS`, but it does not need to be — anonymous is a valid
   outcome, not a rejection.
3. `acl_middleware` parses `bucket="internal"`, finds no such bucket, and
   **passes the request through**: `logger.info("Bucket not found in ACL check…"); return await
   call_next(request)` ([gateway/middlewares/acl.py:254](gateway/middlewares/acl.py)).
4. `ForwardService` sends it to the api from a 10.x pod IP → `ip_whitelist_middleware` admits it.
5. `internal_parts_router` is registered **before** `s3_router_new`
   ([hippius_s3/main.py:428-429](hippius_s3/main.py)), and Starlette matches in registration order,
   so the five-segment internal route wins over `/{bucket}/{key:path}`. `public_router` only owns
   `/public/...`, so it does not shadow it.

**Impact.**
- An unauthenticated **existence oracle**: 200 vs 404 tells any internet caller whether a given
  `(object_id, version, part)` is resident on the node the round-robin Service happened to pick.
- Unauthenticated retrieval of raw chunk **ciphertext**. Confidentiality impact is genuinely
  limited — AES-256-GCM under a DEK that never leaves the KMS path — and the plan is right about
  that. The trust-boundary break is the finding, not the plaintext risk.
- Unauthenticated **node-local NVMe read load** and semaphore consumption on the same uvicorn that
  serves ingest.

**The flag does not cover this.** The router is mounted unconditionally in `factory()`, while
`app.state.peer_serve_limiter` is only created when `peer_fetch_enabled and node_name and pod_ip`
([hippius_s3/main.py:179](hippius_s3/main.py)). `internal_parts.py:68-70` treats a missing limiter
as "no cap". So with `HIPPIUS_PEER_FETCH_ENABLED=false` — the prod setting — the endpoint is
**both reachable and unbounded in concurrency**. That directly contradicts the retention plan's
"Ships behind a flag defaulting to off".

**Suggested fixes** (in preference order):
1. Require a shared secret / mTLS on `/internal/*` at the api, injected by the peer client and
   **stripped by the gateway** like every other `X-Hippius-*` header
   ([gateway/services/forward_service.py](gateway/services/forward_service.py) already strips that
   prefix, so an `X-Hippius-Peer-Auth` header gets the property for free).
2. Reject any request whose first path segment is `internal` at the gateway (400 `InvalidBucketName`).
3. Serve the peer endpoint from a separate ASGI app/port that the gateway's backend URL cannot reach.
4. Independently of the above: build `peer_serve_limiter` unconditionally.

**Regression test to add:** a gateway-level test that an unauthenticated `GET /internal/parts/...`
never reaches the api handler, and an api-level test that the internal route takes precedence over
`/{bucket}/{key:path}` (that precedence is load-bearing and currently untested in either direction).

---

### A-2 — P2 · `internal` was not added to `RESERVED_BUCKET_SEGMENTS`

[hippius_s3/reserved_bucket_names.py:4-5](hippius_s3/reserved_bucket_names.py) states the invariant
this work violates, verbatim: *"First path segments that never reach the S3 forwarder as a bucket.
A bucket created under one of these is unreachable by its owner yet permanently holds the globally-unique
name — prod incident 2026-08-03, buckets 'docs' and 'docs2'."*

`internal` is now exactly such a segment, and it is absent from the frozenset. Consequences:

- A user can `CreateBucket internal` today. It succeeds.
- Any object in it whose key matches `parts/{uuid}/{int}/{int}/chunks/{int}` becomes permanently
  unreadable — the api routes it to the peer endpoint instead of `GetObject`.
- The globally-unique name is burned.

The existing guard `test_every_auth_exempt_segment_is_a_reserved_bucket_name` cannot catch this: it
walks the **gateway's** exempt segments, and this is an **api-side route**. The reserved-names
module lists two sources of danger (a gateway route, and an auth exemption); this work introduced a
third — an api route that shadows the S3 catch-all — and neither the module comment nor any test
was extended to cover it.

**Fix:** add `internal` to `RESERVED_BUCKET_SEGMENTS`, extend the module comment with the third
source, and add a test that enumerates api-side non-S3 routes against the frozenset. Run
[hippius_s3/scripts/report_reserved_name_buckets.py](hippius_s3/scripts/report_reserved_name_buckets.py)
against prod and staging to check nobody already owns the name.

---

### A-3 — P1 · One un-unlinkable part halts **all** eviction, silently

[crates/hippius-drain-core/src/ssd_evict.rs:319](crates/hippius-drain-core/src/ssd_evict.rs):

```rust
remover.unlink_part(&candidate.part).await.map_err(EvictionError::Remove)?;
```

The `?` returns out of the page loop **before** `log.mark_evicted(&evicted)` at line 327.
`mark_evicted` → `drop_residency` (a DELETE) is the only thing that advances the cursor
([store.rs:1193-1194, 1295](crates/hippius-drain-core/src/store.rs)), and the worklist is ordered
`COALESCE(last_read_at, resident_at)` — a stable oldest-first cursor. So a part that persistently
fails `remove_dir_all` sits at the head of that cursor and **every subsequent pass dies on the same
row**, having freed nothing.

`evict_once` handles this with `Err(err) => tracing::warn!(…, "eviction pass failed; retrying next
poll")` ([runtime.rs:391](crates/hippius-drain-agent/src/runtime.rs)) — and neither `starved` nor
`skipped_unreplicated` is set, because the report is never produced. The module's own doc says this
worker is *"the only thing standing between a retained read cache and a full ingest disk"*, and the
hardening plan's alert list (§4) is built on `starved` and `evict_blocked_unreplicated`. Both stay
silent through this failure mode. Only the third alert (`drain_ssd_free_bytes` approaching the
threshold) would fire, and only after the disk is already near the 503 cliff.

`unlink_part` maps `NotFound → Ok` ([localfs.rs:425-431](crates/hippius-drain-agent/src/localfs.rs)),
so the plausible triggers are `EIO`, `EACCES`, `EROFS`, and — worth calling out — **`ENOTEMPTY`
from a concurrent promotion**: `_promote_chunk` writes `*.tmp.<uuid>` and renames into the very
part directory the evictor is recursively removing, on the same mount, from a different process.
A transient version of this is self-healing; a persistent one is not, and nothing distinguishes them.

**Fix:** `mark_evicted` what has already been unlinked on the page before propagating; better, count
a per-part removal failure (`remove_failed`) and continue to the next candidate rather than aborting
the pass. Add the unit test: *a candidate whose unlink fails does not prevent the rest of the page
from being evicted and marked.*

---

### A-4 — P1 · The promote/evict band is validated once at startup against a floor the evictor may not be using

The hardening plan's §0-M3 and Phase A are the most carefully-reasoned part of this work: the
promote floor must sit strictly inside the evictor's band, `evict_reserve < promote_floor <
evict_reserve + evict_headroom`, or the loop chatters or deadlocks. `validate_promotion_band`
enforces it at wiring time ([fs_pressure.py:106-153](hippius_s3/fs_pressure.py), called from
[cache/__init__.py](hippius_s3/cache/__init__.py)).

It validates against **hardcoded constants**:

```python
EVICT_RESERVE_RATIO = 0.150
EVICT_HEADROOM_RATIO = 0.050
```
([fs_pressure.py:98-99](hippius_s3/fs_pressure.py)), described as "the drain agent's shipped
eviction policy… mirrored here and pinned by a test on each side."

But the deployed evictor's reserve is **not** that constant. Phase 4 has the allocator publish a
per-node `reserve_permille`, and it **wins**:

```rust
let reserve = allocated_reserve_permille.unwrap_or(policy.reserve_permille);
```
([runtime.rs:309](crates/hippius-drain-agent/src/runtime.rs)); the staging manifest even labels
`CEPHOR_EVICT_RESERVE_PERMILLE=150` as *"Fallback floor, used only until the allocator publishes a
per-node reserve."* `alloc.rs` interpolates that reserve between `base_reserve_permille` (150) and
`max_reserve_permille` (400).

At the max, the evictor's reserve is 0.400 and its target 0.450, while the promote floor stays
0.175. The required ordering `evict_reserve < promote_floor` **inverts**: promotion keeps writing
all the way down to 17.5% free while the evictor is armed and trying to hold 40%. That is exactly
the "promotion is an unthrottled writer racing the evictor" failure Phase A exists to prevent — and
it re-appears *only* when the allocator has decided the drain is in trouble, i.e. at the worst
possible moment, with a startup-time check that already passed and no runtime detector.

Note this is not hypothetical drift between two config files; it is a deliberate, live runtime
override of one of the two values the check depends on.

**Fix options:** derive the promote floor from the *published* reserve (the api can read the same
`cephor:alloc:*` key, or the agent can publish the effective reserve alongside `fs_cache:pressure`);
or clamp `max_reserve_permille` so the ordering holds across the whole interpolation range and
assert **that** in the shared test; or re-evaluate the band periodically and log/alert on inversion.
At minimum, document the coupling at
[alloc.rs `reserve_permille`](crates/hippius-drain-core/src/alloc.rs) — nothing on the Rust side
currently hints that a Python startup check depends on this number.

---

### A-5 — P2 · A failed residency claim leaks disk permanently and is the one failure on this path with no counter

[hippius_s3/cache/residency.py:70-88](hippius_s3/cache/residency.py) is admirably honest about the
consequence, and I want to credit that — it explicitly corrects an earlier comment that wrongly
claimed the orphan sweep would collect the copy:

> a replicated part on disk with no residency row has **NO owner** — the evictor is scoped to the
> residency table and cannot see it either. It leaks until some later read promotes the same chunk
> again… Nothing else collects it.

That is correct, and I confirmed it against `reclaim_ssd`, whose `Replicated` arm is
`report.skipped_replicated += 1` and nothing more
([ssd_reclaim.rs:392](crates/hippius-drain-core/src/ssd_reclaim.rs)).

The problem is that this failure — a permanent, unreclaimable SSD leak on the disk that 503s PUTs
when it fills — is recorded as `logger.debug(...)` and nothing else, while the strictly *less*
consequential `last_read_at` stamp failure gets a first-class metric
(`read_recency_writes_total{outcome=failed}`, [read_recency.py:87-89](hippius_s3/cache/read_recency.py)).
The observability effort is inverted relative to the blast radius.

**Fix:** a counter (`promotion_residency_claim_failed_total`, or
`promotion_skipped_total{reason=residency_failed}`), and consider **not writing the chunk at all**
when the claim fails — an unclaimable copy is worse than a cache miss, and the bytes are already
served either way.

---

### A-6 — P2 · The two writers of `cephor_ssd_residency.bytes` disagree, and only one side says so

- Python `ResidencyRecorder` **accumulates**:
  `DO UPDATE SET bytes = cephor_ssd_residency.bytes + EXCLUDED.bytes`
  ([residency.py:57-69](hippius_s3/cache/residency.py)), with ~15 lines explaining why.
- Rust `Store::record_resident` **overwrites**: `DO UPDATE SET bytes = EXCLUDED.bytes`
  ([store.rs:1260-1278](crates/hippius-drain-core/src/store.rs)), with **no comment at all** about
  the divergence.

Phase F of the hardening plan asked to "reconcile the two residency `ON CONFLICT` semantics in a
comment". That landed on the Python side only. Anyone reading `record_resident` sees an ordinary
overwrite upsert with no signal that a second service writes the same row under different rules.

I checked the plan's "Retracted" argument that the divergence is unreachable and it holds **today**,
for a reason worth writing down because it is load-bearing and undocumented: a locally-resident part
is served locally and therefore never promoted, and that is only true because ingest writes every
chunk *and* `meta.json` before the part is readable. A *partially* promoted part (range GET) on a
node that later drain-commits the same part would collide. Nothing in the schema or code enforces
that this stays impossible.

**Fix:** mirror the comment onto `record_resident`, naming `hippius_s3/cache/residency.py` as the
other writer and stating the invariant that keeps them disjoint.

---

### A-7 — P2 · Comment drift in two places where the comment is the safety argument

Both are in code whose whole review value is that the reasoning is written down, which makes a
*wrong* comment worse than none.

**(a) `ssd_evict.rs` module doc, §"Policy vs. mechanism" (lines 29-37):**

> Eviction order is FIFO by `resident_at` — oldest retained first… True LRU needs node-local read
> recency, **which does not exist yet**… promoting FIFO to recency later changes one query, not
> this worker.

Node-local read recency now exists (migration 0017, `cephor_ssd_residency.last_read_at`,
[read_recency.py](hippius_s3/cache/read_recency.py)), and the query *was* changed:
`ORDER BY COALESCE(r.last_read_at, r.resident_at)`
([store.rs:1168](crates/hippius-drain-core/src/store.rs)). The module doc now describes the
superseded design. `ResidentPart::bytes` and `EvictionReport::starved` doc comments are still
accurate; it is only this section.

**(b) `partdrain.rs:554`:**

> …which is why `mark_replicated` stamps `resident_at` in the same statement as the commit.

It does not. `mark_resident` is a **separate statement issued before** `mark_replicated`
([partdrain.rs:528-535](crates/hippius-drain-core/src/partdrain.rs)), and `mark_replicated`'s SQL
([store.rs:937-940](crates/hippius-drain-core/src/store.rs)) touches only
`status`/`corrupt_attempts`/`updated_at`. The real design is fine — and safe, because the window
where residency exists at `status='draining'` is invisible to `evictable_parts`, which filters on
`status='replicated'` — but the ordering is a genuine correctness property and the comment asserts
the wrong mechanism for it.

---

### A-8 — P3 · Observability and manifest nits

- **`peer_serve_limiter` is coupled to the client flag.** See A-1; worth fixing on its own merits
  even after the auth issue is closed, so a node that serves peers but does not fetch from them is
  still capped.
- **Prod manifest was touched** despite the plan's non-goal *"No production manifest changes in any
  phase"*. I checked the diff: `k8s/production/drain-agent-daemonset.yaml` is **comment-only** (the
  rollback-load-bearing note on `CEPHOR_REPLICATED_RECLAIM_GRACE_SECS`, and the corrected reconcile
  rationale). That is Phase 0 doing what it said, so this is a plan/diff wording mismatch, not a
  defect — recording it so the next reviewer does not re-raise it.
- **Prod carries no `CEPHOR_EVICT_*` at all**, so the first prod binary carrying this work runs the
  evictor on code defaults. Consistent with the plan's F0 governance finding; restating it because
  F0 is about the *retention* default and this is about the *eviction* defaults, which F0 does not
  mention.

---

## 3. Findings (Agent B)

_Agent B = independent re-review of the same scope (`bfd1c050..HEAD`, PRs #398 + #400). Written
2026-08-07 after reading Agent A's findings, re-tracing every cited path in the live tree, and
looking for issues Agent A did not raise. A-findings are **not** restated here unless the
evidence differs; see §4 for the per-finding verdict._

### B-1 — P1 · Peer-registry Redis keys are an unauthenticated SSRF primitive

`PeerRegistry.resolve` returns whatever string sits in `hippius:peer:{node_name}` and
`PeerChunkFetcher` immediately `GET`s it
([peers.py:157-170, 264-267](hippius_s3/cache/peers.py)). There is **no** validation that the
URL is:

- `http` (not `file:`, `https:` to an external host, etc.),
- a pod-network address (10.x/172.16–31.x),
- port 8000,
- or even a host this cluster owns.

The value is written only by `PeerRegistry.register` in the happy path, but the trust root is
**whoever can `SET` on the Redis instance used for peer keys** (the main `REDIS_URL` client at
[main.py:152-158](hippius_s3/main.py)). Any process with that write access — a compromised
worker, a mis-scoped debug session, another service sharing the DB — can redirect every peer
fetch from every api pod to an attacker-controlled endpoint.

**Impact (stackable with A-1):**
- **SSRF from the api pod identity.** The fetch uses the pod's network path and the api's
  `httpx` client; targets include cloud metadata, other ClusterIPs, and anything the pod
  network can reach.
- **Cache poisoning.** A 200 response body is returned as chunk bytes and, when promotion is
  on, written onto local NVMe by `_promote_chunk` with **no length or content check**
  ([dual_fs_store.py:207-215](hippius_s3/cache/dual_fs_store.py),
  [peers.py:284-287](hippius_s3/cache/peers.py)). Subsequent local reads serve the poison until
  eviction; AEAD decrypt then fails (or, under a crafted length match, yields garbage). Either
  way the peer tier becomes a fleet-wide read DoS amplifier.
- The plan's threat model ([retention plan §"New internal surface"](docs/plans/2026-08-06-ssd-read-tier-retention-and-residency-allocator.md))
  only considered "who can call `/internal/parts`". It did not model "who can make the **client**
  call an arbitrary URL".

**Fix:** treat the peer URL as untrusted input: parse it, require `http`, require a private
literal IP (or a known Service DNS form), pin port 8000, reject anything else and count
`peer_fetch_shed_total{reason=bad_peer_url}`. Prefer signing registration (HMAC with a
cluster secret) or writing only from a pod-identity side channel the API alone can refresh.
Independently, cap `response.content` against the part's known cipher size before promote.

**Regression test:** registry returns `http://169.254.169.254/` → fetcher returns `None` and
does not issue the request (mock transport must see zero calls).

---

### B-2 — P1 · `UploadPart` re-upload after `replicated` never re-drives the pool (silent
stale-read after eviction)

Agent A's open question §6.1 is answerable from the code, and the answer is a real integrity
bug that **retention makes load-bearing**.

**Does `UploadPart` retry bump the object version?** No.
[multipart.py:547-549, 748-755](hippius_s3/api/s3/multipart.py) reuses
`ongoing_multipart_upload.current_object_version` and the same
`(object_id, version, part_number)` for every attempt. That is correct S3 semantics.

**What happens if the first attempt already drained?**

1. First `UploadPart` lands meta → `LandedPartPublisher` announces → agent
   `record_landed_part` inserts `status='pending'`
   ([store.rs:584-601](crates/hippius-drain-core/src/store.rs)).
2. Drain copies SSD→pool, `mark_resident`, `mark_replicated`.
3. Client re-`UploadPart`s the same part number with **different bytes** (legal before
   `CompleteMultipartUpload`; also the shape of a late retry after a perceived timeout).
4. SSD is rewritten; meta is rewritten; another landed announcement is published
   ([write_through_writer.py:71-73](hippius_s3/writer/write_through_writer.py)).
5. `record_landed_part` on conflict only fills a NULL `node_id` — it **never** resets
   `status` ([store.rs:589-593](crates/hippius-drain-core/src/store.rs)):

   ```sql
   ON CONFLICT (object_id, version, part_number)
   DO UPDATE SET node_id = EXCLUDED.node_id
   WHERE cephor_replication_status.node_id IS NULL
   ```

6. The reconciler sees `ReplicationState::Replicated` and tallies
   `replicated_orphan` — explicitly "leaves it alone"
   ([reconcile.rs:211](crates/hippius-drain-core/src/reconcile.rs)).
7. **Pool still holds the first attempt's ciphertext. SSD holds the second.**

**Why this is worse under the read tier:**
- Before retention the SSD was unlinked on commit, so a re-upload at least re-created the
  only local copy the arion-uploader would read at Complete time. Cross-node pool reads of an
  *in-flight* MPU part were already racy, but Complete + arion upload still preferred local.
- After retention the evictor may **delete the SSD copy of the second attempt** while the
  pool still has the first. A later GET that misses local (or runs on another node and falls
  through peer→pool) decrypts the **first** attempt under the same DEK/chunk index — AEAD
  succeeds, plaintext is wrong, ETag from Complete describes the second attempt. That is
  silent data corruption, not a 500.

This predates the SSD read tier as a drain/reconciler gap, but **retention + eviction is the
mechanism that converts "pool is stale" into "the only durable copy is stale"**. It is in
scope for this review for that reason.

**Fix options:**
1. On landed announcement for a row already `replicated`/`failed`/…, if SSD content can
   change, reset to `pending` (or a dedicated `re_drain` state) and clear pool-side
   assumptions; or
2. Reject / no-op `UploadPart` once the part is `replicated` (stricter than S3, needs a
   product call); or
3. Content-hash the SSD set at drain time and re-drive when the hash diverges (reconciler
   already walks complete parts — a hash compare is the honest fix).

**Regression test:** part drains to `replicated` → re-`UploadPart` with different body →
assert status returns to a drainable state (or pool bytes match the new SSD) before
Complete.

---

### B-3 — P2 · `ip_whitelist` admits the entire `172.0.0.0/8`, not RFC1918 `172.16.0.0/12`

[ip_whitelist.py:36-38](hippius_s3/api/middlewares/ip_whitelist.py):

```python
client_ip.startswith("172.") or client_ip.startswith("10.") or client_ip == "127.0.0.1" ...
```

RFC1918 private space is `172.16.0.0/12` only. `172.15.0.1` and `172.32.0.1` are **public**
and currently pass the middleware. Pre-existing, but the peer-serve threat model and the
plan's "pod network only" claim both rest on this check. Combined with A-1 (gateway already
inside a true private range) this is secondary; combined with a mis-SNAT or a hostPort
exposure it becomes the next hole after the gateway path is closed.

**Fix:** parse with `ipaddress.ip_address` / `ip_network` and allow only
`10.0.0.0/8`, `172.16.0.0/12`, `192.168.0.0/16` (if needed), loopback. Add a unit table for
the boundary addresses.

---

### B-4 — P2 · Peer `200` bodies are trusted and promoted with no size bound

`PeerChunkFetcher` returns `response.content` for any non-503/non-error status
([peers.py:284-287](hippius_s3/cache/peers.py)). There is no check against
`meta.chunk_size` / `cipher_size_bytes` from DB or from the local meta the promoter already
reads for `set_meta`. A peer (or B-1 SSRF target) can:

- return multi-GB bodies and pin memory on the client pod,
- return short/wrong bodies that get promoted and fail every subsequent local decrypt until
  eviction.

Serving ciphertext without ACL is intentional (plan); **promoting unauthenticated peer bytes
onto the ingest NVMe without a length check** is not defended anywhere.

**Fix:** after a 200, require `len(content)` in an expected range (cipher size from DB when
available, else `chunk_size + GCM_TAG` bound); on mismatch, return `None` (fall through to
pool) and count a shed reason. Do not promote on mismatch.

---

### B-5 — P2 · Shared-disk startup guard from hardening plan §0-M2 never shipped

Agent A's open question §6.2: the plan required a cheap startup check that logs loudly when
`du(cache) ≪ (total − free)` so a process that does not own its filesystem cannot silently
drive free-space gates
([hardening plan lines 84-86](docs/plans/2026-08-07-ssd-read-tier-hardening.md)).

I grepped the tree for any such probe (Python `du`/`disk_usage` compare, Rust
`drain_ssd_cache_bytes` vs `statvfs` at boot). **It is not present** in agent startup, api
lifespan, or `fs_pressure.py`. Staging still runs on shared `/dev/md3` per the plan's own
measurements, so the evictor / promote floor / `fs_cache_pressure` still reason about free
space they do not own. This is an outstanding plan item, not a "dropped deliberately with a
record" item.

**Fix:** on agent + api startup, compare accounted cache bytes to `total − free`; if
accounted is &lt; some fraction of used (e.g. 50%), log ERROR with both numbers and a
`drain_ssd_shared_filesystem` metric/flag. Do not refuse to start (prod may have other
small tenants briefly); make the signal alertable.

---

### B-6 — P3 · `chunks_exist_batch` comment still describes pre-retention drain behaviour

[dual_fs_store.py:242-247](hippius_s3/cache/dual_fs_store.py):

> under drain-direct the drain unlinks the primary SSD copy after replicating to the pool

That is exactly the behaviour this work **removed**. The code path (check primary, then
fallback) is still correct — pool-only parts must count as cache — but the justification is
the superseded design. Same class of comment drift as A-7; called out because this method is
on the GetObject hot path and the next reader will re-learn the wrong model.

---

### B-7 — P3 · Multi-resident parts pick an arbitrary peer (`LIMIT 1`)

`_owner` selects any other node with a residency row
([peers.py:209-218](hippius_s3/cache/peers.py)):

```sql
... AND r.node_id <> $4 AND s.status = 'replicated'
LIMIT 1
```

After promotion, several nodes can hold the same part. There is no preference for the
ingest node, for lowest latency, or for a node under its serve cap. Under load this
concentrates on whichever row Postgres returns first (often insert order ≈ promotion
order), not the healthiest peer. Not a correctness bug — 404/503 already fall through —
but it works against the serve-side cap design (A-8 / peers.py doc on oversubscription).

**Fix (later):** `ORDER BY r.resident_at` (prefer long-held ingest copy) or randomize; or
probe and pick the first non-503.

---

### B-8 — P3 · No metric when peer fetch returns non-200 other than 503

`client_cap` and `server_busy` are counted; connect failures poison the memo and debug-log;
plain 404 / 5xx other than 503 return `None` with **no counter**
([peers.py:284-286](hippius_s3/cache/peers.py)). Under a systemic eviction storm or a bad
deploy of the peer endpoint, the fleet silently falls to pool with nothing on the dashboard
except a drop in `chunk_reads_by_tier_total{tier=peer}`. A
`peer_fetch_shed_total{reason=peer_miss|peer_error}` (or a separate counter) would make the
tier operable.

---

## 4. Cross-review

### Agent A on Agent B's findings

_Written after re-tracing every B-citation in the live tree and running the additional checks in
the header table. Two findings need correction: **B-2's causal story is inverted**, and **B-1's
severity rests on a precondition that is already game-over**. Both are still worth fixing._

**Credit where it is due first:** B-1/B-4 are a genuinely good axis I missed. I reviewed *who can
call* the peer endpoint and never asked *what the peer client will call*. That is the mirror half
of the same trust boundary and it belongs in the review. B-3 is worse — I read
`ip_whitelist.py` while tracing A-1 and did not flag the `startswith("172.")` bug. That is my miss,
not an oversight of scope.

#### B-1 — peer-registry Redis keys as an SSRF primitive

**PARTIALLY CONFIRMED — code fact exact, severity overstated. Recommend P2, not P1.**

The code claim is correct and I verified it: `resolve` returns the raw `url` field with no
validation ([peers.py:157-170](hippius_s3/cache/peers.py)) and `__call__` interpolates it straight
into `self._client.get(...)` ([peers.py:264-267](hippius_s3/cache/peers.py)). No scheme check, no
private-IP check, no port pin.

What I dispute is the P1 rating, because of the precondition. B-1's trust root is "whoever can
`SET` on the Redis instance used for peer keys" — `app.state.redis_client`, i.e. `config.redis_url`.
**That is the same instance the gateway caches access-key authentication in**:
`cached_auth` does `redis_client.get(cache_key)` / `setex(...)`
([gateway/services/auth_cache.py:31,46](gateway/services/auth_cache.py)) on
`app.state.redis_client = create_redis_client(config.redis_url)`
([gateway/main.py:70](gateway/main.py)).

So an attacker who can write that Redis can forge an access-key auth result and impersonate any
account outright. SSRF from the api pod is *strictly weaker* than the capability the precondition
already grants. Rating B-1 alongside A-1 — which needs **no** precondition at all, just an HTTP
client — misprices it and would distort the fix order.

That said the fix is ~15 lines, it is the correct posture for a URL crossing a process boundary,
and it costs nothing: **do it**, as defence in depth, at P2. Note its "cap `response.content`"
clause duplicates B-4; keep that in B-4 only.

One thing B-1 gets right that survives the downgrade: the plan's threat model genuinely never
asked "who can make the client call an arbitrary URL". That gap is worth recording in the plan
even if the exploit path is gated.

#### B-2 — `UploadPart` re-upload after `replicated` never re-drives the pool

**CONFIRMED as a bug — DISPUTED on attribution and on the "worse under retention" claim.
Recommend P1, filed against the drain, not against this work.**

The mechanism is real and I verified every link, including two B did not cite:

| Link | Verified |
|---|---|
| No version bump on retry | [multipart.py:547-549](hippius_s3/api/s3/multipart.py) reuses `current_object_version` |
| `record_landed_part` never resets status | [store.rs:589-593](crates/hippius-drain-core/src/store.rs) — `DO UPDATE SET node_id … WHERE node_id IS NULL` |
| Reconciler leaves it | [reconcile.rs:211](crates/hippius-drain-core/src/reconcile.rs) `Replicated => replicated_orphan += 1` |
| **Nothing else re-drives it** | `wake_version_replication` clears defer backoff on **still-`pending`** rows only; `fail_version_replication` only moves rows *to* `failed`. Neither returns a `replicated` row to a drainable state ([mpu_cleanup.py:70-83](hippius_s3/services/mpu_cleanup.py)) |
| Same DEK/AAD ⇒ stale bytes decrypt cleanly | DEK is per `object_version`, AAD binds `(bucket_id, object_id, part_number, chunk_index, upload_id)` — all unchanged on retry, so the *old* ciphertext AEAD-verifies |

That last row is the part that makes it nasty and neither of us stated it explicitly: this fails
**silently**, not with a decrypt error, whenever the two attempts have equal length.

**Where B-2 is wrong.** Its §"Why this is worse under the read tier" argues retention converts
"pool is stale" into "the only durable copy is stale". The pre-retention code did that *already,
and on a guarantee rather than a policy*. `git show dcbe9ef1:crates/hippius-drain-core/src/ssd_reclaim.rs`
lines 383-390:

```rust
ReplicationState::Replicated => {
    if status.age >= graces.replicated {
        remover.unlink_part(part).await.map_err(ReclaimError::Remove)?;
        report.reclaimed_replicated += 1;
```

With prod's `CEPHOR_REPLICATED_RECLAIM_GRACE_SECS=3600`, the second attempt's SSD copy was
**unconditionally deleted within an hour** while the pool kept the first attempt. Retention
replaces that guaranteed 1h deletion with a free-space policy, and the shard is ~26% of the disk —
so post-retention the *correct* copy now survives indefinitely on an uncontended node.

**Retention makes this bug less likely to bite, not more.** The direction is inverted.

Also unverified in B-2: *"the only local copy the arion-uploader would read at Complete time"*.
The uploader builds its store with a bare `create_fs_store(config)`
([workers/uploader.py:88](hippius_s3/workers/uploader.py)) and I found **no** per-worker
`HIPPIUS_OBJECT_CACHE_DIR` override in `k8s/`, so I could not confirm it reads node-local rather
than the pool. That sentence should be dropped or evidenced; the finding does not need it.

**Net:** raise the *bug* to P1 on its own merits — silent wrong plaintext is worse than most of
what either of us found — but file it as a pre-existing drain-direct integrity gap. Putting it in
the read-tier fix order (B's §7 places it fifth) is the wrong queue: it is neither caused by this
work nor fixed by reverting it, and it should not gate this promotion.

#### B-3 — `ip_whitelist` admits `172.0.0.0/8` instead of `172.16.0.0/12`

**CONFIRMED.** [ip_whitelist.py:36-38](hippius_s3/api/middlewares/ip_whitelist.py) — exact.
`172.15.0.1` and `172.32.0.1` are public and pass. Pre-existing, but squarely in scope: this
middleware is the *stated* authorization boundary for the endpoint in A-1, so a review of that
endpoint has to audit it. My miss.

Agree P2. One addition to the fix: allow-list the **actual pod/service CIDRs** via config rather
than hardcoding RFC1918 — the current code would still admit any 10.x host on a flat network, which
is the property A-1 exploits through the gateway.

#### B-4 — peer `200` bodies trusted and promoted with no size bound

**CONFIRMED.** [peers.py:284-287](hippius_s3/cache/peers.py) returns `response.content` for any
200 and [dual_fs_store.py:207-215](hippius_s3/cache/dual_fs_store.py) writes it to local flash with
no length check.

Nuance on the two arms, since they are not equally severe:

- *Memory pinning* is partly bounded — `httpx.AsyncClient(timeout=config.peer_fetch_timeout_seconds)`
  is 0.5s total ([main.py:167](hippius_s3/main.py)), so a body has to arrive inside that window.
  Real but capped at whatever the pod network delivers in 0.5s, which is not nothing.
- *Promote-poisoning* is the serious arm, and it needs **no** compromise: a peer running a
  rolled-back or half-deployed image that answers 200 with a wrong-length body gets that body
  written onto local NVMe and served to every subsequent local read until eviction. That is a
  self-inflicted failure mode, not an attack one.

Agree P2. The check is nearly free — the promoter already reads the fallback meta for `set_meta`
([dual_fs_store.py:189](hippius_s3/cache/dual_fs_store.py)), so `chunk_size` is in hand at exactly
the right moment.

#### B-5 — hardening plan §0-M2 shared-disk startup guard never shipped

**CONFIRMED, independently.** I grepped for any `du`-vs-`statvfs` comparison across `*.py` and
`*.rs` and found nothing in agent startup, the api lifespan, or `fs_pressure.py`. This answers my
own open question §6.2 the same way. Agree P2 — and note the consequence is that **every
free-space gate in this work is unvalidatable on staging**, which is where the soak is happening.

#### B-6 — `chunks_exist_batch` comment describes pre-retention drain behaviour

**CONFIRMED.** [dual_fs_store.py:242-247](hippius_s3/cache/dual_fs_store.py). Worth stating
precisely: the comment's *conclusion* is still correct — a pool-only part must read as `cache`, not
be re-fetched through the pipeline — only its stated *cause* is dead ("the drain unlinks the
primary SSD copy after replicating" → it is now the evictor that unlinks, on a free-space policy).
Same class as A-7. P3.

#### B-7 — multi-resident parts pick an arbitrary peer (`LIMIT 1`)

**CONFIRMED.** [peers.py:209-219](hippius_s3/cache/peers.py) — `LIMIT 1` with no `ORDER BY`, so the
row is not merely arbitrary but **non-deterministic across replans**. The `PartMemo` pins one answer
per part for 30s, which hides the instability locally while leaving the fleet-wide distribution
arbitrary. P3 is right; it only becomes interesting once the serve caps actually bind.

#### B-8 — no metric for non-200/non-503 peer responses

**CONFIRMED.** [peers.py:284-286](hippius_s3/cache/peers.py) — 404 and non-503 5xx return `None`
silently. Agree P3, with one link worth drawing: this compounds **A-3**. Between them, an eviction
pass dying on an unlinkable part and a peer tier failing wholesale are both observable only as the
*absence* of a signal (`chunk_reads_by_tier_total{tier=peer}` sagging, free space drifting down).
Several of this feature's failure modes are currently negative-space alerts, which is the weakest
kind.

---

### Agent A — revised assessment after cross-review

I stand by A-1 as the sole ship-blocker and by A-3/A-4 as the two P1s.

Two changes to B's recommended order in §7:

1. **B-1 moves down** (P1 → P2). Its precondition already permits auth-cache poisoning and full
   account impersonation, so it adds little marginal risk. Still do it — but not ahead of A-3/A-4,
   which need no attacker at all.
2. **B-2 moves out of this queue.** It is a real P1 integrity bug and the best catch in §3, but it
   is pre-existing, retention makes it *less* likely rather than more, and it will not be fixed or
   mitigated by anything in this promotion. It deserves its own issue against the drain, urgently —
   not a slot in the read-tier gate.

**Agreed fix order for prod promotion:**
`A-1` → `A-3` → `A-4` → `A-2` / `B-3` (both cheap, both close A-1's neighbourhood) → `B-1` / `B-4`
(one PR, same file) → `A-5` → `B-5` → comment/observability cleanup (`A-6`, `A-7`, `B-6`, `B-7`,
`B-8`).

**Tracked separately, not gating this promotion:** `B-2`.

### Agent B reply to Agent A's cross-review of B-findings

_Re-verified every disputed claim against the tree and history. No re-disputes._

#### B-1 severity (A: PARTIALLY CONFIRMED, P1→P2)

**ACCEPT.** Same Redis instance is exact: gateway and api both wire
`create_redis_client(config.redis_url)` ([gateway/main.py:70](gateway/main.py),
[main.py:114](hippius_s3/main.py)); k8s base mounts the same `REDIS_URL` secret key into both.
`cached_auth` writes `hippius_auth:*` on that client
([auth_cache.py:29-50](gateway/services/auth_cache.py)); peer registry writes `hippius:peer:*` on
the same client. An attacker who can `SET` peer URLs can also plant a forged auth-cache hit —
full account impersonation, which dominates SSRF. **B-1 is P2 defence-in-depth**, not a
promotion gate. Keep the URL validation; drop the content-length clause (stays in B-4 only).

One residual that does **not** restore P1: SSRF needs no forged auth shape — only a string URL —
so the *ease* of the write is slightly lower than crafting `TokenAuthResponse` JSON. Still the
same privilege class. P2 stands.

#### B-2 attribution and "worse under retention" (A: CONFIRMED bug, DISPUTED causal story)

**ACCEPT the correction.** Re-checked:

| Claim | Verdict |
|---|---|
| Mechanism (no re-drive after `replicated`) | Still exact — including `wake`/`fail` not touching `replicated` ([mpu_cleanup.py:58-85](hippius_s3/services/mpu_cleanup.py)) |
| Silent AEAD success on equal-length retry | Exact — same `upload_id` + same `object_version` DEK; AAD/nonce unchanged ([crypto_service.py:90-159](hippius_s3/services/crypto_service.py)) |
| Pre-retention reclaim unlinked `Replicated` past grace | Exact — `git show dcbe9ef1` `ssd_reclaim.rs` 383-390; happy path also unlinked at commit (`c3bd1c34` message: *"unlinked its SSD copy as the last step of every successful drain"*) |
| "Retention makes it worse" | **Inverted.** Pre-retention: second-attempt SSD was guaranteed gone within `CEPHOR_REPLICATED_RECLAIM_GRACE_SECS` (prod 3600s) while pool kept attempt one. Post-retention: second-attempt SSD survives until free-space eviction — often forever on an uncontended node. Likelihood ↓, not ↑. |
| Arion-uploader "only local copy" sentence | Unverified / drop — bare `create_fs_store(config)` ([uploader.py:88](hippius_s3/workers/uploader.py)), no worker-level local-dir override found in k8s |

**Revised B-2 stance:** real **P1 integrity bug**, pre-existing under drain-direct, **not caused
by this work**, retention makes the *correct* copy more durable rather than less. File as its
own drain issue; **do not gate this promotion**. Original §3 wording on "worse under the read
tier" is wrong and is superseded by this reply (per protocol, §3 text left in place).

#### B-3 … B-8

No dispute with A's confirmations or nuances. B-4's 0.5s timeout bound is exact
([config.py:399](hippius_s3/config.py)); promote-poisoning from a half-deployed peer remains the
serious arm. B-8 ↔ A-3 negative-space alert link is a good catch.

#### Fix order

**ACCEPT A's revised order** over §7's original recommendation:

`A-1` → `A-3` → `A-4` → `A-2`/`B-3` → `B-1`/`B-4` → `A-5` → `B-5` → cleanup → **B-2 tracked
separately**.

---

### Agent B on Agent A's findings

#### A-1 — P0 · peer-serve reachable unauthenticated from the public internet

**CONFIRMED.** Re-traced end-to-end against the live tree:

| Step | Evidence |
|---|---|
| Gateway catch-all | [gateway/main.py:192](gateway/main.py) `/{path:path}` |
| Anonymous GET | [auth_orchestrator.py:63-65](gateway/services/auth_orchestrator.py) — no `Authorization`, method GET, path ≠ `/` → `is_valid=True` |
| ACL pass-through on missing bucket | [acl.py:253-255](gateway/middlewares/acl.py) — `bucket_owner_id is None` → `call_next` |
| Forward from 10.x pod IP | gateway → api; [ip_whitelist.py:36-38](hippius_s3/api/middlewares/ip_whitelist.py) admits 10.x/172.x |
| Route wins over S3 catch-all | [main.py:428-429](hippius_s3/main.py) `internal_parts_router` before `s3_router_new` |
| Limiter only when peer fetch on | [main.py:151-179](hippius_s3/main.py); missing limiter = no cap ([internal_parts.py:68-74](hippius_s3/api/internal_parts.py)) |
| Router always mounted | [main.py:428](hippius_s3/main.py) unconditional |

Staging has `HIPPIUS_PEER_FETCH_ENABLED` on (`k8s/staging/api-local-deployments-staging.yaml`),
so on staging the endpoint is both **reachable and concurrency-capped**; with the flag off
(prod default) it is reachable and **uncapped**. Severity P0 is correct for any environment
where the gateway fronts the api — the plan's "network-level only" model is wrong once the
gateway is on that network.

No dispute. Suggested fix ordering (gateway reject `internal` + shared secret on `/internal/*`)
is sound; gateway-side reject alone is the smallest ship-blocker close.

#### A-2 — P2 · `internal` missing from `RESERVED_BUCKET_SEGMENTS`

**CONFIRMED.** [reserved_bucket_names.py:32-41](hippius_s3/reserved_bucket_names.py) has no
`internal`. CreateBucket name length for `internal` (8) clears the min-length gate. The
module comment still lists only two danger sources; the api-side shadow route is a third.
`test_every_auth_exempt_segment_is_a_reserved_bucket_name` cannot catch it. Severity P2 is
right (stranded name + unreadable keys, not a public data leak by itself).

#### A-3 — P1 · one unlink failure halts all eviction

**CONFIRMED.** [ssd_evict.rs:319](crates/hippius-drain-core/src/ssd_evict.rs) uses `?` on
`unlink_part` inside the page loop, **before** `mark_evicted` at 327. Runtime treats the whole
pass as `eviction pass failed; retrying` without setting `starved` /
`skipped_unreplicated`. `remove_part_dir` maps only `NotFound → Ok`
([localfs.rs:425-430](crates/hippius-drain-agent/src/localfs.rs)); `ENOTEMPTY` / `EACCES` /
`EIO` propagate. Cursor is `ORDER BY COALESCE(last_read_at, resident_at)` — stable — so a
persistent failer pins the head.

One nuance worth adding (does not weaken the finding): successful unlinks on the **same**
page that later hits a failing candidate also skip `mark_evicted` for that page. Those
already-unlinked rows heal on a later pass via NotFound→Ok → mark. The stuck case is only
the persistently failing row. The alert silence claim is still exact.

#### A-4 — P1 · promote/evict band validated against a static reserve the allocator overrides

**CONFIRMED.** `validate_promotion_band` defaults
`EVICT_RESERVE_RATIO = 0.150` / `HEADROOM = 0.050`
([fs_pressure.py:98-99, 106-153](hippius_s3/fs_pressure.py)), called once from
[cache/__init__.py:38-41](hippius_s3/cache/__init__.py). Runtime:

```rust
let reserve = allocated_reserve_permille.unwrap_or(policy.reserve_permille);
```

([runtime.rs:309](crates/hippius-drain-agent/src/runtime.rs)). Allocator interpolates
base 150 → max 400 permille ([alloc.rs:119-123, 213-224](crates/hippius-drain-core/src/alloc.rs)).
At max: evict reserve 0.40, target 0.45; promote floor stays 0.175 →
`evict_reserve < promote_floor` **inverts**. This is a live runtime override, not config
drift. P1 is correct; the failure mode arrives exactly when the drain is stressed.

#### A-5 — P2 · failed residency claim leaks disk with no counter

**CONFIRMED.** [residency.py:70-88](hippius_s3/cache/residency.py) logs at debug and returns.
`ssd_reclaim` `Replicated` arm only increments `skipped_replicated`
([ssd_reclaim.rs:392](crates/hippius-drain-core/src/ssd_reclaim.rs)).
`promotion_skipped_total` only has reason `disk_pressure`
([monitoring.py:32, 546-548](hippius_s3/monitoring.py)) — no `residency_failed`.
`read_recency` has `written|failed` outcomes; residency claim does not. Observability
inversion claim holds. Suggest also counting claim failure separately from "skipped before
write".

#### A-6 — P2 · residency `bytes` ON CONFLICT: accumulate vs overwrite, comment only on Python

**CONFIRMED.** Python accumulates ([residency.py:57-62](hippius_s3/cache/residency.py));
Rust overwrites ([store.rs:1264-1268](crates/hippius-drain-core/src/store.rs)) with no
cross-reference. The "unreachable today" argument (local hit ⇒ no promote) holds while
ingest writes full parts + meta before readability; a partial promote concurrent with a
late drain-commit on another node is the collision window. Comment-only fix is the minimum;
a schema invariant or single writer would be stronger but is out of scope for a P2.

#### A-7 — P2 · comment drift where the comment is the safety argument

**CONFIRMED (both sub-points).**

**(a)** Module doc still says FIFO / "recency does not exist yet"
([ssd_evict.rs:29-37](crates/hippius-drain-core/src/ssd_evict.rs)); store orders on
`COALESCE(last_read_at, resident_at)` ([store.rs:1168](crates/hippius-drain-core/src/store.rs)).

**(b)** [partdrain.rs:554](crates/hippius-drain-core/src/partdrain.rs) claims
`mark_replicated` stamps `resident_at` in the same statement; actual order is
`mark_resident` then `mark_replicated` (528-535), and `mark_replicated` SQL only touches
status/corrupt_attempts/updated_at ([store.rs:937-939](crates/hippius-drain-core/src/store.rs)).
Safety still holds via `status='replicated'` filter on `evictable_parts`.

#### A-8 — P3 · observability / manifest nits

**CONFIRMED** as nits. `peer_serve_limiter` coupled to client flag is real and independently
worth fixing under A-1's fix list item 4. Prod comment-only drain-agent touch is not a
defect. Prod lacking `CEPHOR_EVICT_*` means code defaults — consistent with F0-style
governance gap for eviction, not retention.

---

## 5. What this work does well (recorded so the review is honest, not just a defect list)

- **The durability invariant is defended three times, independently**: SQL (`JOIN … AND s.status =
  'replicated'`), the orchestrator's per-candidate re-check, and an alerting counter that must stay
  at zero. The comment at [store.rs:1136-1141](crates/hippius-drain-core/src/store.rs) explaining
  *why* residency and status are independent axes (`redrive_corrupt_parts` resets `corrupt →
  pending` without clearing `resident_at`) is the kind of reasoning that prevents the next
  regression.
- **Termination is modelled properly.** `evict_to_target` distinguishes deficit-met / genuine
  exhaustion / time-budget, and refuses to declare progress it cannot measure
  ([ssd_evict.rs:348-351](crates/hippius-drain-core/src/ssd_evict.rs)) — the probe-failed-**and**-
  zero-accounted-bytes case is a subtle one to get right and it is right.
- **Migrations.** `SET LOCAL lock_timeout` ahead of every DDL, with the CrashLoopBackOff reasoning
  spelled out; an explicit, argued rejection of `CREATE INDEX CONCURRENTLY`; no backfill, justified;
  0018's write-up of the cursor bug it fixes is a model postmortem-in-a-migration.
- **`effective_max_inflight` derives an invariant instead of asserting it**
  ([peers.py:65-96](hippius_s3/cache/peers.py)) — the right call for a constraint spanning two
  independently-tuned knobs, and the docstring is explicit about what it does *not* fix.
- **The plans mark estimates vs measurements** and record rejected options with reasons (mtime
  pruning rejected on durability grounds; a retention flag rejected as a shim). §5 of the hardening
  plan is a real adversarial review of the plan itself, with corrections applied inline.
- **Test suites are green and behaviourally named.** 2477 Python + 225 Rust, verified above.

---

## 6. Open questions (Agent A) — not findings, needing someone who knows the history

1. **MPU part re-upload.** A client retrying `UploadPart` for the same part number reuses the same
   `(object_id, version, part_number)`. `record_landed_part` is idempotent and does **not** reset
   `status` ([store.rs:589-600](crates/hippius-drain-core/src/store.rs)), so if the first copy
   already committed `replicated`, the re-uploaded bytes appear to never reach the pool, and the
   evictor may now unlink them. I believe this predates the SSD read tier and is unchanged by it —
   but retention changes *who* deletes the stale SSD copy, so it is worth confirming rather than
   assuming. Does an `UploadPart` retry bump the object version?

   **Agent B answer (promoted to B-2):** No version bump — MPU reuses
   `current_object_version`. Re-upload after `replicated` does **not** re-drive the pool;
   reconciler classifies the on-disk copy as `replicated_orphan` and leaves it → silent wrong
   plaintext when the pool is later the only copy. **Pre-existing drain-direct gap (P1).**
   Agent A's cross-review corrects the causal story: pre-retention reclaim *guaranteed* the
   second-attempt SSD was deleted within the replicated grace while the pool kept attempt
   one; retention makes the correct copy *more* durable, not less. Track separately — does
   not gate this promotion.

2. **Staging's shared `/dev/md3`** (§0-M2 of the hardening plan): the plan says a `du(cache) ≪
   (total − free)` startup check should be added "worth having in prod too as a regression guard".
   I did not find it in the diff. Was it dropped deliberately, or is it still outstanding?

   **Agent B answer (promoted to B-5):** Still outstanding — no startup guard in agent, api
   lifespan, or `fs_pressure.py`. No commit message or plan amendment marks it dropped.

3. **`HIPPIUS_PEER_SERVE_MAX_INFLIGHT = 16`** is acknowledged in the plan as a guessed number. Has
   anything measured what a peer's NVMe + uvicorn sustains without hurting its own ingest?

   **Agent B:** Still open — no measurement artefact or dashboard annotation found in-repo.
   Not promoted to a finding; the cap existing at all is better than the prior unbounded
   serve path, but B-1/B-4 matter more than the constant until the trust boundary is closed.

---

## 7. Agent B overall assessment

_(Original assessment kept for the record; **superseded** by the consensus order after
cross-review — see below.)_

Agent A's review is high quality and correctly prioritised. **A-1 is a genuine ship-blocker**
and the chain is not subtle once you put the gateway on the same trust network the plan
treated as "internal only". A-3 and A-4 are the two P1s I would also refuse to promote past
staging without a fix or a hard operational mitigation (A-4 can be temporarily mitigated by
clamping `max_reserve_permille` so the band cannot invert; A-3 needs code).

What Agent A initially missed (still valid findings; severity/queue adjusted in cross-review):

| ID | Revised stance after cross-review |
|---|---|
| **B-1** | Real defence-in-depth (URL validation). **P2**, not P1 — same Redis already allows auth-cache forgery. |
| **B-2** | Real **P1** silent-corruption bug in the drain. **Not caused by this work**; retention makes the correct copy *more* durable. Own issue, not a gate. |
| **B-3–B-8** | Stand as written; B-3/B-4 cheap and near A-1. |

### Consensus fix order (both agents, post cross-review)

**Gates this promotion:**

1. **A-1** (P0) — close the public peer-serve path  
2. **A-3** (P1) — unlink failure must not halt all eviction  
3. **A-4** (P1) — promote/evict band must track allocated reserve  
4. **A-2** / **B-3** (P2) — reserved name + real RFC1918 (or CIDR config)  
5. **B-1** / **B-4** (P2) — peer URL allow-list + response size check (one PR)  
6. **A-5** (P2) — residency-claim failure metric (± don't write unclaimable copies)  
7. **B-5** (P2) — shared-disk startup guard  
8. Comment / observability cleanup: **A-6**, **A-7**, **A-8**, **B-6**, **B-7**, **B-8**

**Tracked separately (does not gate this promotion):**

- **B-2** (P1 integrity) — drain re-drive on part content change after `replicated`
