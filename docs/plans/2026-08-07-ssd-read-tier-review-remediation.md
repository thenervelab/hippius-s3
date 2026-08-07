# SSD read-tier review remediation plan

Implementation plan for the valid findings in [CODE_REVIEW.md](../../CODE_REVIEW.md) (adversarial
review of PRs #398 + #400, scope `bfd1c050..6152e3a3`).

Written 2026-08-07 after independently re-verifying every code citation in the review against the
live tree. Verification notes are in §1; the plan proper starts at §3.

---

## 1. Verdict on every finding

Each row was re-checked by reading the cited file at the cited lines. "Verified" means the code
says what the review says it says — not that I agree with the severity.

| ID | Claim | Code verified | My severity | Action |
|---|---|---|---|---|
| **A-1** | `/internal/parts/...` reachable unauthenticated from the internet via the gateway | ✅ full chain | **P0** | Fix — WI-1 |
| **A-2** | `internal` missing from `RESERVED_BUCKET_SEGMENTS` | ✅ | **P2** | Fix — WI-4 |
| **A-3** | One un-unlinkable part halts all eviction, silently | ✅ | **P1** | Fix — WI-2 |
| **A-4** | Promote/evict band validated against a reserve the allocator overrides at runtime | ✅ | **P1** | Fix — WI-3 |
| **A-5** | Failed residency claim leaks disk permanently, no counter | ✅ | **P2** | Fix — WI-7 |
| **A-6** | Two writers of `cephor_ssd_residency.bytes` disagree; only Python says so | ✅ | **P3** (not P2) | Fix — WI-9 |
| **A-7** | Comment drift in `ssd_evict.rs` module doc and `partdrain.rs:554` | ✅ both | **P3** (not P2) | Fix — WI-9 |
| **A-8** | `peer_serve_limiter` coupled to the client flag; manifest notes | ✅ | **P3** | Folded into WI-1 |
| **B-1** | Peer-registry Redis URL is an unvalidated SSRF primitive | ✅ | **P2** | Fix — WI-5 |
| **B-2** | `UploadPart` re-upload after `replicated` never re-drives the pool | ✅ | **P1** | **Parallel track, not this queue** — §6 |
| **B-3** | `ip_whitelist` admits all of `172.0.0.0/8`, not `172.16.0.0/12` | ✅ | **P2** | Fix — WI-4 |
| **B-4** | Peer 200 bodies trusted and promoted with no size bound | ✅ | **P2** | Fix — WI-6 |
| **B-5** | Shared-disk startup guard (hardening §0-M2) never shipped | ✅ absent | **P2** | Fix — WI-8 (agent only) |
| **B-6** | `chunks_exist_batch` comment describes pre-retention drain | ✅ | **P3** | Fix — WI-9 |
| **B-7** | Multi-resident parts pick an arbitrary peer (`LIMIT 1`, no `ORDER BY`) | ✅ | **P3** | Fix — WI-9 |
| **B-8** | No metric for non-200/non-503 peer responses | ✅ | **P3** | Fix — WI-9 |
| **new** | Nothing invalidates a cached chunk that fails AEAD → permanent retry-immune read failure | ✅ absent | **P2** | Fix — WI-10 |

The last row is not from either review. It surfaced while checking whether B-4's length bound
actually closes B-4's serious arm; it does not, and this is why. See §2 and WI-10.

Additional facts I confirmed that the review asserted without citing:

- `public_router` owns only `GET/HEAD /public/{bucket_name}/{object_key:path}`
  ([public_router.py:23,48](../../hippius_s3/api/s3/public_router.py)) — it genuinely does not
  shadow `/internal/...`, so A-1's route-precedence step holds.
- `forward_service` strips every client-supplied `x-hippius-*` header before forwarding
  ([forward_service.py:117-119](../../gateway/services/forward_service.py)). This is what makes
  A-1's *preferred* fix (a shared secret in an `X-Hippius-*` header) sound: the anti-forgery
  property is already there.
- Allocator ships `base_reserve_permille: 150` / `max_reserve_permille: 400`
  ([run.rs:169-170](../../crates/hippius-drain-allocator/src/run.rs)), against a promote floor of
  `HIPPIUS_PROMOTE_MIN_FREE_RATIO:0.175` ([config.py:383](../../hippius_s3/config.py)). The band
  `evict_reserve < promote_floor` therefore inverts for any allocated reserve ≥ 175 permille —
  more than half the allocator's published range. A-4 is not a corner case.

Two facts the review states that are **wrong**, both of which make findings worse rather than
better:

- **CODE_REVIEW.md §4 on B-4**: *"`httpx.AsyncClient(timeout=config.peer_fetch_timeout_seconds)`
  is 0.5s total, so a body has to arrive inside that window… capped at whatever the pod network
  delivers in 0.5s."* Checked against the installed client (httpx 0.28.1): `Timeout` carries only
  `connect`/`read`/`write`/`pool` and **has no total-response timeout**; `read` bounds the wait
  *between* body chunks. A peer dripping one byte every 0.4s holds the connection and the buffer
  indefinitely, today, with no attacker and no code change. The memory-pinning arm of B-4 is
  unbounded, not "capped at 0.5s" — see WI-6.
- **`decrypter.py` has no `InvalidTag` handling and nothing invalidates a cached chunk on AEAD
  failure.** `errors.py:157` maps the exception to an S3 error and the local copy stays on disk.
  Because the local tier is checked first, a single bad promoted chunk is a **permanent, retry-immune
  read failure** for that object on that node until eviction. Neither agent modelled this, and it is
  what turns B-4 from "a wasted fetch" into an availability bug — see WI-6 and WI-10.

## 2. Where I differ from the reviewers

Three corrections. None change the fix order.

**A-6 and A-7 are P3, not P2.** Both are comment-only. A wrong comment in safety-critical code is
worth fixing promptly, but a severity that means "fix before prod promotion" should be reserved for
things that change runtime behaviour. Grouping them with B-6 in the cleanup PR (which both agents
already did in the fix order) is the right call; the label should match.

**B-5 should be scoped to the drain agent, not "agent + api".** Both agents proposed a
`du(cache)` vs `total − free` comparison on api startup as well. On the api side there is no
accounted-bytes number to compare against — it would have to walk a multi-TB cache directory at
lifespan start, which is exactly the readdir-bound cost the evictor was designed to avoid. The
agent already has `Store::node_cache_bytes()`, an O(1) SQL sum. Put the guard there only and let
the api learn about it through the metric.

**A-5's "don't write the chunk when the claim fails" needs an ordering change, not a skip.** The
promoter writes the chunk and then claims it
([dual_fs_store.py:207-215](../../hippius_s3/cache/dual_fs_store.py)), so by the time the claim
fails the bytes are already on disk. The honest fix is to invert: claim first, write only if the
claim succeeded. That trades a permanent unreclaimable leak for a transient over-accounting (a
residency row for bytes never written), which the evictor self-corrects — it re-probes actual free
bytes rather than trusting the accounted sum
([ssd_evict.rs:334-337](../../crates/hippius-drain-core/src/ssd_evict.rs)), so an over-accounted
part costs one wasted candidate and nothing else. Strictly better than the status quo.

One observation neither agent recorded about **B-2**: post-retention, promotion copies pool bytes
onto other nodes' local flash. If the pool holds the stale first attempt, that stale answer now
gets *cached fleet-wide* rather than merely re-read from the pool each time.

An earlier draft of this plan characterised that as "persistence grows, blast radius unchanged."
That was wrong, and the correction matters for scheduling: **growing persistence is a worsening.**
Both agents concluded retention makes B-2 strictly less likely (correct, for the ingest node's own
copy) and neither weighed the promotion path pulling in the opposite direction on every other node.
The net is not "neutral" — it is "less likely to arise, longer-lived and wider once it does."

That does not make B-2 a gate on these PRs (it is separable, pre-existing, and untouched by
anything here), but it does mean B-2 must not *queue behind* them. See §4.

---

## 3. Work items

Each item is one PR. Files listed are the ones expected to change; tests are the acceptance
criteria, written before the fix per repo convention.

### WI-1 — P0 · Close the public peer-serve path

**Problem.** `internal_parts_router` is mounted unconditionally
([main.py:428](../../hippius_s3/main.py)) and defended only by `ip_whitelist`, which the gateway
satisfies from its own pod IP. `peer_serve_limiter` is built only when `peer_fetch_enabled and
node_name and pod_ip` ([main.py:151,179](../../hippius_s3/main.py)), and
[internal_parts.py:68-70](../../hippius_s3/api/internal_parts.py) treats a missing limiter as "no
cap" — so with the prod flag off the endpoint is reachable *and* unbounded.

**Fix.** Three layers, all in one PR:

1. **Shared secret at the api.** New `HIPPIUS_INTERNAL_PEER_SECRET` (64-hex, from the existing
   k8s secret pattern). `get_local_chunk` compares `X-Hippius-Peer-Auth` with
   `hmac.compare_digest` and returns **404** — not 403 — on mismatch or absence, so the endpoint
   is not an oracle for its own existence. The gateway already strips every inbound
   `x-hippius-*` header, so a client cannot forge this.
2. **Serve gate decoupled from the fetch flag.** New `HIPPIUS_PEER_SERVE_ENABLED`. Mount
   `internal_parts_router` and build `peer_serve_limiter` under that flag *and* a non-empty
   secret, together — no secret means no serving. That closes A-8 item 1 (a node that serves but
   does not fetch is now capped) and removes the "mounted while the feature is off" state.
3. **Gateway denylist.** Reject any request whose first path segment is `internal` with
   `InvalidBucketName` before forwarding. Defence in depth: it holds even if the secret leaks or
   a future route lands under `/internal/`.

The peer client injects the header in `PeerChunkFetcher.__call__`.

**PRE-FLIGHT, blocking: run the reserved-name audit before this PR merges, not in WI-4.**
Step 3 rejects every request whose first segment is `internal`. If a customer already owns a bucket
by that name, this silently kills every read on it — and the check that would have found them
(`report_reserved_name_buckets.py` against staging and prod) sat four PRs downstream in the
original sequence, so nobody would look for three more merges. Run it first; if the name is taken,
that is an ops conversation before any of this ships.

**Follow-up to record, not to build now: the denylist is a blocklist and will rot.** The durable
answer is that the peer-serve route should not live on the ASGI app the gateway forwards to at all
— a separate port is unreachable by construction, whereas a path denylist holds only until someone
adds another non-S3 route (which is exactly how A-1 happened). The shared secret is the right fast
fix and the gateway's existing `x-hippius-*` strip genuinely makes it unforgeable, so ship that
now; file the port split as the structural follow-up.

**Files.** `hippius_s3/api/internal_parts.py`, `hippius_s3/main.py`, `hippius_s3/config.py`,
`hippius_s3/cache/peers.py`, `hippius_s3/cache/__init__.py`,
`gateway/middlewares/input_validation.py`, `k8s/staging/api-local-deployments-staging.yaml`,
`k8s/base/` secret.

**Tests.**
- api: `GET /internal/parts/...` with no header → 404; with a wrong header → 404; with the right
  header → reaches the store. (Assert the store was not called in the first two.)
- api: **route precedence** — `/internal/parts/{uuid}/1/1/chunks/0` resolves to `get_local_chunk`,
  not `/{bucket}/{key:path}`. That precedence is load-bearing and currently untested in either
  direction.
- api: with `HIPPIUS_PEER_SERVE_ENABLED=false`, the route is absent (404 from the S3 catch-all,
  and the handler is not in `app.routes`).
- gateway: unauthenticated `GET /internal/parts/...` is rejected at the gateway and
  `forward_request` is never called.
- `peer_serve_limiter` exists whenever the router is mounted (regression for A-8 item 1).
- **Handshake integration test — the one the rollout note is actually afraid of.** Every test
  above exercises one side of the handshake, so a header *name* or *casing* mismatch between
  `PeerChunkFetcher` and `get_local_chunk` passes all of them and takes the tier dark, caught only
  by the post-deploy staging metric check. Add a test that drives a real `PeerChunkFetcher` against
  the actually-mounted router (ASGI transport, no hand-built header) and asserts bytes come back.
  Both sides must derive the header name from one shared constant.

**Rollout.** Deploy the secret to staging and prod *before* the image, or the peer tier goes dark
on the first pod that restarts. This is a fail-closed change by design.

---

### WI-2 — P1 · An un-unlinkable part must not halt all eviction

**Problem.** [ssd_evict.rs:319](../../crates/hippius-drain-core/src/ssd_evict.rs) propagates
`unlink_part` failure with `?`, returning before `mark_evicted` at 327. `mark_evicted` →
`drop_residency` is the only thing that advances the cursor, and the worklist is ordered
`COALESCE(last_read_at, resident_at)` — stable — so a persistently failing part pins the head and
every subsequent pass dies on the same row having freed nothing. `evict_once` logs
`warn!("eviction pass failed; retrying next poll")`
([runtime.rs:391](../../crates/hippius-drain-agent/src/runtime.rs)) and neither `starved` nor
`skipped_unreplicated` is set, because the report is never produced. Both of the hardening plan's
eviction alerts stay silent through this failure.

Plausible triggers: `EIO`, `EACCES`, `EROFS`, and `ENOTEMPTY` from a concurrent `_promote_chunk`
renaming a `*.tmp.<uuid>` into the very directory being recursively removed. `unlink_part` maps
only `NotFound → Ok` ([localfs.rs:425-431](../../crates/hippius-drain-agent/src/localfs.rs)).

**Fix.** Count and continue instead of aborting:

- Add `EvictionReport::remove_failed: u64`.
- In the page loop, on `unlink_part` error: increment, log at `warn` with the part key, `continue`
  to the next candidate. Do **not** add the part to `evicted` and do not credit its bytes.
- **Move the `projected` credit to after a successful unlink.** Today the order is
  ([ssd_evict.rs:317-319](../../crates/hippius-drain-core/src/ssd_evict.rs)):

  ```rust
  projected = projected.saturating_add(candidate.bytes);
  remover.unlink_part(&candidate.part).await.map_err(EvictionError::Remove)?;
  ```

  Count-and-continue on top of that ordering creates a *phantom credit*: a part that failed to
  unlink still counts toward the `projected >= goal` early-stop, so the page stops evicting having
  freed less than it believes. It self-corrects on the next page — the probe re-reads real free
  space — but it wastes an iteration under exactly the disk pressure that makes iterations
  expensive. Advance `projected` only once `unlink_part` returns `Ok`.

**On alerting, scale this back from the original draft.** The existing
`if evicted.is_empty() { report.starved = true; break; }` already covers "a whole page failed to
unlink" — it simply never runs today because the `?` short-circuits first. So this fix *restores an
already-wired alert* rather than needing a new one. `remove_failed` still earns its place, but as
the **discriminator** between starved-because-unlink-is-failing and
starved-because-the-cursor-is-exhausted — two conditions with completely different operator
responses that are currently indistinguishable. Surface it in `evict_once` at `warn` when non-zero
and include it in the existing `starved` ERROR line; it does not need an alert rule of its own.

A persistently failing part now costs one failed unlink per pass — self-limiting, visible, and it
no longer blocks the parts behind it.

**Files.** `crates/hippius-drain-core/src/ssd_evict.rs`,
`crates/hippius-drain-agent/src/runtime.rs`, snapshot/metrics module, hippius-otel alert rules
(separate repo — file the PR alongside).

**Tests.** `cargo test -p hippius-drain-core --lib`:
- *a candidate whose unlink fails does not prevent the rest of the page from being evicted and
  marked* — fake remover fails on candidate 2 of 5; assert 4 evicted, 4 marked, `remove_failed ==
  1`.
- *a persistently failing head does not starve the pass across repeated calls* — same remover,
  two passes; assert the second pass evicts the same 4 and does not regress to zero.
- `remove_failed` is zero on the all-succeed path (guards against a counter that always fires).

---

### WI-3 — P1 · The promote floor must track the allocator's published reserve

**Problem.** `validate_promotion_band` enforces
`evict_reserve < promote_floor < evict_reserve + headroom` once, at wiring time, against
hardcoded `EVICT_RESERVE_RATIO = 0.150` / `EVICT_HEADROOM_RATIO = 0.050`
([fs_pressure.py:98-99](../../hippius_s3/fs_pressure.py)). The deployed evictor does not use that
constant: `let reserve = allocated_reserve_permille.unwrap_or(policy.reserve_permille)`
([runtime.rs:309](../../crates/hippius-drain-agent/src/runtime.rs)), and the allocator
interpolates 150 → 400 permille on drain severity
([alloc.rs `reserve_permille`](../../crates/hippius-drain-core/src/alloc.rs)). At an allocated
reserve of 400 the evictor holds 40% free while promotion keeps writing down to 17.5% — the
ordering inverts, and it inverts *precisely* when the allocator has decided the drain is in
trouble.

**Rejected option: clamp `max_reserve_permille` below 175.** The band only holds for a reserve in
(125, 175) at the shipped headroom, so clamping would confine the allocator to a 25-permille range
and delete the runway-buying behaviour Phase 4 exists for. That trades a real control-loop feature
for a static assertion.

**Fix — make the floor derived, not mirrored.** The agent is the only component that knows both
numbers, so it publishes and the api consumes:

1. Agent computes and publishes **`promote_floor_permille` already resolved** — not the inputs —
   to a TTL'd Redis key per node, alongside the existing pressure-signal pattern
   (`fs_cache:pressure`, [pressure_signal.py](../../hippius_s3/pressure_signal.py), is the
   precedent for the contract shape). Published every eviction poll (30s), TTL well above that.
   The agent already carries `redis` in its `Cargo.toml` ([enqueue.rs](../../crates/hippius-drain-agent/src/enqueue.rs)),
   so the publisher costs nothing new.
2. `FreeSpaceGate` reads that number under the same 5s memo it already uses for `statvfs`
   ([fs_pressure.py:29,68-84](../../hippius_s3/fs_pressure.py)) and divides by 1000. **Python does
   no policy arithmetic.**
3. **Absent or malformed key → the statically configured `promote_min_free_ratio`**, which
   `validate_promotion_band` still checks at startup. Fail-open to today's behaviour; a missing
   signal must not disable the read tier.
4. Log at `warn` + counter when the published floor and the static floor disagree, so the
   divergence is visible rather than merely handled.

**Why the resolved number and not the two inputs.** An earlier draft had the agent publish
`reserve_permille` + `headroom_permille` and Python compute `(reserve + headroom/2)/1000`. That
still mirrors — it just mirrors the *formula* instead of the constant, so the moment the Rust side
changes what headroom means or adds a knob, the Python expression is silently wrong again. That is
precisely the failure A-4 is. Publishing the resolved floor removes the second drift site, and it
puts the `reserve < floor < reserve + headroom` invariant in the language that owns the constants,
where it can be asserted as a Rust property test. The wire cost is identical.

Consequence worth stating: at a maximum allocated reserve the derived floor is ~42.5% free, so
promotion effectively stops on a stressed node. That is the intended behaviour — warming a cache
while the drain is stalled is the exact failure Phase A exists to prevent.

**Design decision to confirm during implementation.** Whether to publish a new key from the agent
or have the api read the allocator's existing per-node key directly
(`{prefix}alloc:{node}`, [coordination.rs:330](../../crates/hippius-drain-core/src/coordination.rs)).
Reading the alloc key adds no publisher but couples the api to the allocator's wire format and
misses the case where the agent falls back to its static floor. **Recommend the agent publishes
its effective band** — one-directional, and it is the number actually in force.

Independently of the mechanism: document the coupling at `alloc.rs reserve_permille`. Nothing on
the Rust side currently hints that a Python threshold depends on that number.

**Files.** `crates/hippius-drain-agent/src/runtime.rs`, `crates/hippius-drain-core/src/alloc.rs`
(doc), `hippius_s3/fs_pressure.py`, `hippius_s3/cache/__init__.py`, `hippius_s3/monitoring.py`.

**Tests.**
- Rust: the published floor tracks the effective reserve across `allocated_reserve = None` and
  `Some(400)`.
- Rust property test: for every reserve in 150..=400 and the shipped headroom, the computed floor
  satisfies `reserve < floor < reserve + headroom`. This is the assertion `validate_promotion_band`
  should have been making all along, now in the language that owns the constants.
- Python: `FreeSpaceGate` with a published floor of 425 permille refuses a promotion at 30% free
  that it would allow under the static floor; with no published floor it matches today's behaviour
  exactly.
- Python: a malformed / expired published value falls back rather than raising.

---

### WI-4 — P2 · Reserved name + a real RFC1918 check

Two small, independent fixes in one PR because both close A-1's neighbourhood.

**A-2 — `internal` is a reserved bucket segment.**
[reserved_bucket_names.py](../../hippius_s3/reserved_bucket_names.py) documents exactly this
invariant and lists two sources of danger (a gateway route; an auth exemption). This work
introduced a third: **an api-side route that shadows the S3 catch-all**. `internal` is 8
characters, a legal bucket name, and creatable today; any key in it matching
`parts/{uuid}/{int}/{int}/chunks/{int}` becomes permanently unreadable, and the globally-unique
name is burned.

- Add `internal` to `RESERVED_BUCKET_SEGMENTS`.
- Extend the module comment with the third source.
- New test enumerating the api's non-S3 routes and asserting each first *static* path segment is
  in the frozenset. The existing `test_every_auth_exempt_segment_is_a_reserved_bucket_name` walks
  the **gateway's** exempt segments and structurally cannot catch an api-side route.
- Run [report_reserved_name_buckets.py](../../hippius_s3/scripts/report_reserved_name_buckets.py)
  against staging and prod before merging, to check nobody already owns the name. If someone does,
  that is an ops decision, not a code one.

Note this does **not** fix A-1: reserved names gate `CreateBucket` only
([input_validation.py:86](../../gateway/middlewares/input_validation.py)), not `GET`.

**B-3 — `ip_whitelist` admits `172.0.0.0/8`.**
[ip_whitelist.py:36-38](../../hippius_s3/api/middlewares/ip_whitelist.py) uses
`startswith("172.")`, so the public addresses `172.15.0.1` and `172.32.0.1` pass. RFC1918 is
`172.16.0.0/12`.

- Parse with `ipaddress.ip_address`; deny on parse failure.
- Match against a CIDR list from config (`API_IP_WHITELIST_CIDRS`) defaulting to
  `10.0.0.0/8, 172.16.0.0/12, 192.168.0.0/16, 127.0.0.1/32, ::1/128` — configurable so a
  deployment can narrow to its actual pod/service CIDRs, which is the property A-1 exploits
  through the gateway. Do not hard-pin a cluster-specific CIDR here without measuring first; a
  wrong constant 403s the gateway.
- **Delete the fictional `API_IP_WHITELIST` from the docs in the same commit.**
  [hippius_s3/api/CLAUDE.md:27](../../hippius_s3/api/CLAUDE.md) says *"if `API_IP_WHITELIST` is
  configured, only allows those IPs"* — I grepped the tree: that name exists nowhere in
  `config.py` or any code, and the middleware hardcodes string prefixes. Introducing
  `API_IP_WHITELIST_CIDRS` while leaving that line in place would give one thing two names, one of
  them imaginary. Fix the doc to describe the real knob.
- Boundary table test: `172.15.255.255` deny, `172.16.0.0` allow, `172.31.255.255` allow,
  `172.32.0.0` deny, `10.0.0.1` allow, `9.255.255.255` deny, `::1` allow, garbage deny,
  `None` client deny.

---

### WI-5 — P2 · Validate the peer URL before fetching it

**Problem.** `PeerRegistry.resolve` returns whatever string sits in `hippius:peer:{node}` and
`PeerChunkFetcher.__call__` interpolates it straight into `self._client.get(...)`
([peers.py:157-170, 264-267](../../hippius_s3/cache/peers.py)). No scheme check, no private-IP
check, no port pin.

**On severity.** Agent A's downgrade from P1 to P2 is correct and I verified the reasoning: the
peer keys live on `config.redis_url`, the same instance `cached_auth` uses
([auth_cache.py:29-50](../../gateway/services/auth_cache.py),
[gateway/main.py:70](../../gateway/main.py)), so anyone who can `SET` peer URLs can already forge
an access-key auth result and impersonate any account. SSRF is strictly weaker than the capability
the precondition grants. Fix it as defence in depth, not as a gate.

**Fix.** Validate in `resolve` — one choke point, so no caller can bypass it:

- Parse with `urllib.parse.urlsplit`; require scheme `http`.
- Require the host to be a **literal** private IP (`ipaddress.ip_address(...).is_private`) — not a
  DNS name, so there is no resolution step to poison.
- Pin the port to the api port.
- Reject empty path/query/fragment.
- On rejection: return `None`, log at `warning` (this is never routine), and count
  `peer_fetch_shed_total{reason=bad_peer_url}`.

Prefer signing registration with a cluster secret later; not in this PR.

**Test.** Registry returns `http://169.254.169.254/` → fetcher returns `None` and issues **zero**
requests (mock transport asserts no calls). Table over: `file:///etc/passwd`, `https://evil.com`,
`http://8.8.8.8:8000`, `http://10.1.2.3:9999`, `http://10.1.2.3:8000` (the only accept).

---

### WI-6 — P2 · Bound and check peer response bodies

**Problem.** `PeerChunkFetcher` returns `response.content` for any 200
([peers.py:284-287](../../hippius_s3/cache/peers.py)) and `_promote_chunk` writes it to local
flash with no length check
([dual_fs_store.py:207-215](../../hippius_s3/cache/dual_fs_store.py)).

The serious arm needs no attacker: a peer running a rolled-back or half-deployed image that
answers 200 with a wrong-length body gets that body promoted onto local NVMe and served to every
subsequent local read until eviction, where AEAD then fails on every decrypt.

**An upper bound does not close that arm.** A rolled-back peer usually returns a *short* body, and
`len(data) <= chunk_size + overhead` accepts every short body — the bound has to be an upper bound
(the last chunk of a part is legitimately short), which is exactly why it cannot discriminate. The
original draft dismissed the exact check as "not worth a query"; that was wrong on cost, because
this path already performs a **synchronous residency-claim DB write per promoted chunk**
([dual_fs_store.py:207-215](../../hippius_s3/cache/dual_fs_store.py) → WI-7), so one more read is
marginal.

**Fix.** Three parts:

1. **Exact content check before promote.** Compare against `part_chunks.cipher_size_bytes` for
   this `(object_version, part_number, chunk_index)`. Fetch it alongside the residency claim WI-7
   already makes, so it costs no extra round-trip. The loose `chunk_size + overhead` bound stays
   as a cheap pre-filter for the transport tier below, where the exact size is not yet in hand.
2. **Transport bound with a real deadline.** Switch to `client.stream()` and abort past
   `chunk_size + AEAD overhead` — `await client.get()` has already buffered the body by the time
   you could inspect `Content-Length`. **Add an explicit overall deadline
   (`asyncio.timeout`) around the whole fetch.** Per §1, httpx has no total-response timeout, so
   `peer_fetch_timeout_seconds` bounds only the gap between chunks: a slow-drip peer can hold a
   connection and its buffer indefinitely *today*, before any streaming change. The deadline is
   therefore closing a pre-existing hole, not compensating for the switch — and it must be a new
   explicit value, not the 0.5s inter-chunk timeout reused.
3. On any mismatch: return `None` (fall through to the pool), count
   `peer_fetch_shed_total{reason=bad_length}`, and **do not promote**.

**Tests.** Oversized body → `None`, nothing written locally, counter incremented. **Short body
whose length is legal for the part but wrong for this chunk → rejected** (this is the test the
upper-bound-only design fails). A genuinely short *final* chunk → accepted. A drip-feeding peer
that sends one byte per 0.4s → aborted at the overall deadline, asserted against wall clock.

---

### WI-7 — P2 · Residency-claim failure must be counted, and must not leak disk

**Problem.** [residency.py:70-88](../../hippius_s3/cache/residency.py) logs a failed claim at
`debug` and returns. The comment is honest about the consequence and it is correct — I confirmed
`ssd_reclaim`'s `Replicated` arm is `report.skipped_replicated += 1` and nothing more
([ssd_reclaim.rs:392](../../crates/hippius-drain-core/src/ssd_reclaim.rs)) — so a replicated part
on disk with no residency row has no owner in either process and leaks permanently, on the disk
that 503s PUTs when it fills. Meanwhile the strictly less consequential `last_read_at` stamp
failure gets a first-class metric
([read_recency.py:87-89](../../hippius_s3/cache/read_recency.py)). The observability effort is
inverted relative to blast radius.

**Fix.**

1. Add `PromotionSkipReason` variant `residency_failed` and record it
   (`promotion_skipped_total` currently carries only `disk_pressure`,
   [monitoring.py:140,546](../../hippius_s3/monitoring.py)). Raise the log from `debug` to
   `warning`. Alert on non-zero.
2. **Invert the promote ordering**: claim residency *first*, and write the chunk only if the claim
   succeeded. `_on_promote` must therefore return a success boolean rather than `None`.

   **The primary rationale is fail-closed, not the accounting trade.** Claim-first means a
   residency-DB outage disables promotion entirely rather than leaking one unreclaimable copy per
   promoted chunk for the duration of the outage. Failing closed on an *optimisation* — the bytes
   are already served and the pool copy is authoritative — is straightforwardly correct, and it is
   a better argument than the one the first draft led with. The over-accounting trade (a residency
   row for bytes a subsequent failed write never put on disk) is real but secondary, and cheap:
   the evictor re-probes actual free bytes rather than trusting the accounted sum
   ([ssd_evict.rs:334-337](../../crates/hippius-drain-core/src/ssd_evict.rs)), so an over-accounted
   part costs one wasted candidate and self-corrects at the next eviction.

**Files.** `hippius_s3/cache/residency.py`, `hippius_s3/cache/dual_fs_store.py`,
`hippius_s3/monitoring.py`.

**Tests.** Claim raises → no chunk written, counter incremented, read still succeeds (the bytes
were already served). Claim succeeds → chunk written, `bytes` accumulate per chunk as today.

---

### WI-8 — P2 · Shared-disk startup guard (hardening §0-M2), agent only

**Problem.** The hardening plan required a startup check that logs loudly when accounted cache
bytes are far below `total − free`, so a process that does not own its filesystem cannot silently
drive free-space gates. It is not in the tree — not in agent startup, the api lifespan, or
`fs_pressure.py`. Staging still runs on shared `/dev/md3`, which is where the soak is happening,
so **every free-space gate in this work is currently unvalidatable on the environment that is
validating it.**

**Fix (agent only — see §2).** At agent startup, compare `Store::node_cache_bytes()` against
`total − free` from `statvfs`. If accounted < 50% of used, log `ERROR` with both numbers and set a
`drain_ssd_shared_filesystem` gauge to 1. **Do not refuse to start** — prod may legitimately have
small co-tenants — make the signal alertable instead.

**Test.** `cargo test`: a fake store/probe pair at 10% accounted trips the flag; at 90% it does
not; a probe error leaves the flag unset rather than defaulting either way.

---

### WI-10 — P2 · Invalidate a cached chunk that fails AEAD

**New work item, absent from both reviews and from the first draft of this plan.**

**Problem.** Nothing anywhere invalidates a locally-cached chunk when its decrypt fails.
`decrypter.py` has no `InvalidTag` handling; the exception propagates to the global handler, which
maps it to an S3 error ([errors.py:157](../../hippius_s3/api/s3/errors.py)) and leaves the bytes on
disk. Because `DualFileSystemPartsStore` checks the local tier **first**, that object is then a
permanent, **retry-immune** read failure on that node until the evictor happens to reclaim the
part — which, under retention, may be never on an uncontended node.

This is what upgrades B-4 from "a wasted fetch" to an availability bug, and it is the durable fix:
it protects against a bad chunk from *any* source (a torn write, bit rot, a half-deployed peer, a
future promoter), not just the one WI-6 length check catches.

**Fix.** On AEAD failure for a chunk served by the **local** tier: unlink that chunk, drop the
part's residency claim for this node, count `cache_chunk_invalidated_total`, and re-read through
the pipeline (which will fall to peer/pool). Constraints:

- **Only the local copy.** Never the pool copy — it is authoritative and the failure may be in the
  DEK, not the bytes. If the pool copy fails AEAD, that is a genuine 500 and must stay one.
- **Bounded retry.** One invalidate-and-retry per chunk per request, never a loop. A DEK-level
  fault would otherwise turn every read into a cache-wipe storm.
- **Attribute it.** Log which tier served the failing bytes, so a systemic poisoner is
  distinguishable from isolated corruption.

**Files.** `hippius_s3/reader/decrypter.py`, `hippius_s3/reader/streamer.py`,
`hippius_s3/cache/fs_store.py` (a narrow per-chunk unlink; only `trim_chunks_from` and
`delete_part` exist today, and neither has the right granularity), `hippius_s3/monitoring.py`.

**Tests.** A poisoned local chunk → first read invalidates and succeeds from the pool; the local
file is gone; the counter fires. A poisoned *pool* chunk → still an error, nothing unlinked. A
wrong DEK → one retry at most, then a clean error.

---

### WI-9 — P3 · Comment and observability cleanup

One PR, no behaviour change except the last two items.

- **A-6.** Mirror the accumulate-vs-overwrite comment onto `Store::record_resident`
  ([store.rs:1260-1278](../../crates/hippius-drain-core/src/store.rs)), naming
  `hippius_s3/cache/residency.py` as the other writer and stating the invariant that keeps them
  disjoint: a locally-resident part is served locally and therefore never promoted, which holds
  only because ingest writes every chunk *and* `meta.json` before the part is readable. A
  partially-promoted part (range GET) on a node that later drain-commits the same part would
  collide. Nothing in the schema enforces that this stays impossible — say so.
- **A-7(a).** `ssd_evict.rs` module doc §"Policy vs. mechanism" still says eviction is FIFO by
  `resident_at` and that node-local read recency "does not exist yet". It exists (migration 0017,
  `last_read_at`) and the query was changed to
  `ORDER BY COALESCE(r.last_read_at, r.resident_at)`
  ([store.rs:1168](../../crates/hippius-drain-core/src/store.rs)). Rewrite the section to describe
  LRU-with-FIFO-fallback.
- **A-7(b).** `partdrain.rs:554` claims `mark_replicated` stamps `resident_at` in the same
  statement as the commit. It does not — `mark_resident` is a separate statement issued first
  ([partdrain.rs:528-535](../../crates/hippius-drain-core/src/partdrain.rs)) and
  `mark_replicated`'s SQL touches only `status`/`corrupt_attempts`/`updated_at`. The design is
  safe (the window where residency exists at `status='draining'` is invisible to
  `evictable_parts`, which filters on `status='replicated'`), but the comment asserts the wrong
  mechanism for a genuine correctness property. State the real one.
- **B-6.** `chunks_exist_batch`'s comment
  ([dual_fs_store.py:242-247](../../hippius_s3/cache/dual_fs_store.py)) justifies itself with
  "under drain-direct the drain unlinks the primary SSD copy after replicating" — the behaviour
  this work removed. The conclusion is still right (a pool-only part must read as cache); only the
  cause is dead. It is now the evictor that unlinks, on a free-space policy. This is on the
  GetObject hot path, so the next reader will re-learn the wrong model.
- **B-7.** `_owner` uses `LIMIT 1` with no `ORDER BY`
  ([peers.py:209-218](../../hippius_s3/cache/peers.py)) — non-deterministic across replans, not
  merely arbitrary. Add `ORDER BY r.resident_at` to prefer the long-held ingest copy. The row set
  per part is bounded by the node count, so the sort is free.
- **B-8.** Extend `PeerShedReason` ([monitoring.py:29](../../hippius_s3/monitoring.py)) with
  `peer_miss` (404) and `peer_error` (other non-200), and record them at
  [peers.py:284-286](../../hippius_s3/cache/peers.py). Today a wholesale peer-tier failure is
  visible only as `chunk_reads_by_tier_total{tier=peer}` sagging — a negative-space alert, the
  weakest kind, and the same shape as A-3's silence.

---

## 4. PR sequence

Matches the consensus order in CODE_REVIEW.md §7, with WI-4's two halves merged and severities
corrected per §2.

| # | Work item | Gates promotion | Rough size |
|---|---|---|---|
| **0** | **Pre-flight: reserved-name audit on staging + prod** | ✅ blocks WI-1 | XS — read-only script |
| 1 | **WI-1** — close the public peer-serve path (A-1, A-8.1) | ✅ | M — Python + gateway + k8s secret |
| 2 | **WI-2** — unlink failure must not halt eviction (A-3) | ✅ | S — Rust |
| 3 | **WI-3** — promote floor tracks the allocated reserve (A-4) | ✅ | L — Rust + Python contract |
| 4 | **WI-4** — reserved `internal` + real RFC1918 + doc fix (A-2, B-3) | ✅ | S |
| 5 | **WI-7** — residency-claim metric + claim-before-write (A-5) | ✅ | S |
| 6 | **WI-5** + **WI-6** — peer URL allow-list + exact size check + deadline (B-1, B-4) | ✅ | M — same file |
| 7 | **WI-10** — invalidate a cached chunk that fails AEAD | ✅ | M |
| 8 | **WI-8** — shared-disk startup guard (B-5) | ✅ | S — Rust |
| 9 | **WI-9** — comment + observability cleanup (A-6, A-7, B-6, B-7, B-8) | — | S |

Two ordering constraints beyond the original draft's:

- **The reserved-name audit is now step 0, not part of WI-4.** WI-1's gateway denylist rejects
  every request whose first segment is `internal`; the audit is what tells you whether a customer
  already owns that bucket. Running it four PRs downstream meant a silent outage would go unlooked-for
  through three more merges. It is a read-only script — there is no reason it is not first.
- **WI-7 moves ahead of WI-6.** WI-6's exact-size check reads `part_chunks.cipher_size_bytes`
  alongside the residency claim, and WI-7 is what restructures that claim (returning a success
  boolean, moving it before the write). Landing WI-6 first means writing that query twice.

WI-1 through WI-3 remain independent of each other and can run in parallel once step 0 clears.
WI-9 last, so it does not conflict with the substantive changes above it.

One out-of-repo follow-up: the `drain_ssd_shared_filesystem` alert rule (WI-8) goes to
**hippius-otel** and should land with its code PR. WI-2 no longer needs a new rule — it restores an
existing one (see WI-2).

Recorded as structural follow-ups, not scheduled here: the **peer-serve port split** (the durable
answer to A-1, versus the path denylist that will rot) and a **measurement of
`HIPPIUS_PEER_SERVE_MAX_INFLIGHT`**.

## 5. Verification gates

Per PR, before merge:

- `pytest tests/unit -q` — baseline 2477 passed / 37 skipped.
- `cargo test -p hippius-drain-core --lib` — baseline 225 passed.
- `ruff check . --fix && ruff format .` and `ty check hippius_s3 gateway`.
- `cargo clippy --all-targets --all-features -- -D warnings` and `cargo fmt`.
- Every new test verified to fail against the unfixed code before the fix lands (break-it-first,
  per repo convention).

Before prod promotion, on staging with the soak running:

- `HIPPIUS_PEER_SERVE_ENABLED` on, secret deployed, peer tier serving — confirm
  `chunk_reads_by_tier_total{tier=peer}` is non-zero, i.e. WI-1 did not silently kill the tier.
- Unauthenticated `GET https://<staging-gateway>/internal/parts/...` returns an S3 error, not
  ciphertext.
- `drain_ssd_shared_filesystem` reports 1 on staging's shared `/dev/md3` — that is the guard
  working, and it is the reason the free-space numbers from this soak need reading with care.

## 6. Runs in parallel, not behind this queue

**B-2 — `UploadPart` re-upload after `replicated` never re-drives the pool.** A real P1 integrity
bug: `record_landed_part`'s `ON CONFLICT ... DO UPDATE SET node_id = EXCLUDED.node_id WHERE
cephor_replication_status.node_id IS NULL` ([store.rs:589-593](../../crates/hippius-drain-core/src/store.rs))
never resets `status`, the reconciler leaves a `Replicated` row alone
([reconcile.rs:211](../../crates/hippius-drain-core/src/reconcile.rs)), and neither
`wake_version_replication` nor `fail_version_replication` re-drives it
([mpu_cleanup.py:58-85](../../hippius_s3/services/mpu_cleanup.py)). Because the retry reuses the
same `object_version` and `upload_id`, the DEK/AAD/nonce are unchanged, so the *stale* ciphertext
AEAD-verifies cleanly — this fails silently with wrong plaintext, not with a decrypt error.

It does not **gate** this promotion — it is pre-existing under drain-direct, separable from every
PR here, and would not be fixed by reverting any of them. But it must not **queue behind** them
either, and the original consensus order (a slot after eight PRs) had that backwards for two
reasons:

1. **It is the only item in either review that returns wrong plaintext with no error.** Everything
   else in this document is an availability, observability, or trust-boundary problem. Silent
   corruption outranks all of them.
2. **This work makes it worse, not neutral.** Per §2: retention makes the ingest node's correct
   copy more durable (both agents' point, and correct), but promotion now caches the *stale* pool
   copy fleet-wide. Longer-lived and wider once it arises is a worsening, not a wash.

**Open a drain issue now and staff it independently of this queue.** Of the three fix options in
CODE_REVIEW.md §3, option 3 (content-hash the SSD set at drain time and re-drive on divergence) is
the honest one — the reconciler already walks complete parts, and options 1 and 2 either guess at
when SSD content can change or break S3 semantics. Do not let it inherit this plan's sequencing.

## 7. Explicitly out of scope

**`HIPPIUS_PEER_SERVE_MAX_INFLIGHT = 16` is an unmeasured guess.** Open question 3 in the review;
no measurement artefact exists in-repo. Worth a soak measurement of what a peer's NVMe + uvicorn
sustains without hurting its own ingest, but the constant existing at all beats the prior unbounded
path, and it matters less than the trust boundary WI-1 closes.

**Prod carries no `CEPHOR_EVICT_*`**, so the first prod binary carrying this work runs the evictor
on code defaults. Recorded, not fixed here — that is a governance decision about eviction defaults,
adjacent to the retention-default question the hardening plan filed as F0.
