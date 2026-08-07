# Hardening the SSD read tier before it meets production load

Status: proposed, 2026-08-07. Targets `staging` only; `k8s/production` stays untouched until each
phase clears its soak gate. Follows on from
[2026-08-06-ssd-read-tier-retention-and-residency-allocator.md](2026-08-06-ssd-read-tier-retention-and-residency-allocator.md),
which shipped as PR #398 (merged to `staging`, commit `814f404a`).

The read tier itself is sound: reads tier local → peer → pool, the durability invariant is
defended in three independent places, and the allocator no longer reads a warm cache as a drain
emergency. What is not sound is what happens when the tier actually **fills**. Three of the
findings below are load-scaling defects that are invisible at today's 1–2% disk occupancy and
become outages at the occupancy the design is aiming for.

**The one-sentence risk:** promotion is an unthrottled writer to the same NVMe that ingest writes
to and that `fs_cache_pressure` gates PUTs on, and the only counterweight is an evictor capped at
512 parts per 30 s. Everything else on this page is secondary to closing that loop.

---

## 0. Task 0 — MEASURED (2026-08-07, context `hippius`, read-only)

Everything below replaces the derived estimates. Where a prior figure was an estimate it is shown
against the measurement, because the plan's credibility depends on knowing which is which.

**Deployment state.** Staging runs retention **live now** (`drain:814f404`, `CEPHOR_EVICT_*` set,
`HIPPIUS_PEER_FETCH_ENABLED=true`, `HIPPIUS_OBJECT_CACHE_PROMOTE_ON_READ=true`). Prod is on
`drain:fa927d8` — pre-retention, with `CEPHOR_REPLICATED_RECLAIM_GRACE_SECS=3600` live and read.
F0 is confirmed exactly as written.

**Prod, per node** (`cephor_replication_status` ⋈ `parts`):

| | measured |
|---|---|
| replicated parts per node | **2.25–2.32 M** (node1 2,317,894 … node2 2,254,208) |
| shard bytes per node | **906–951 GB** |
| average part in the shard | **408 KB** |
| ingest SSD | `/dev/nvme4n1`, **3.5 TB**, `du` = `df` = 138 GB (**dedicated device**) |
| fleet | 11,406,007 replicated · 22,123 failed · 12 pending |

**Prod `parts` size distribution is extreme and bimodal:** n = 142,898,360, **p50 = 686 bytes**,
avg = 1321 kB, p95 = 4096 kB, max = 14 GB.

**Estimates vs measurements — the estimates held:**

| Quantity | Estimated | Measured | |
|---|---|---|---|
| parts per node shard | 2.2 M | **2.28 M** | ✓ |
| average part in shard | 487 KB | **408 KB** | ✓ |
| DB round-trips per reconcile tick | ~4,400 | **~4,560** | ✓ |
| shard as share of disk | 28% | **26.6%** | ✓ |
| eviction rate (512 / 30 s) | 8 MB/s | **7 MB/s** | ✓ |
| deficit at arming (50‰) | 192 GB | **175 GB** | ✓ |
| time to close one deficit | 6.4 h | **~7 h** | ✓ |

So F1 and F2 are confirmed at prod scale with real data: 2.28 M parts walked and ~4,560 batched
queries every 15 s per node, and an armed evictor needing ~7 hours of continuous `ERROR "starved"`
to close a single deficit.

### Three things the measurements changed

**M1 — `CEPHOR_EVICT_BATCH` counts parts while the deficit is in bytes, and p50 is 686 bytes.**
New finding, only visible with the real distribution. A 512-part page is almost uncorrelated with
bytes freed: walking the small-part tail frees as little as 512 × 686 B ≈ **350 KB**. Phase B's fix
must be **byte-driven** — page until the byte deficit is met — and the page size should adapt, not
be a fixed part count. A count-based batch is the wrong unit for this workload.

**M2 — staging cannot validate any free-space gate.** The staging ingest "disk" is `/dev/md3`, an
**878 GB filesystem shared with other tenants on the same node**:

```
du  /var/lib/hippius/local_object_cache =   1.2 GB   (matches drain_ssd_cache_bytes exactly)
df  /dev/md3                            = 628 GB used / 878 GB, 205 GB free (23%)
```

Prod is unaffected — `du` == `df` == 138 GB on a dedicated `nvme4n1`, so the shipped `statvfs`
accounting is **correct in production**. But on staging the evictor, `fs_cache_pressure`, and any
promotion gate all reason about 628 GB they neither own nor can evict. Consequences:

- Staging's evictor will arm at 15% free **because of another tenant's usage**, find 1.2 GB of
  evictable cache against a ~44 GB deficit, evict the entire staging read tier, and report
  `starved` forever. That is live and imminent (23% free and drifting).
- Every free-space soak gate in §4 is unmeasurable there.

Actions: give staging a dedicated device (infra), **and** add a cheap startup check that logs
loudly when `du(cache) ≪ (total − free)` — "this process does not own this filesystem" — so the
confusion cannot recur silently. That check is worth having in prod too as a regression guard.

**M3 — the corrected promote floor from §5-A2 was wrong in the other direction.** With
`evict_reserve = 0.15` and `evict_headroom = 0.05`, the evictor frees to **0.20 free and no
further**. A promote floor of 0.25 therefore **deadlocks**: promotion stops below 0.25, eviction
never restores past 0.20, promotion never resumes. Staging is at 23% free today, so Phase A as
last written would have disabled promotion permanently on deploy and the soak would have proved
nothing.

The correct constraint is a four-way ordering with the floor **inside** the evictor's band:

```
fs_cache_min_free (0.08)  <  evict_reserve (0.15)  <  promote_floor  <  evict_reserve + evict_headroom (0.20)
```

Take **`promote_floor = 0.175`**. Then: above 0.20 both are quiet; at 0.175 promotion stops while
eviction is still unarmed; at 0.15 eviction arms and frees back to 0.20, which is above the floor,
so promotion resumes. The band is real and promotion recovers after every pass.

**This is the test that matters** — not "the thresholds are ordered", but "the evictor's target is
strictly above the promote floor", which is what makes the loop live rather than deadlocked.

### What is already working

Staging's read tier is serving: `chunk_reads_by_tier_total` shows **local 26/100/111, peer 20/27/48,
pool 35** across pods — the pool is already the minority tier. And
`peer_fetch_shed_total{reason=client_cap} = 9` confirms F6/Phase G is real and observable, on a
2-node staging where the *serve* cap cannot possibly be the constraint (1 remote pod × 8 < 16).

Note for §5-A3: staging has **2** api-local pods, not 5. The peer oversubscription arithmetic
(4 remote pods × cap vs serve cap) is a **prod-only** condition and cannot be soaked on staging.

## 1. Verified findings

Each was re-derived from the code, not from the PR description. "Uncertain" states honestly what
is estimated rather than read.

### F0 — Retention is not flag-gated, and prod's manifest still names the backstop it removed

*Added 2026-08-07 from an independent review. This one is about release governance, not code, and
it is the reason nothing else on this page should ship to prod casually.*

`drain_part` retains unconditionally ([partdrain.rs:549-555](../../crates/hippius-drain-core/src/partdrain.rs))
and `ssd_reclaim`'s `Replicated` arm skips unconditionally
([ssd_reclaim.rs:363](../../crates/hippius-drain-core/src/ssd_reclaim.rs)). There is **no config
knob for retention** — `rg 'retain|retention' crates/hippius-drain-agent/src/config.rs` returns
only doc comments. So:

- **Retention arrives with the binary.** `k8s/production` being untouched does not mean prod is
  unaffected; the next `staging → main` promotion turns retention on in production, since main
  deploys prod ([f43af742](../../.github/workflows/production-deploy.yaml)). Verified today:
  none of `c3bd1c34`, `32e0feda`, `7ec187c2`, `814f404a` are ancestors of `origin/main`, and
  staging is 20 commits ahead — so this is a **future** release hazard, not a live one.
- **Rollback is a binary revert, not a config flip.** Every other phase here has a kill switch.
  Retention itself does not.
- **Prod still sets `CEPHOR_REPLICATED_RECLAIM_GRACE_SECS=3600`**
  ([k8s/production/drain-agent-daemonset.yaml:151](../../k8s/production/drain-agent-daemonset.yaml)),
  an env var the new binary **no longer reads** (removed from `config.rs`). The manifest documents
  a safety backstop that silently stops existing on promotion.

**Severity, stated honestly.** Acute risk is low: with `HIPPIUS_OBJECT_CACHE_PROMOTE_ON_READ` off
in prod, retention alone fills to the node's ingest shard (~1.08 TB of ~3.8 TB ≈ 28%), which never
reaches the evictor's 15%-free floor. The disk does keep growing with the dataset, so eviction
arms eventually, but not on day one. The problem is governance: an unflagged, un-revertable
behaviour change riding a routine release, with a stale manifest asserting a protection that is
gone. Fix before promotion, not after.

### F1 — The SSD scanners are unbounded, and retention multiplies their input

`LocalSsd::scan_parts` ([localfs.rs:697](../../crates/hippius-drain-agent/src/localfs.rs)) is a
three-level recursive `read_dir` with a `stat` per part
([localfs.rs:685](../../crates/hippius-drain-agent/src/localfs.rs)), returning an unbounded
`Vec<DiscoveredPart>`. It has two callers:

- `reconcile_parts` ([reconcile.rs:185](../../crates/hippius-drain-core/src/reconcile.rs)) — every
  **15 s** (`CEPHOR_RECONCILE_POLL_SECS`). Clones every `PartKey` into a second `Vec`, then
  `Store::statuses` chunks the lookup at `RECLAIM_STATUS_BATCH = 500`
  ([store.rs:92](../../crates/hippius-drain-core/src/store.rs)).
- `reclaim_ssd` ([ssd_reclaim.rs:286](../../crates/hippius-drain-core/src/ssd_reclaim.rs)) — every
  **300 s**, same shape.

Until PR #398 the SSD held only the undrained backlog (21–36 GB/node, prod 2026-08-06). Retention
makes it hold the node's shard, and promotion makes it hold whatever reads touch. Agent memory
limit is **1 Gi** ([drain-agent-daemonset.yaml:218](../../k8s/staging/drain-agent-daemonset.yaml)).

**The 15 s poll was chosen on a premise retention destroyed, and the manifest still states it.**
[drain-agent-daemonset.yaml:179-181](../../k8s/staging/drain-agent-daemonset.yaml) justifies the
setting with: *"Safe to lower: a scan is a walk of the node-local SSD ingest tree (undrained parts
only — drained parts are unlinked …)"*. That clause is now false, and it is the written safety
argument for the interval. Same text in the prod manifest at lines 181-182. The comment must be
corrected in the same PR that changes the behaviour — a stale safety rationale is how the next
person re-lowers this poll.

**Uncertain — and this is an estimate, not a measurement.** The part count is derived from the
prior plan's own figures: 5.4 TB pool ÷ 11.08M `replicated` rows ≈ 487 KB average part, 1.08 TB
shard ≈ 2.2M parts/node → ~4,400 Postgres round-trips per reconcile tick per node, and roughly
700 MB peak resident across the two `Vec`s and the `HashMap`. If parts average a full 4 MiB chunk
it is 270k parts and ~540 round-trips. **Measure before sizing the fix** (§4, task 0).

Note what that 700 MB figure is and is not: tight against a 1 Gi limit, not a demonstrated OOM.
The argument for fixing this is that the walk is **unbounded in principle** and its input now grows
with promotion — not that a specific crash is predicted. Either part-count figure is 5–40× the
present load; the direction is not in doubt, the magnitude is.

### F2 — Eviction cannot close its deficit, and mislabels the failure

`evict_to_target` ([ssd_evict.rs:184](../../crates/hippius-drain-core/src/ssd_evict.rs)) makes
exactly one `evictable_parts(batch)` call and never loops. `evict_once`
([runtime.rs:330](../../crates/hippius-drain-agent/src/runtime.rs)) is the only call site; nothing
wraps it in a loop.

Two separable defects:

- **F2a (certain).** `starved = freed_bytes < deficit` treats "the batch ran out" and "the
  worklist ran out" as the same condition, and `evict_once` logs `tracing::error!` on it. All four
  unit tests use `batch = 128` against ≤ 3 candidates, so batch truncation is untested. Armed, the
  pass must free `headroom` = 50‰ of a ~3.8 TB disk ≈ **192 GB**, while one pass frees at most
  512 × 487 KB ≈ **250 MB**. The signal that means "PUTs are about to 503" will sit at ERROR for
  hours of entirely normal catch-up.
- **F2b (structural, rates estimated).** Sustained eviction is capped at `batch / evict_poll` =
  512 parts / 30 s ≈ 8 MB/s/node. Its input — promotion — has no cap at all.

**Correction to an earlier reading:** retention *alone* never arms the evictor. The shard is
~28% of the disk and the floor is 15% free. It is promotion that fills the disk, which makes F2
a promotion-scaling problem, not a retention one.

### F3 — Promotion has no free-space guard (root cause of F2b)

`_promote_chunk` ([dual_fs_store.py:121](../../hippius_s3/cache/dual_fs_store.py)) writes
unconditionally — no `statvfs`, no pressure check, no rate limit — and every non-local read
promotes, peer hits included ([dual_fs_store.py:86](../../hippius_s3/cache/dual_fs_store.py)).

`should_reject_fs_cache_write` measures `shutil.disk_usage(config.object_cache_dir)`
([fs_pressure.py:20](../../hippius_s3/fs_pressure.py)). On `api-local` that is
`/var/lib/hippius/local_object_cache`
([api-local-deployments-staging.yaml:145](../../k8s/staging/api-local-deployments-staging.yaml)) —
**the same mount as the drain agent's `CEPHOR_SSD_ROOT`**
([drain-agent-daemonset.yaml:108](../../k8s/staging/drain-agent-daemonset.yaml)).

So promotion, ingest, and the PUT-503 gate share one disk with no coupling between them except a
rate-limited evictor. A read-heavy period can refuse writes. Promotion failures are swallowed
(`OSError` caught), so the disk fills silently until the middleware starts shedding.

### F4 — The Phase-4 reserve measures the wrong thing (downgraded)

`reserve_permille` ([alloc.rs:213](../../crates/hippius-drain-core/src/alloc.rs)) computes
shortfall as `(demand − budget) / demand`, and `distribute` sets
`demand = if backlog == 0 { 0 } else { max_drain_rate }`
([alloc.rs:254](../../crates/hippius-drain-core/src/alloc.rs)). `CEPHOR_MAX_DRAIN_RATE_BPS` is set
nowhere in `k8s/` → the 100 MB/s code default.

**Correcting an earlier overstatement:** `CEPHOR_CEPH_CEILING_BPS` is also unset → 1 GB/s default,
above the fleet's 500 MB/s aggregate demand, so under a healthy Open ceiling with the AIMD ramped
the shortfall genuinely can be 0 and the base reserve applies. The accurate claim is narrower:
shortfall is a ratio of a **calibrated constant** to the granted budget and is insensitive to how
far behind the node actually is. It reads near-max in every degraded regime — NearFull caps the
fleet at 50 MB/s → 10 MB/s/node → 900‰ → reserve ≈ 375‰ — and the AIMD has historically sat far
below the ceiling. It only *binds* once the cache exceeds ~60% of disk, so: real, P1 not P0.

### F5 — The peer tier excludes the case only it can fix

`PeerChunkFetcher._owner` requires `s.status = 'replicated'`
([peers.py:180](../../hippius_s3/cache/peers.py)), and residency rows exist only for replicated
parts, so a fresh part that lives *only* on its ingest node's SSD is unreachable. That is exactly
the documented staging failure — "every cross-node GET of a fresh object 503'd (parts not ready)"
([drain-allocator-deployment.yaml:107](../../k8s/staging/drain-allocator-deployment.yaml)).
`chunks_exist_batch` ([dual_fs_store.py:196](../../hippius_s3/cache/dual_fs_store.py)) also never
consults the peer tier, so such a read is routed into a download pipeline that cannot succeed.

Scope gap in what shipped, not a defect in it.

### F6 — Read-path tuning

- Promotion is `await`ed before the chunk is yielded. Prefetch = 16 with `create_task`
  ([streamer.py:54](../../hippius_s3/reader/streamer.py)) overlaps most of it. **Low.**
- Client per-peer cap is 8, prefetch depth is 16, so one reader of a large part sheds half its own
  chunks to the pool and books them as `client_cap`. Already commented in
  [peers.py:218](../../hippius_s3/cache/peers.py); it skews `chunk_reads_by_tier_total{tier=peer}`
  low for the whole soak.
- `HIPPIUS_PEER_FETCH_TIMEOUT_SECONDS = 2.0` against a ~40 ms pool fallback — a 50× loss-cut.

### Retracted

The residency byte-accounting difference (Rust `bytes = EXCLUDED.bytes` vs Python
`bytes + EXCLUDED.bytes`) is **not reachable**. The drain records only at its own commit; a local
read never promotes because it hits locally; eviction deletes row and directory together. Worth a
comment reconciling the two, not a fix.

---

## 2. Fix phases

Ordered so the cheapest outage-preventing change lands first and nothing depends on a later phase.
Every phase is independently revertable and carries a config kill-switch.

### Phase 0 (P0, do first) — Make the retention rollback real, and stop the manifest lying

**Rewritten 2026-08-07 after adversarial review. The first draft proposed a
`CEPHOR_RETAIN_REPLICATED` flag and deleting the dead env var. Both were wrong; one was harmful.**

**Retention already has a working rollback: the container image.** Verified — the pre-#398
`ssd_reclaim` unlinks a `replicated` part once `status.age >= graces.replicated`
(`git show dcbe9ef1:crates/hippius-drain-core/src/ssd_reclaim.rs`, the `Replicated` arm). So
`kubectl rollout undo daemonset/drain-agent` both restores unlink-on-commit **and self-heals the
retained cache** within `CEPHOR_REPLICATED_RECLAIM_GRACE_SECS` (3600 s in prod). An immutable image
plus a tested `rollout undo` is the standard mechanism; a flag would be a second one.

**So do NOT delete `CEPHOR_REPLICATED_RECLAIM_GRACE_SECS` from
[k8s/production/drain-agent-daemonset.yaml:151](../../k8s/production/drain-agent-daemonset.yaml).**
It is dead for the new binary and **load-bearing for the rollback path**. Deleting it — which the
first draft called "manifest hygiene" — would have removed the grace that makes a rollback drain
the disk, leaving a rolled-back fleet holding a full cache with nothing to sweep it. Keep it and
comment it as rollback-only.

**And do NOT add `CEPHOR_RETAIN_REPLICATED`.** It keeps the pre-#398 code path alive as a
backward-compatible shim, which [CLAUDE.md](../../CLAUDE.md) forbids outright ("Replace, don't
deprecate… No backward-compatible shims"; "don't add flags unless users actively need them"). It
also buys nothing operationally: an env change requires a pod restart, so it is no faster than
`rollout undo`, while adding a second reachable behaviour to test forever.

**Change, therefore:**

1. Comment `CEPHOR_REPLICATED_RECLAIM_GRACE_SECS` in both manifests as *read only by the
   pre-retention binary; retained deliberately so an image rollback sweeps the retained cache*.
2. Correct the stale "drained parts are unlinked" rationale for the 15 s poll at
   [staging:179-181](../../k8s/staging/drain-agent-daemonset.yaml) / prod:181-182.
3. **Exercise the rollback on staging and measure it** — deploy retention, let occupancy build,
   `rollout undo`, and confirm SSD occupancy returns to backlog-only within the grace. A rollback
   nobody has run is a hypothesis.
4. Add the release note: *this binary changes production behaviour with no config flag; rollback
   is `rollout undo`.*

**Why first.** Not because retention is unsafe, but because its rollback was undocumented,
untested, and about to be silently disabled by a manifest cleanup.

### Phase A (P0) — Close the promotion/ingest loop

**Change.** Gate `_promote_chunk` on free space. Add a memoized pressure read (~5 s TTL, same
shape as `get_published_pressure_mode`'s `_PUBLISHED_MEMO_SECONDS` in
[pressure_signal.py](../../hippius_s3/pressure_signal.py)) and skip promotion when free ratio is
below `HIPPIUS_PROMOTE_MIN_FREE_RATIO`. Count skips as
`promotion_skipped_total{reason=disk_pressure}`.

**Threshold, twice corrected — see §0-M3 for the measurement that settled it.** The first draft
used 0.20, which sits *exactly on* the evictor's target (150‰ + 50‰) — degenerate chatter. The
review then raised it to 0.25, which is worse: promotion stops below 0.25 and eviction only
restores to 0.20, so promotion **never resumes**. Staging sits at 23% free today, so that version
would have switched promotion off permanently on deploy.

**`HIPPIUS_PROMOTE_MIN_FREE_RATIO = 0.175`**, inside the evictor's band:

```
fs_cache_min_free 0.08  <  evict_reserve 0.15  <  promote_floor 0.175  <  evict target 0.20
```

The load-bearing assertion is **`evict_reserve + evict_headroom > promote_floor`** — the evictor's
target must be strictly above the floor, or the loop is dead. Ordering alone is not enough; both
prior drafts satisfied a naive ordering test and were still broken.

**Design caveat, recorded because it is a real limit of this approach.** A cache that *refuses
admission* when full cannot adapt its hot set; the textbook behaviour is to *evict* and admit. This
gate is deliberately the cruder option — it is 40 lines and it protects ingest, which is the
outage risk — but it means that at steady-state fill, promotion is effectively throttled to the
evictor's duty cycle. That is acceptable only because Phase H makes eviction recency-aware; a FIFO
evictor plus an admission gate would freeze whatever happened to be cached first. **Do not ship
Phase A's floor without Phase H on the same roadmap.**

**Why first — and what that claim is worth.** Not because it matters most: Phase B is at least as
load-bearing. A goes first because it is ~40 lines, needs no migration and no Rust, and cannot
break anything, so it buys headroom while the riskier changes are still in review. The risk it
addresses is steady-state churn after the disk saturates, **not** a burst — promotion only fires
on a miss, so its rate decays as the cache warms. It is self-limiting in rate, not in total.

**Kill switch.** `HIPPIUS_OBJECT_CACHE_PROMOTE_ON_READ=false` (already exists).

**Staging cannot gate this** (§0-M2): the shared `/dev/md3` means the free ratio staging measures
is 99.8% other tenants'. Until staging has a dedicated device, Phase A ships validated by unit
tests plus a **prod** observation of `promotion_skipped_total`, not by a staging soak.

### Phase B (P0) — Make eviction close its deficit and tell the truth

**Change.** Loop inside `evict_to_target`: page → unlink → `mark_evicted` → re-query, until the
deficit is met, a page returns fewer than `limit` rows (genuine exhaustion), or a per-pass budget
is hit. `mark_evicted` → `drop_residency` DELETEs the rows, so a re-query returns fresh candidates
— the loop is safe by construction, but the mark **must** precede the re-query or the pass spins
on the same page. Set `starved` **only** on genuine exhaustion; add
`EvictionReport::budget_exhausted` for the "more to do next tick" case, logged at INFO.

Per-pass budget: wall-clock (`CEPHOR_EVICT_MAX_PASS_SECS`, default 10 s) rather than a part count,
so the bound tracks actual disk cost rather than a proxy. Carry the remainder to the next tick.

**M1 — the page size must stop being the unit of progress.** Measured prod `parts` p50 is **686
bytes** against a 408 KB shard mean: the distribution is bimodal, so a fixed 512-*part* page is
almost uncorrelated with bytes freed. Walking the small-part tail frees ~350 KB per page against a
175 GB deficit. `CEPHOR_EVICT_BATCH` therefore becomes the **page size for one query**, not the
pass budget — the loop keeps paging until the *byte* deficit is met, exhaustion, or the wall-clock
budget. Without this the loop alone would just make 342 queries of 350 KB each and still time out.

**A9 — re-probe free space per page.** The deficit is derived from one `statvfs` at pass start and
then acted on for up to 10 s while ingest writes concurrently. Re-probe between pages so the pass
converges on reality rather than on a stale target.

**Why.** Without it the fix in Phase A is the *only* thing holding the disk, and the one alert
that would tell us it failed is already crying wolf.

**Kill switch.** `CEPHOR_EVICT_MAX_PASS_SECS` → small value restores near-current behaviour;
`CEPHOR_EVICT_RESERVE_PERMILLE=0` disables eviction entirely.

### Phase C (P0) — Bound the SSD scanners

**Revised 2026-08-07 after review: C2 comes FIRST, and mtime pruning is dropped.** The original
ordering had it backwards. Reasoning kept below, because the discarded option is the one a future
reader will otherwise re-propose.

**C2 (do this) — the api announces the landed part; the drain agent writes the row.**

**Reshaped 2026-08-07 after adversarial review.** The first draft had the api `INSERT` into
`cephor_replication_status` directly. That gives the table **two writers across two services with
two separate migration systems** (`crates/hippius-drain-core/migrations/` and
`hippius_s3/sql/migrations/`) — and this repo already established the opposite convention for the
same handoff: the drain is the *sole producer* of upload requests, publishing via Redis rather than
letting the api write the consumer's state.

Use the same shape. On finishing a part — strictly **after** `meta.json`, the readiness gate — the
api `LPUSH`es a landed-part notice onto a per-node queue on `redis-queues`. The drain agent
consumes it and calls its own existing `record_landed_part`. Benefits over the direct write:

- `cephor_replication_status` keeps exactly one writer and one migration owner.
- No new flag: an agent that never sees a message behaves as it does today, so the rollout is the
  api-side publish alone, with the consumer inert until it arrives.
- Identical failure profile — a lost message means the part is picked up by the reconciler
  backstop, which is precisely what a failed `INSERT` would have done.

The reconciler then demotes from *sole trigger at 15 s* to *backstop at 10 min*, and the walk leaves
every latency path — with **no pruning heuristic, no mtime invariant, and no discovery window**.

Verified prerequisite: the api's `NODE_NAME` and the agent's `CEPHOR_NODE_ID` both derive from
`spec.nodeName`, so per-node routing is sound — but that is two manifests independently holding the
same `fieldRef` with nothing asserting it. Add a startup log of both, and a soak guard that the
queue is drained by the agent on the same node.

The poll change is a **separate commit and separate deploy** from the publish, so rolling back one
does not force the other.

**C3 — the low-risk half of the original C1, ships either way.** Independent of C2:

- *Reclaimer:* drive the `failed` disposition from the DB
  (`status='failed' AND node_id=$1 AND updated_at < now()-grace`) instead of from the walk. Keep
  the FS walk **only** for the no-DB-row orphan case and drop its cadence from 300 s to hourly —
  `CEPHOR_ORPHAN_RECLAIM_GRACE_SECS` is already 24 h, so hourly is ample.
- Add `drain_scan_parts_total` and `drain_scan_duration_seconds` so F1 cannot return unnoticed.

**Rejected: mtime pruning of the reconciler walk.** The invariant is sound — a part dir can only
appear by `mkdir`, which bumps its parent version dir's mtime — but it is the wrong invariant. A
part is not *discoverable* until `meta.json` lands, and writing meta bumps the **part** dir's
mtime, not the version dir's. On a slow multi-chunk upload the version dir's mtime is stale by the
whole upload duration, so a pruned scan skips the part and it waits for the next full sweep.

That converts a 15-second drain trigger into a worst-case one-hour window in which the part's
**only durable copy is a single SSD** — a durability regression traded for scan cost, which is the
wrong direction on this system. A fix exists (have the api `os.utime` the version dir when it
writes meta, making the invariant exact), but if the api is being touched at all then C2 is barely
more work and strictly better. Do not resurrect pruning unless C2 is rejected outright.

### Phase D (P1) — Make the eviction reserve measure drain lag

**Change.** Compute shortfall in `reserve_permille` from `backlog` and `budget` directly rather
than reusing `demand`: time-to-drain, `backlog / max(budget × horizon, 1)` clamped to 1000‰, with
`horizon` = the allocator tick (`DEFAULT_TICK` = 5 s) × a configurable multiplier. Requires
carrying `backlog` onto `Entry` in `distribute`. **Deliberately does not touch the budget
water-fill** — only the reserve derived from it.

### Phase E (P2) — Peer-serve fresh parts

**Change.** A second resolver over `cephor_replication_status.node_id WHERE status IN
('pending','draining')`, consulted when the residency lookup misses. Own flag,
`HIPPIUS_PEER_FETCH_PENDING`, default off. Biggest remaining user-visible win; it directly retires
a documented staging failure.

**Blocked on a design decision — do not implement as originally specced.** The first draft said
"teach `chunks_exist_batch` that peer-resident counts as cache." That is unsafe as written.
`source` is decided **once**, up front ([object_reader.py:278-290](../../hippius_s3/services/object_reader.py)),
and a `cache` verdict means **no pipeline request is ever enqueued**. If the peer then evicts or
drains the part between the check and the read, the streamer blocks on a pub/sub notification that
will never arrive — bounded by `HIPPIUS_CACHE_TTL` = **3600 s** for non-first chunks
([config.py:218-221](../../hippius_s3/config.py)). A peer losing a part mid-stream would turn a
fast 503 into an hour-long hang: strictly worse than the failure this phase exists to fix.

Whatever ships must keep a live fallback — re-check on miss and enqueue the pipeline then, or bound
the peer-sourced wait far below `cache_ttl` and fail over. A feature flag does not cover this,
because the bad path only appears under a race that will not show up in a quiet soak. Design and
review this before writing code; it is the reason this phase sits at P2 rather than P1.

### Phase G (P1, re-ranked up from F6) — Stop the reader shedding its own prefetch

**Re-ranked 2026-08-07 after independent review.** Originally filed as P3 polish. It is the
dominant latency and Ceph-load term for large sequential reads, which is the workload this tier
exists for.

`HTTP_STREAM_PREFETCH_CHUNKS` is 16; `HIPPIUS_PEER_FETCH_MAX_INFLIGHT` is 8. Every chunk of one
part resolves to the same peer, so a part with more than 8 chunks has a single reader shed its own
excess to the pool and book it as `client_cap` contention — **contention that does not exist**.
The effect is to route half a large part's reads to Ceph while the peer sits idle, and to
under-report `chunk_reads_by_tier_total{tier=peer}` for the whole soak.

Two refinements the original filing missed:

- **Scope.** This bites at >8 chunks, i.e. parts over 32 MiB at the 4 MiB chunk size. It does
  **not** bite the fleet-average 487 KB single-chunk part — but MPU parts for large sequential
  uploads are exactly the >32 MiB shape, so it bites the workload that matters here and is
  invisible in fleet-wide averages.
- **It is worse than a per-reader cap.** `PeerChunkFetcher._inflight` is built once in the
  lifespan, so the 8 slots are **pod-wide per peer**, shared across every concurrent reader — not
  8 per reader. Under concurrent trainer load the cap binds far harder than the single-reader
  analysis suggests.

**Change.** Raise the client cap to at least the prefetch depth.

**Scoped down 2026-08-07 after adversarial review — the first draft overclaimed this fix.** Run the
aggregate arithmetic, which the first draft did not: `api-local` is a **DaemonSet** (5 pods), so a
peer can be addressed by 4 remote pods, each within its own per-peer cap.

| | client side (4 remote pods) | serve cap | ratio |
|---|---|---|---|
| today | 4 × 8 = 32 | 16 | 2× oversubscribed |
| after raising client cap to 16 | 4 × 16 = 64 | 16 | **4× oversubscribed** |

So under *concurrent* readers this change does not add peer capacity — it **converts `client_cap`
sheds into `server_busy` sheds**. Identical Ceph read load, different label. What it genuinely
fixes is the **single-reader** case: one pod, 16 chunks of one part, serve cap 16 accommodates it
exactly, and the reader stops shedding to Ceph against an idle peer. That is a real and common
case, and it is the only thing this phase should claim.

Two consequences for the plan:

- The gate and test must be written for the single-reader case specifically (they are), and the
  phase must **not** be sold as "peer tier stops leaking to Ceph."
- `HIPPIUS_PEER_SERVE_MAX_INFLIGHT = 16` is a guessed number. Size it from a measured figure —
  what concurrency the peer's NVMe and uvicorn sustain without hurting its own ingest — and state
  the **intended** steady-state pool read share, because Ceph is by design the overflow valve here
  and pretending it goes to zero will misread every soak.

A part-level bulk fetch (16 chunks → 1 request) is the better shape but is **not** a capacity fix
either: slot-seconds at the peer are roughly conserved (16 × ~7 ms ≈ 1 × ~90 ms for a 64 MiB part).
Its real wins are request overhead and removing the client-cap interaction entirely. Treat it as a
follow-up, not as the answer to oversubscription.

**Do not read the soak's tier split until this lands.** Until then, "the peer tier is not helping"
is unfalsifiable, because half the peer opportunity on large parts is mis-booked as pool.

### Phase H (P1 for the trainer workload — moved out of non-goals) — Don't FIFO-evict the hot set

**Moved 2026-08-07 after independent review.** The prior plan filed recency-aware eviction as a
non-goal on the grounds that node-local read recency does not exist. That reasoning is sound and
the conclusion was still wrong for this workload: eviction is FIFO on `resident_at`
([store.rs `ORDER BY r.resident_at`](../../crates/hippius-drain-core/src/store.rs)), so once the
disk is full enough for the tier to matter, a re-read working set is evicted in arrival order and
immediately re-promoted — paying a peer/pool read plus a local write per cycle. FIFO is materially
worse than LRU under skewed re-reads, and a training set re-read per epoch is maximally skewed.

**Cheapest concrete option, which the original non-goal reasoning overlooked:** `resident_at`
already *is* the ordering key, so a **sampled or batched bump of `resident_at` on a local hit**
converts FIFO into approximate LRU with no new table, no new column, and no fleet-wide-vs-node-local
ambiguity — the row is already node-scoped. The objection is a write on the read path; sampling
(bump at most once per part per N minutes, via the existing `PartMemo`) bounds that to roughly the
promotion write rate, which the path already pays.

Prove the thrash before building it: during the soak, correlate `drain_ssd_evicted_total` against
re-`promotion` of the same parts. If parts are being promoted, evicted, and re-promoted inside one
epoch, that is the signal.

### Phase F (P3) — Remaining tuning, one commit

`HIPPIUS_PEER_FETCH_TIMEOUT_SECONDS` 2.0 → 0.5; fire promotion via `create_task`; reconcile the
two residency `ON CONFLICT` semantics in a comment. (The peer-cap item moved to Phase G.)

---

## 3. Test plan

TDD throughout: the failing test lands in the same commit as, and before, the fix. Every test below
names the property it protects, not the function it calls.

### Phase 0 — rollback is a drill, not a unit test

No new code, so no unit tests. The verification is operational and must actually be run:

| Level | Where | Check |
|---|---|---|
| drill | staging | deploy retention → build occupancy → `kubectl rollout undo daemonset/drain-agent` → SSD occupancy returns to backlog-only within the grace. **Timed and recorded**, not asserted |
| drill | staging | during that rollback, no part loses its only durable copy — cross-check `cephor_replication_status` against on-disk parts before and after |
| review | both manifests | `CEPHOR_REPLICATED_RECLAIM_GRACE_SECS` present and commented as rollback-only; the "drained parts are unlinked" rationale corrected |

### Data-loss drill (A7) — required before any phase reaches prod

| Level | Where | Check |
|---|---|---|
| drill | staging | kill a node mid-drain with retention on; every part is either still `pending`/`draining` with its SSD copy intact, or `replicated` with a verified pool copy. **No part in neither state** |
| soak | `stress-test/inv/guards.py` | the above as a standing invariant, not a one-off |

### Phase G — prefetch/peer-cap alignment

| Level | File | Test |
|---|---|---|
| unit | `tests/unit/test_peer_fetch.py` | a single reader fetching `prefetch_depth` chunks of one part records **zero** `client_cap` sheds — the regression test, which fails on today's 16-vs-8 defaults |
| unit | same | the cap still sheds under genuine multi-reader contention (the Owl property must survive) |
| unit | new | **config invariant**: `peer_fetch_max_inflight >= http_stream_prefetch_chunks`, asserted against shipped defaults so a future prefetch bump cannot silently re-open this |

### Phase H — recency-aware eviction

| Level | File | Test |
|---|---|---|
| unit | `hippius_s3/cache/` | a local hit bumps `resident_at` at most once per part per sample window (bounds the read-path write) |
| integration | `#[sqlx::test]` | a part read recently sorts **behind** an older-read part in `evictable_parts`, i.e. the ordering actually became recency |
| integration | same | the durability invariant is untouched by the reordering — a non-replicated part is still never offered, whatever its recency |
| soak | `stress-test/inv/guards.py` | no part is promoted → evicted → re-promoted within one read epoch |

### Phase A — promotion backpressure

| Level | File | Test |
|---|---|---|
| unit | `tests/unit/test_promotion_pressure_guard.py` | a chunk read below the free-space floor is **served** but **not promoted**, and increments `promotion_skipped_total` |
| unit | same | above the floor, promotion still happens (no regression of the tier) |
| unit | same | the pressure read is memoized — N chunk reads in one window cause **one** `disk_usage` call |
| unit | same | **threshold ordering invariant**: `promote_min_free_ratio > evict_reserve > fs_cache_min_free_ratio`, asserted against the shipped defaults so a retune cannot invert it |
| unit | same | a `disk_usage` failure does not fail the read (degrades to *allow* promotion, matching the existing best-effort posture) |

### Phase B — eviction loop

| Level | File | Test |
|---|---|---|
| unit | `crates/hippius-drain-core/src/ssd_evict.rs` | **batch smaller than the candidate set still frees the full deficit** and reports `starved == false` — the F2a regression test |
| unit | same | genuine exhaustion (worklist shorter than the deficit needs) still reports `starved == true` |
| unit | same | the pass marks each page **before** re-querying (fake log asserts no page is offered twice) |
| unit | same | wall-clock budget exhaustion sets `budget_exhausted`, not `starved`, and leaves the remainder for the next pass |
| unit | same | the unreplicated-refusal invariant survives the loop — a non-replicated part on **any** page is never unlinked, `skipped_unreplicated` counts every one |
| property | same | for any (free, reserve, headroom, candidate set), `freed_bytes ≤ sum(candidate bytes)` and the pass terminates |
| integration | `crates/hippius-drain-core/src/store.rs` (`#[sqlx::test]`) | successive `evictable_parts` pages after `mark_evicted` return **disjoint** rows in FIFO order |

### Phase C2 — api records the landed part

| Level | File | Test |
|---|---|---|
| unit | `tests/unit/` | the landed-part write is ordered strictly **after** `meta.json` — the drain must never be able to claim an incomplete part |
| unit | same | a failed landed-part write does **not** fail the PUT, and leaves the reconciler backstop able to recover it |
| unit | same | the write is idempotent — a retried PUT does not disturb an existing row's status (an `ON CONFLICT DO NOTHING` that reset a `draining` row would be data loss) |
| unit | same | with the flag off, no row is written and the PUT path is byte-identical to today |
| e2e | `tests/e2e/` | with the flag on, `ReconcileReport.recovered` is 0 for parts the api recorded |
| e2e | same | **kill the api mid-part**: a part whose meta landed but whose row did not is still recovered by the reconciler within one poll — the property that lets the poll be lengthened |

### Phase C3 — reclaimer

| Level | File | Test |
|---|---|---|
| unit | `crates/hippius-drain-core/src/ssd_reclaim.rs` | DB-driven `failed` reclaim removes exactly the parts the walk-driven one did (behavioural equivalence, both paths over one fixture) |
| integration | `#[sqlx::test]` | the `failed` worklist query is node-scoped, grace-gated, and excludes servable parts (the corrupt-live guard must survive the rewrite) |
| integration | same | a `corrupt` part is never returned by the new query, at any age |
| bench-ish | new | scan duration and part count on a seeded tree of ≥ 200k parts, asserted under a ceiling; guards F1 from returning |

### Phase D — reserve

| Level | File | Test |
|---|---|---|
| unit | `crates/hippius-drain-core/src/alloc.rs` | a node with **small** backlog and a partial budget keeps a reserve near base — the F4 regression test, which today's code fails |
| unit | same | a node with backlog far exceeding `budget × horizon` reserves at max |
| unit | same | a caught-up node (zero backlog) never exceeds base — existing property, must survive |
| unit | same | the Ceph-ceiling term still dominates on a satisfied node (the existing NearFull test must still pass unchanged) |
| property | same | reserve is monotonic non-decreasing in backlog and non-increasing in budget |

### Phase E — peer-serve fresh parts

| Level | File | Test |
|---|---|---|
| unit | `tests/unit/test_peer_fetch.py` | a `pending` part resolves to its ingest node when the residency lookup misses |
| unit | same | with the flag off, behaviour is identical to today |
| unit | `tests/unit/test_dual_fs_store.py` | a peer-resident, pool-absent part reads as **cache**, not pipeline |
| unit | same | promotion of a peer-fetched **pending** part does **not** record residency (its only durable copy is elsewhere; claiming it would offer it to an evictor) — the durability invariant at the new boundary |
| e2e | `tests/e2e/` | cross-node GET of a freshly-written object succeeds instead of 503ing |

### Cross-cutting

- `stress-test/inv/guards.py` — add a `PromotionPressure` guard (promotion skips rise while free
  space falls, never the reverse) and an `EvictionKeepsUp` guard (`drain_ssd_free_bytes` never
  crosses the `fs_cache_pressure` threshold while `drain_ssd_evicted_bytes_total` is climbing).
- Mutation-test Phase B's invariant with `cargo-mutants` scoped to `ssd_evict.rs`. The prior plan
  found a real bug this way; do not skip it on the module whose failure mode is data loss.
- `ruff check` / `ruff format` / `ty check` and `cargo clippy --all-targets --all-features -D
  warnings` clean before every commit.

---

## 4. Rollout

**Task 0 — measure before sizing anything.** On staging and prod, per node: resident part count,
average part size, and current `scan_parts` duration. Phase C's sizing and the eviction-rate math
both depend on real numbers, and the 487 KB figure is derived, not measured. Read-only; no deploy.

Then, one phase per PR, each soaking before the next starts:

| Phase | Staging gate before proceeding |
|---|---|
| 0 | **`rollout undo` exercised on staging**: retention deployed, occupancy built, rolled back, occupancy returns to backlog-only within `CEPHOR_REPLICATED_RECLAIM_GRACE_SECS`. Var kept + commented as rollback-only; stale poll rationale corrected; the three §5-A11 numbers agreed |
| G | `peer_fetch_shed_total{reason=client_cap}` ≈ 0 for a single reader of a >32 MiB part; `chunk_reads_by_tier_total{tier=pool}` falls measurably on the same object. **This gate must clear before any tier-split number from an earlier soak is trusted** |
| H | evict/re-promote of the same part inside one read epoch is measured; if present, ship the sampled `resident_at` bump and show it falls |
| A | `promotion_skipped_total` is 0 while free space is healthy; forcing the disk down (a controlled fill) makes it rise **before** `fs_cache_shed` does |
| B | armed eviction reaches `reserve + headroom` within one pass; `starved` stays 0 across a full fill/drain cycle; `drain_ssd_evict_blocked_unreplicated_total` remains **0** |
| C2 | with the flag on, `ReconcileReport.recovered` ≈ 0 for ≥ 24 h **and** an induced api kill still recovers. Lengthening `CEPHOR_RECONCILE_POLL_SECS` is a **separate** deploy after that, not part of this gate |
| C3 | `drain_scan_duration_seconds` p99 well under the (new, hourly) orphan cadence; `failed`-part reclaim counts match the pre-change baseline |
| D | no node's budget moves; reserves track backlog rather than sitting pinned — verify against the live `cephor:alloc:*` Redis keys, which are ground truth (metrics lag) |
| E | design review of the miss-after-`cache`-verdict path signed off **before** implementation; then `chunk_reads_by_tier_total{tier=peer}` picks up fresh-object reads and the 503 "parts not ready" rate falls, with no rise in stream-timeout errors |

**Production promotion.** Corrected 2026-08-07: an earlier draft said "the Rust-side changes are
safe without the api flags." That was wrong — **retention is itself an unflagged Rust change**
(F0), so the first prod deploy carrying this binary changes production behaviour whatever the api
flags say. The rule is therefore:

1. **Phase 0 must be in the first prod-bound promotion**, so retention has a kill switch the
   moment it reaches prod.
2. Then ≥ 72 h staging soak of the phase set, including a peak-traffic window.
3. `HIPPIUS_OBJECT_CACHE_PROMOTE_ON_READ` and `HIPPIUS_PEER_FETCH_ENABLED` stay **off** in prod for
   that first deploy, so a regression has one candidate cause rather than six.

**Rollback.** Per phase: A → flip `HIPPIUS_OBJECT_CACHE_PROMOTE_ON_READ=false`. B → set
`CEPHOR_EVICT_MAX_PASS_SECS` low, or `CEPHOR_EVICT_RESERVE_PERMILLE=0` to stop eviction outright.
C2 → revert the api-side publish (the agent's consumer is inert without messages), and restore
`CEPHOR_RECONCILE_POLL_SECS` if it was already lengthened — which is why the poll change is a
separate deploy. C3 → revert the commit.
D → revert; the wire format is unchanged. E → flag off, but see the design caveat in §2: the flag
does not cover the race, so the fallback path must be right before it ships at all.

**Alerting** lives in the separate `hippius-otel` repo, not here (see PR #21/#22 precedent).
Required there, in order:

1. `drain_ssd_evict_blocked_unreplicated_total > 0` — page. The durability invariant. Needed
   **now**, before any further soak.
2. `drain_ssd_free_bytes` approaching the `fs_cache_pressure` threshold — page.
3. `starved` — **do not wire until Phase B lands**, or the first real firing is already
   discredited.
4. `drain_scan_duration_seconds` above a ceiling — warn, after Phase C.

---

## 5. Adversarial review of this plan (2026-08-07)

Findings against the plan itself. The severe ones are corrected inline above; the rest are recorded
here with their disposition, so a reviewer can see what was considered and rejected.

### Corrected inline

| # | Finding | Disposition |
|---|---|---|
| A1 | Phase 0 proposed deleting `CEPHOR_REPLICATED_RECLAIM_GRACE_SECS` from prod. That var is **load-bearing for the image-rollback path** — the pre-#398 binary uses it to sweep retained parts. Deleting it would have left a rolled-back fleet holding a full cache with nothing to reclaim it. | Rewritten. Keep and comment the var; drop the proposed flag entirely (image rollback is the mechanism, and a shim contradicts `CLAUDE.md`). |
| A2 | Phase A's 0.20 promote floor **equals** the evictor's target (150‰ + 50‰ = 0.20 free). Zero hysteresis; promotion enabled only in the instant after a pass. | Floor moved to 0.25; test now asserts a non-empty band, not just ordering. |
| A3 | Phase G was sold as stopping the peer tier leaking to Ceph. Aggregate arithmetic (5-pod DaemonSet: 4 × 16 = 64 vs serve cap 16) shows it converts `client_cap` sheds into `server_busy` sheds under concurrency. | Scoped to the single-reader case, which it genuinely fixes; serve-cap sizing and intended pool share called out as open. |
| A4 | C2 gave `cephor_replication_status` two writers across two services and two migration systems. | Reshaped to a Redis handoff — api announces, agent writes. Also removes a flag. |

### Accepted, folded into the phases

- **A5 — Phase D optimizes an unmeasured mechanism.** Nobody has shown a dynamic free-space floor
  prevents 503s better than the static one. **Gate Phase D on evidence from the Phase B soak**; if
  the static floor holds, drop it rather than tune it.
- **A6 — the plan had no load test, though the repo just gained one.** Every gate is written
  against production-shaped traffic the plan never said how to generate.
  [`stress-test/throughput.py`](../../stress-test/throughput.py) (added in PR #398) already does
  sized, concurrent upload + re-read, and [`stress-test/inv/guards.py`](../../stress-test/inv/guards.py)
  is the invariant harness. **Every soak gate in §4 must be expressed as a rung in that harness**,
  not as a hand-run observation.
- **A7 — no canary, and no data-loss drill.** The DaemonSet's `RollingUpdate` defaults to
  `maxUnavailable: 1`, so it does roll one node at a time — but nothing *pauses* between nodes.
  For a change whose worst case is losing a part's only durable copy, add (a) an explicit one-node
  canary with a hold and an observation window, and (b) a **kill-a-node-mid-drain drill** proving
  no part is lost with retention on. Neither exists today.
- **A8 — flag proliferation contradicts `CLAUDE.md`.** The first draft added five knobs. After A1
  and A4 the count is three: `HIPPIUS_PROMOTE_MIN_FREE_RATIO`, `CEPHOR_EVICT_MAX_PASS_SECS`, and
  `HIPPIUS_PEER_FETCH_PENDING`. The first two are tuning thresholds, not feature switches; the
  third is a genuine staged rollout of new behaviour. That is defensible; five was not.
- **A9 — Phase B's deficit is computed once** from a `statvfs` at pass start, then acted on for up
  to 10 s while ingest writes concurrently. **Re-probe free space per page**, or the pass over- or
  under-evicts by whatever landed during it.
- **A10 — Phase H overloads `resident_at`.** Bumping it on read means the column stops meaning
  "when it became resident," contradicting both the migration comment and the evictor's FIFO docs.
  Add `last_read_at` and order on `COALESCE(last_read_at, resident_at)` — one nullable column, no
  semantic overload, and the FIFO fallback stays explicit.
- **A11 — the gates are directional, not numeric.** "pool falls", "cache rises" are unfalsifiable.
  Before Phase 0 ships, fix three numbers as the acceptance bar: a maximum PUT 503 rate, a p99
  per-chunk read latency, and a maximum steady-state pool read share. Without them, a soak cannot
  fail.

### Considered and rejected

- **Rolling retention back with a flag instead of the image** (A1). Rejected: an env change needs a
  pod restart anyway, so it is no faster than `rollout undo`, while permanently keeping a second
  reachable behaviour to test.
- **Bulk part fetch as the answer to peer oversubscription** (A3). Rejected as a *capacity* fix —
  slot-seconds at the peer are roughly conserved. Kept as a follow-up for request overhead.
- **mtime pruning of the reconciler walk** — see §2 Phase C; rejected on durability grounds.

## 6. Non-goals

- No change to the budget water-fill in `distribute`. Phase D touches only the reserve derived
  from it.
- ~~No true LRU eviction.~~ **Withdrawn as a non-goal 2026-08-07** — promoted to Phase H. FIFO is
  fine as a v1 mechanism and wrong as a v1 *policy* for a re-read working set.
- No Owl-style tracker deciding what each node caches. Same reasoning as the prior plan: five
  nodes and a 5.4 TB working set do not justify that control plane. Phase H is a replacement
  policy change, not a placement one — the distinction is what keeps it small.
- No production manifest changes in any phase of this plan.
