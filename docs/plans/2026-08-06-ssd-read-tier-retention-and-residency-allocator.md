# Restoring the drainer SSDs as a read tier

Status: Phases 1–4 implemented on `feat/ssd-read-tier-phase1-backlog-signal` (2026-08-06, not
pushed). **Staging only** — `k8s/production` is untouched and carries forward in a separate PR
after soak. Diagnosis is evidence-backed against prod.

**Implemented so far** — commits `019da038` (signal split), `c3bd1c34` (retention + evictor core),
`32e0feda` (evictor worker + residency reporting). 443 tests green, clippy clean at `-D warnings`.
The eviction invariant was mutation-tested and that exposed a real bug: residency and status are
independent axes, because `redrive_corrupt_parts` resets `corrupt → pending` without clearing
`resident_at`, so the worklist must filter on `status = 'replicated'` — selecting on residency
alone offered the evictor parts whose SSD copy was again the only durable one.

**Safety margin, verified.** The evictor's floor is 15% free (frees to 20%); the api's
`fs_cache_pressure` 503 gate fires at `HIPPIUS_FS_CACHE_MIN_FREE_RATIO=0.08` or 10 GiB free, on
the same mount. The evictor is comfortably the more aggressive of the two, which is the
invariant that keeps retention from turning into failed PUTs.

**Deploy note.** Phase 2 must go out as one unit — retention without the evictor means nothing
frees the ingest SSD. Migration 0016 is backfill-free and its indexes build near-empty.

**Per-peer fanout (added after review).** Owl (OSDI '22) selects a peer subject to per-peer
fanout and bandwidth constraints, and the failure that prevents is live here: a part that is
hot and resident on one node draws every other node's fetches onto that node's `api-local` pod,
the same uvicorn serving its own ingest. Both sides are capped — the client skips to the pool
rather than queueing (waiting behind a saturated peer would add its latency on top of the pool
read that follows), and the server sheds with 503, since five pods each within their own cap
still add up at the peer. `peer_fetch_shed_total{reason=client_cap|server_busy}` reports both.

Not adopted from Owl: a tracker that decides *what* each node caches. Owl moves 800 PB/day to
millions of processes; this is five nodes and a 5.4 TB working set, so a control plane of that
shape would be premature. The transplant is the mechanism, not the architecture. Value-aware
replication also needs node-local read recency, which does not exist yet — the shared
`fs_cache_inventory.last_access_at` is fleet-wide, so a read on one node looks hot on all of
them.

**What to watch during the staging soak.** `chunk_reads_by_tier_total{tier=...}` is the whole
point: `local` should climb as retention fills each node's shard, `peer` should take most of what
used to be `pool`, and `pool` should fall. Alongside it, `drain_ssd_cache_bytes` should rise and
plateau under the evictor's floor, `drain_ssd_evicted_total` should be non-zero only once a node
approaches that floor, and `drain_ssd_evicted_bytes_total` climbing while `cache_bytes` stays
pinned at the disk size means eviction is not keeping up.

## 1. Intended design vs. what shipped

**Intended.** The five ingest nodes each carry a 3.84 TB NVMe. A part lands there on PUT, the
drain replicates it to CephFS in the background, and the part *stays* on NVMe so GETs are served
from local flash. The allocator, which already sees both Ceph pressure and per-node disk
pressure, balances the two: it decides how hard to push Ceph and how much data each node holds
resident for reads.

**Shipped.** The NVMe is a pure write-staging buffer. It is emptied as fast as the drain can run,
and nothing ever puts data back. Draining a part *means* deleting it locally.

## 2. The four mechanisms that enforce "empty the SSD"

1. **The drain unlinks unconditionally on commit.** The last statement of the happy path in
   [`drain_part`](../../crates/hippius-drain-core/src/partdrain.rs) is
   `ssd.remove_part(part)` (partdrain.rs:544). Replication and deletion are one step.

2. **The reclaimer re-drives that unlink as a backstop.**
   [`ssd_reclaim`](../../crates/hippius-drain-core/src/ssd_reclaim.rs) treats a lingering
   `replicated` part as a *crash-orphan* and deletes it after `replicated_grace`
   (ssd_reclaim.rs:383-390); prod sets `CEPHOR_REPLICATED_RECLAIM_GRACE_SECS=3600`. So even if
   step 1 were patched, retained parts would be swept an hour later.

3. **The read path treats the pool as equivalent to flash.**
   [`DualFileSystemPartsStore.chunks_exist_batch`](../../hippius_s3/cache/dual_fs_store.py)
   counts a pool-only part as `source="cache"` (dual_fs_store.py:42-54). Correct for
   availability, but it means the loss of the fast tier is invisible — no metric, no repopulation
   trigger.

4. **Nothing can fill the fast tier.** `arion-downloader` mounts only the CephFS
   `object-cache-pvc`, so a cache-miss fill lands on the pool. Outside of the api's own PUT there
   is no code path in the repo that writes to a node-local SSD.

## 3. Evidence from prod (2026-08-06, context `hippius`, ns `hippius-s3-prod`)

| Signal | Measurement |
|---|---|
| Aggregate NVMe | 5 × 3.5 TB = **17.5 TB**, used **21–36 GB per node (1–2%)** |
| CephFS pool | **5.4 TB used / 20 TB** — the whole working set is ~⅓ of aggregate NVMe |
| CephFS read (api-local pod, node1) | 76 MB in **813 ms** ≈ 94 MB/s, ~40 ms/chunk |
| Local NVMe read (same pod) | 84 MB in **119 ms** ≈ 705 MB/s, ~6 ms/chunk |

**~7.5× throughput and ~7× per-chunk latency, paid on essentially every GET.** The 21–36 GB
residual is pure drain backlog — exactly what the code intends the disk to hold.

## 4. Blocker A — the allocator consumes raw disk occupancy as drain demand

[`disk.rs`](../../crates/hippius-drain-agent/src/disk.rs) derives both its outputs from one
`statvfs` and asserts the identity explicitly (disk.rs:44-57):

> `used_bytes` is the drain backlog: the SSD ingest cache holds exactly the undrained chunks
> (a drained chunk is unlinked), so its occupied space is the work waiting to drain.

That identity holds *only* because the drain unlinks on replicate, and retention is precisely
what destroys it.

**A true DB-sourced backlog already exists and is already computed every heartbeat tick.**
`Store::node_backlog_bytes` (store.rs:369-384) sums `parts.size_bytes` over this node's
`pending`/`draining` rows, and `record_drain_signals` calls it on every tick
(runtime.rs:230-233). It is tested (store.rs:1731) and correctly excludes terminal states and
peer nodes.

It is simply **not wired into the allocator.** `heartbeat_once` builds the observation from raw
occupancy instead (runtime.rs:280-286), with a deliberate comment:

> The allocator observation keeps SSD occupancy as its demand weight (a leak-inflated value is a
> conservative over-demand, not a safety issue; refining it is coordination work).

**That safety argument is true today and false under retention.** Today the inflation is the
A21/orphan leak — bounded, hundreds of GB, and erring toward over-draining, which is harmless.
Under retention the inflation becomes the entire read cache: terabytes, permanent, and growing by
design. A node holding 3 TB of warm cache would report backlog 3 TB and pressure 0.86,
`allocate()` would read a critically-behind node, grant it the `reservation_floor`, and drive Ceph
writes for work that does not exist — the 2026-07-24 pool-fill shape.

So the prerequisite is real, but **most of it is already built**: Phase 1 is mainly a wiring
change plus a redefinition of `pressure`, not new query infrastructure.

## 5. Blocker B — there is no read locality

`Service/api` selects `app: api-local` with `sessionAffinity: None` — round-robin across the
5-pod DaemonSet. A part exists on exactly the node that ingested it, so **retention alone yields
a ~20% hit rate**; 80% of GETs still fall through to CephFS.

The enabler already exists: `cephor_replication_status.node_id` persists past replication and the
shard is near-perfectly balanced —

| node | replicated parts |
|---|---|
| k8s-v3-node1 | 2,276,486 |
| k8s-v3-node4 | 2,263,696 |
| k8s-v3-node2 | 2,202,484 |
| k8s-v3-node3 | 2,181,783 |
| k8s-v3-node5 | 2,156,429 |

±3% spread. Per-node shard ≈ 5.4 TB / 5 ≈ **1.08 TB into a 3.5 TB disk (31% fill)**.

## 6. Plan

### Phase 1 — feed the allocator true backlog, and redefine pressure (prerequisite)

Retention is unsafe until the allocator can tell backlog from cache. Ship this alone, confirm the
budget delta matches prediction, then proceed.

1. **Wire the existing DB backlog into the allocator.** `record_drain_signals` already computes
   `Store::node_backlog_bytes` every tick but only records it to a gauge; return it and use it for
   `NodeObservation.backlog` in place of `usage.used_bytes` (runtime.rs:280-286). Keep the
   fail-safe: on a query error, reuse the last good value rather than reporting a zero backlog,
   which would read as "idle" and starve the node of budget.
2. **Add `cache_bytes` and `free_bytes`** to `NodeObservation` and the `NodeState` wire DTO
   (coordination.rs:166-196). `cache_bytes` is the retained-`replicated` byte sum — zero until
   Phase 2, so it is inert on arrival.
3. **Redefine pressure as ingest headroom.** `ingest_headroom = free_bytes + cache_bytes`; the
   allocator weight and `critical_pressure` gate read `backlog_bytes / ingest_headroom` rather
   than raw fullness. A disk 95% full of *evictable* cache has full headroom and must not earn a
   reservation floor. Keep raw fullness as a reported gauge for dashboards.

**Predicted impact — from how `distribute` actually consumes these signals** (alloc.rs:194-235),
which is narrower than "budgets scale with backlog":

- `demand = if backlog == 0 { 0 } else { max_drain_rate }` — backlog is only a **zero/nonzero
  predicate**, never a proportional term. So the backlog swap changes exactly one thing: a node
  that is genuinely caught up drops to zero demand instead of claiming its full drain rate on the
  strength of leaked bytes. A node with any real backlog is unaffected.
- `weight = pressure.bps().max(1)` and `reserved = pressure >= critical_pressure` — pressure is
  the water-fill weight and the reservation gate. This is the signal retention would have
  corrupted, and the one Phase 1 redefines.

At today's prod numbers the redefinition is close to a no-op: 26 GB backlog against ~3.3 TB free
gives ≈78 bps, against the ~100–200 bps raw fullness reports now — same order, so the five roughly
equal nodes keep a roughly equal split, and 78 bps is nowhere near the 9000 bps
`critical_pressure` gate, exactly as today.

**Gate.** Assert after deploy that (a) no node's budget moved *up*, (b) any node whose budget went
to zero has zero `pending`/`draining` rows, and (c) no node crossed into a reservation floor.
Verify against the live `cephor:alloc:*` Redis keys, which are the allocator's ground truth
(metrics lag). A budget increase, or a reservation on a caught-up node, means the wiring is wrong.

### Phase 2 — retain instead of delete, with an evictor that owns the space

- `drain_part` stops unlinking on the happy path. The part transitions `draining → replicated`
  and *stays*. The idempotent `AlreadyReplicated` fast-path unlink goes too.
- `ssd_reclaim`'s `Replicated` arm changes from "crash-orphan, delete after grace" to "cache
  resident, hand to the evictor". The `Failed` / `Corrupt` / orphan arms are untouched — those
  are genuine debris and their safety gates stay exactly as they are.
- **New local evictor** in the agent, LRU by last access, driven to the allocator's per-node
  `retain_bytes` target. Its absolute invariant mirrors the janitor's: **never evict a part whose
  row is not `replicated`.** An unreplicated part is the only durable copy.
- Ingest admission keeps a hard reserve: a PUT must never fail because cache filled the disk. The
  evictor frees synchronously ahead of `fs_cache_pressure`'s 90% threshold.

**Effect:** the ingesting node serves its own shard from flash. ~20% of GETs, at zero fill cost.

### Phase 3 — close the locality gap (both mechanisms, one phase)

Decided 2026-08-06: ship routing and promotion together. They compose — routing delivers the
first-read hit on the node that already holds the shard, promotion covers everything routing
misses (a re-ingested object, a node drained for maintenance, a shard rebalanced after a node
is added).

- **Ingest-affinity locality, resolved PER PART.** Not per request — measured on prod
  2026-08-06, only 48 of 2,214 sampled multi-part object versions (2%) have all their parts on
  one node, while 684 (31%) span all five. Each `UploadPart` of an MPU is handled by whichever
  `api-local` pod the round-robin `Service/api` picks, so `node_id` is a per-part fact and
  routing a whole GET to "the object's node" would leave most parts remote anyway.

  The shape that follows is a **peer-fetch tier** in the read path — local SSD → the node that
  owns *that part* → CephFS pool — rather than a routing hop at the gateway. Same DB lookup and
  same api→api transfer, at the granularity the data demands. It composes with promotion (a
  peer-fetched chunk is promoted locally) and needs no gateway or Service change. Peer NVMe
  (~6 ms + ~1 ms network) still beats the pool's ~40 ms by a wide margin.

  Requires per-node addressing (`hostPort` on the `api-local` DaemonSet plus a node→IP map) and
  an internal read-only endpoint serving a part from local SSD, behind the existing IP-whitelist
  middleware. Ships behind a flag defaulting to off, falling through to the pool whenever the
  peer is unknown, unready, or slow. A peer fetch must never be able to fail a read.
- **Read-through promotion.** When an api pod reads a chunk from the CephFS fallback, it copies
  it to its own local NVMe. No routing dependency; warms on the second read on any node. Hot
  objects replicate up to 5×, which the 3× capacity headroom absorbs.

Because both land at once, attribution needs to be built in up front: the `read_source` metric
(§8) carries a `routed | fallthrough` dimension so a routing regression and a promotion
regression are separable in the dashboards without bisecting a deploy.

### Phase 4 — allocator drives the eviction reserve

`allocate()` now returns a per-node `reserve_permille` alongside the budget, carried in the same
fenced write and applied by the agent's evictor in place of its static floor.

**The sign is the opposite of what this section originally said.** The earlier text —
"high Ceph pressure → hold more resident" — conflates Ceph *read* load with Ceph *write*
pressure. Disk = backlog + cache + free. When the pool is degraded the drain writes nothing,
backlog grows at the full ingest rate, and holding *more* cache consumes exactly the space that
incoming backlog needs, bringing the `fs_cache_pressure` 503s forward. The correct law is:
**drain throttled → raise the free-space floor now, buying ingest runway.**

The reserve interpolates between `base_reserve_permille` (150) and `max_reserve_permille` (400)
on the worse of two severities, because either alone starves the drain:

- the fleet's Ceph ceiling (`Open` 0 / `NearFull` 500 / `Critical` 1000 permille);
- the node's own shortfall, `(demand - budget) / demand` — the water-fill can leave one node
  short while its peers are satisfied, and that node is the one accumulating.

A caught-up node has no demand and therefore no shortfall, so it never evicts past the base.
The agent falls back to its configured floor whenever no reserve is published (a pre-Phase-4
leader, an expired allocation key, a malformed value), and the allocation's TTL governs both —
a floor never outlives the budget it arrived with.

## 7. Decisions

- **2026-08-06 — Phase 3 ships routing and promotion together** (see §6). The tradeoff accepted:
  the evictor's first real-load exposure coincides with a request-routing change, so the
  `routed | fallthrough` metric dimension and the routing feature flag are load-bearing for
  attribution and rollback, not optional polish.

## 8. Observability required before any of this ships

- `read_source` split into `ssd | pool | pipeline` — today the pool and the SSD are both `cache`,
  so the tier loss is invisible and the fix would be unmeasurable.
- Per-node `drain_backlog_bytes` vs `drain_cache_bytes` vs `drain_free_bytes`.
- Evictor: bytes freed, parts evicted, and a **`evict_blocked_unreplicated` counter that must
  stay at zero** — the durability invariant.
