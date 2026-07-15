# Drainer optimisations — tightening the SSD→Ceph replication gap

**Goal:** minimise the *replication sync delay* — the time between an object being uploaded and
being durably readable from the Ceph pool (`replicated`). Reviewed on the **staging** branch
(`crates/hippius-drain-{core,agent,allocator}`), the code currently under test.

---

## 1. How draining works today (the mechanism)

**Unit of work = a *part*.** The api part model stores each object as
`<object_id>/v<version>/part_<n>/` holding `chunk_<i>.bin` files + a `meta.json` marker. A "part" is
one S3 multipart part (or the single part of a simple PUT). The drain copies the **whole part tree**
path-preservingly from the node SSD (`/var/lib/hippius/local_object_cache`) to the CephFS pool
(`/var/lib/hippius/object_cache`).

**The pipeline (per part), from `core::partdrain::drain_part`:**

```
reconciler scan  →  record pending  →  drain worker claims  →  drain_part:
   persist each chunk (copy+fsync+rename)  [SEQUENTIAL within the part]
 → byte-verify each copy
 → persist meta.json  LAST   (reader's readiness gate on CephFS)
 → enqueue Arion upload      (needs object_versions.address)
 → mark_replicated (COMMIT)  (a WAL fsync on the ceph-backed Postgres)
 → unlink the SSD copy
```

**Two timer-driven workers** (`agent::runtime::run_periodic`, both jittered):

| Worker | Env knob | Default | Staging | Role |
|---|---|---|---|---|
| **Reconciler** | `CEPHOR_RECONCILE_POLL_SECS` | **60 s** | **60 s** | Walks the SSD cache; records each *complete* part (has `meta.json`) with no row as `pending`. **Sole drain trigger — there is no api NOTIFY.** |
| **Drain** | `CEPHOR_DRAIN_POLL_SECS` | 5 s | **1 s** | `drain_until_empty`: claims up to `drain_concurrency` parts and drains them. |

Claiming: `claim_part` is **node-scoped**, **oldest-`landed_at` first**, `FOR UPDATE SKIP LOCKED`,
honouring `deferred_until`. So the drain is **object-indiscriminate** — it drains the globally-oldest
pending parts across all objects, N at a time.

**Concurrency / rate (`core::enforce::Enforcer`):**

| Knob | Env | Default | Staging |
|---|---|---|---|
| Parts drained at once | `CEPHOR_DRAIN_CONCURRENCY` | **4** | 4 |
| Node write ceiling | `CEPHOR_MAX_DRAIN_RATE_BPS` | 100 MB/s | 100 MB/s |
| Floor rate (silent allocator) | `CEPHOR_FLOOR_RATE_BPS` | 1 MB/s | — |
| Deferred-part backoff | `CEPHOR_DEFER_BACKOFF_SECS` | 5 s | — |
| Allocation re-pull | `CEPHOR_ALLOCATION_POLL_SECS` | 2 s | — |

The allocator (singleton leader) hands each node an AIMD byte-budget; the agent's `Enforcer` is a
local token-bucket + circuit-breaker that admits parts up to that budget and `drain_concurrency`.

---

## 2. Answers to the geometry questions

**Does the copy worker care whether the file is big or small?** *Partly — it is NOT geometry-agnostic.*
- The unit is a **part**, so a big object is *many* parts and a small object is one. Parallelism is at
  the part level, capped at `drain_concurrency` (**4**). A 2 GB object at 64 MiB parts = 32 parts → 8
  serialized waves of 4. A 1 MiB object = 1 part.
- **Within** a part, chunks are copied **sequentially** (`persist_chunk` in a loop), so a part with many
  chunks drains chunk-by-chunk with a fsync each.
- The bandwidth gate charges `part_size`, so bigger parts consume more of the byte budget.

**Does the whole file need to land on SSD before draining starts?** *No for the copy — but two things
gate it in practice:*
- Each part becomes drainable **independently** the moment it is *complete* (its `meta.json` is
  written). Parts do **not** wait for sibling parts.
- **(a) The reconciler only *notices* a complete part on its next 60 s scan.** Until then the part has
  no `pending` row and nothing drains it.
- **(b) An MPU part cannot *commit* until `CompleteMultipartUpload`.** `object_versions.address` is
  written by the api at PUT / MPU-complete; the drain's Arion-enqueue step needs it, so before complete
  the part copies to Ceph but **defers** the commit (`upload context not ready … address is NULL`).
  Net effect: **MPU replication effectively starts only after CompleteMPU** + the next reconcile scan.

**Does a worker copy one file at a time, or indiscriminately?** *Indiscriminately* — `drain_concurrency`
(4) parts at once, globally oldest-`landed_at` first, regardless of which object they belong to.

---

## 3. The sync-delay budget (where the time goes)

For a just-uploaded object to reach `replicated`:

```
  reconciler scan wait      0 – 60 s   (avg ~30 s)   ← DOMINANT + jittery (sole trigger, 60 s timer)
+ drain_poll pickup         0 – 1 s
+ [MPU only] wait for CompleteMPU so address is set  (before the commit can happen)
+ copy time                 Σ parts (chunks × copy+fsync), 4-way parallel, ≤100 MB/s
+ per-part commit fsync      WAL flush on ceph-backed Postgres (the dominant per-part cost)
```

Measured on staging (EU host, 2 GB objects): once a part is *recorded*, per-part sync (`landed_at →
replicated`) is ~2–52 s and convergence after upload is tens of seconds — but the **0–60 s reconciler
wait is invisible to the `landed_at`-based metric** (landed_at is stamped when the reconciler records
the row, not when bytes hit SSD), so the *true* worst case is dominated by that scan interval.

---

## 4. Optimisations (ordered by impact on the sync gap)

### Tier 0 — the AIMD write-budget **deadlock** (the #1 real-world stall; already tuned)

Found live: the drain fully stalled with the allocator budget **pinned at the 1 MB/s `min_total`
floor** while the SSD backlog grew — objects never reached Ceph (sync delay → ∞).

**Mechanism** (`core::alloc::next_capacity`): the fleet write-budget backs off (×0.8/tick, floored at
`min_total`) whenever `observed_p99 > target_p99` — and **`target_p99` (2000 ms) is compared against a
whole `drain_part`'s copy+fsync+verify time**, which is *size-dependent*. Stability math (verified by
simulation): a 64 MiB part needs **≥ 40.7 MB/s/node** to stay under 2 s; below that, p99 exceeds target,
the budget decays, the per-part copy gets slower, p99 rises further — a **self-reinforcing collapse** to
the 1 MB/s floor. At 0.5 MB/s/node a 64 MiB part takes ~130 s, so it can never climb back. (256 MiB parts
need 162 MB/s/node — *unreachable*, so the absolute target is fundamentally wrong for large geometry.)

**Applied (this PR, staging):**
- `CEPHOR_ALLOC_MIN_TOTAL_BPS` **1 MB/s → 50 MB/s** — floor above the collapse threshold, so the AIMD can
  always recover. *Validated live: budget went 0.5 → 46 → 67 → 93 → 100 MB/s and the drain resumed.*
- `CEPHOR_ALLOC_TARGET_P99_MS` **2000 → 8000 ms** — headroom over the real per-part copy time under load.

**Still needed (root fix):** make the saturation signal **size-normalised** — compare per-part
*throughput* (bytes/s) against a target, or a rolling p99 baseline, not an absolute ms threshold. An
absolute ms target on a variable-size copy is inherently fragile (the code comment says as much).

### Tier 0b — the drain is **commit-bound**, not byte-bound

Even with a healthy 100 MB/s budget, the measured fleet rate was **~0.68 parts/s ≈ 44 MB/s** (2 nodes) —
the dominant cost is the per-part `mark_replicated` **WAL fsync on the ceph-backed Postgres**, not the
copy. So the byte-budget is not the real ceiling; the **commit** is. Levers: raise `CEPHOR_DRAIN_CONCURRENCY`
(this PR: 4 → 16) to overlap more commit fsyncs, **group-commit** the status writes, and — the real fix —
**move `cephor_replication_status` off ceph-backed storage onto fast NVMe** (commits drop from ~seconds to
~ms, lifting parts/s by 10–50×).

> **Prod note:** on prod the app DB (`postgres-nvme`) already runs on NVMe, so the commit fsync is ~ms and
> this bottleneck is **largely a staging-only artifact** (staging's `postgres` is ceph-backed). The
> concurrency bump still helps prod for large-object overlap, but the "move the status DB off ceph" lever
> is a staging concern — do not size prod around the ~0.68 parts/s figure, which is the ceph-backed number.

### Tier 1 — kill the 0–60 s trigger latency (biggest, cheapest win)

1. **Add a landing fast-path so the drain sees a part in ~1 s, not up to 60 s.**
   The api already writes `meta.json` last (the completeness marker). Have the **ingest/api path record
   the `pending` `cephor_replication_status` row** (or emit a Postgres `NOTIFY` / Redis signal) at that
   moment, and have the drain worker wake on it. The design deliberately has "no NOTIFY fast path"; adding
   one collapses the dominant, jittery **0–60 s** wait to the `drain_poll` (**~1 s**). Keep the 60 s
   reconciler as a **backstop** for any missed signal (crash between write and record). *This is the
   single highest-leverage change.*

2. **Interim, zero-code — APPLIED (this PR, staging): `CEPHOR_RECONCILE_POLL_SECS` 60 s → 15 s.**
   Cuts the dominant, jittery trigger tail from 0–60 s (~30 s avg) to 0–15 s (~7.5 s avg) — ~4×.
   Safe because a scan is a walk of the node-local SSD ingest tree (undrained parts only — drained
   parts are unlinked, and neither the Ceph pool nor the ~94M-row `parts` table is walked) plus ONE
   batched `UNNEST` status read; `record_landed` writes fire only for genuinely-new parts (a known
   part is seen as `already_pending` and skipped), so a faster poll adds **no** extra write load —
   just the cheap walk + one read, jittered per node. 15 s (not the 5–10 s first floated) is the
   deliberately-conservative first step on shared staging; it can go lower once **(3)** lands. Caveat:
   the walk is O(parts-on-SSD), so under a very large undrained backlog a sub-scan-interval walk gets
   costly — the reason to pair a further cut with the mtime-incremental scan below.

3. **Make the reconciler scan incremental / mtime-indexed** instead of a full-tree walk: only descend
   into part dirs whose `meta.json` mtime is newer than the last scan cursor. This decouples scan
   *frequency* from scan *cost*, making a fast reconcile poll cheap even with a huge cache.

### Tier 2 — make MPU geometry-agnostic (decouple Ceph-commit from the Arion enqueue)

4. **Commit the Ceph replication as soon as copy+verify+meta succeed, independent of the Arion enqueue.**
   The CephFS copy does **not** need `object_versions.address` — only the Arion backend upload + chain
   publish do. Today `drain_part` does `copy → verify → meta → enqueue(needs address) → commit`, so an
   MPU part is fully on Ceph yet stuck `draining` until CompleteMPU. Split the two: mark the part
   **`replicated` (Ceph-durable)** right after `meta` persists, and enqueue the backend upload
   separately (it can wait for the address without holding the Ceph status hostage). Effect: a 10 GB MPU's
   parts become **downloadable-from-Ceph as they upload**, incrementally, instead of all-at-once after
   CompleteMPU. This is the change that makes replication **upload-geometry-agnostic**.

### Tier 3 — parallelism & rate for large objects

5. **Raise `CEPHOR_DRAIN_CONCURRENCY`** 4 → **8–16**. A 32-part 2 GB object drops from 8 waves to 2–4.
   Bounded by the Enforcer byte-budget + Ceph write capacity, so raise alongside a rate check.
6. **Parallelise chunk copies *within* a part** (currently sequential). Speeds a single large part; keep
   the `verify → meta-last → commit` ordering (meta and commit stay last).
7. **Confirm `CEPHOR_MAX_DRAIN_RATE_BPS`** (100 MB/s/node) matches Ceph + network headroom; the AIMD
   allocator only ever tunes *down* under pressure, so a higher healthy ceiling is free upside. Observed
   upload was ~85 MB/s, so per-node drain should be allowed to match.
8. **Group-commit the per-part `mark_replicated`.** The commit is a WAL fsync on the ceph-backed Postgres
   and is the dominant per-part cost; concurrency overlaps them, but a batched/group commit (or moving
   the status DB off the Ceph-backed PG) cuts the floor further.

### Tier 4 — finer granularity (larger change)

9. **Drain at chunk granularity** so a huge part doesn't have to fully copy before any of it is readable
   and so chunks parallelise naturally. Bigger refactor — the *part* is the current readable/commit unit.

---

## 5. Recommended sequence

1. **Ship (1) landing fast-path** + keep the reconciler as backstop — collapses the 0–60 s tail to ~1 s.
2. **Ship (4) decouple Ceph-commit from the Arion enqueue** — makes MPUs replicate incrementally and
   removes the CompleteMPU stall.
3. **Bump (5) `CEPHOR_DRAIN_CONCURRENCY` to ~12 and (2/7) verify the rate/reconcile knobs.**
4. Follow with (3) incremental scan, (8) group-commit, then (6)/(9) if the gap still needs tightening.

(1)+(4) alone should take the *typical* upload→downloadable-from-Ceph delay from *tens of seconds
(0–60 s trigger + MPU-complete wait + copy)* down to **~copy time + ~1–2 s**, and make it independent of
whether the upload is a 1 MB PUT or a 40 GB MPU. The Tier-0 AIMD tuning (already applied) is the
prerequisite — without it the drain deadlocks and no other optimisation matters.

---

## 6. Sizing for 100 concurrent 2 GB writers (~200 GB burst, 3200 parts)

Two hard constraints surface at this scale — both measured on staging (2 ingest nodes):

1. **SSD headroom.** 100 × 2 GB = **200 GB** must land on the ingest SSD *before* it drains. Staging has
   **~166 GB free (81 % used)** on one node — a 200 GB burst would cross the 90 % `fs_cache_pressure`
   watermark and start **503-ing uploads**. The SSD must be sized for `peak concurrent bytes not yet
   drained`, i.e. `burst_size − (drain_rate × burst_duration)`. → **scale ingest nodes** so the burst
   spreads across enough SSD, and/or keep the drain fast enough that the resident set stays bounded.
2. **Drain throughput is commit-bound (~0.68 parts/s/fleet).** 3200 parts ÷ 0.68 = **~78 min** to clear —
   unacceptable. At this scale the per-part Postgres commit is the wall, so:
   - **Move `cephor_replication_status` off ceph-backed Postgres → fast NVMe** (the single biggest lever;
     10–50× parts/s).
   - **`CEPHOR_DRAIN_CONCURRENCY` 16 → higher** and **scale drain nodes** (throughput ≈ nodes ×
     concurrency × commit-rate).
   - **Group-commit** the `mark_replicated` writes.
   - Tier-1 **landing fast-path** so 3200 parts aren't each waiting up to 60 s for the reconciler.
3. **AIMD sizing for the aggregate:** raise `CEPHOR_ALLOC_MAX_TOTAL_BPS` (fleet ceiling) and
   `CEPHOR_MAX_DRAIN_RATE_BPS` (per-node) to the real Ceph write bandwidth, set `CEPHOR_ALLOC_MIN_TOTAL_BPS`
   to `n_nodes × safe-per-node`, and **enable the ceph-mgr probe** (`CEPHOR_CEPH_MGR_METRICS_URL`) so the
   AIMD respects actual Ceph capacity instead of a static ceiling.

**Rule of thumb:** to keep the *last* object of a 200 GB burst downloadable-from-Ceph within `T` seconds,
the fleet must sustain `200 GB / T`. For `T = 300 s` that's **~680 MB/s aggregate** — reachable only once
the commit bottleneck (Tier-0b) is removed; the byte-budget alone (already 100 MB/s/node) is not the limit.
