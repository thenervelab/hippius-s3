# Write-path decision: SSD-staging vs. inline-to-HCFS

**Date:** 2026-09-15 · **Status:** ✅ DECIDED — **SSD-staging** (see Decision below) · **Owner:** (tbd)
**Companions:** [`10a-option-arion-direct.md`](./10a-option-arion-direct.md) · [`10b-option-hcfs-handoff.md`](./10b-option-hcfs-handoff.md) · [`13-hcfs-as-is-integration.md`](./13-hcfs-as-is-integration.md) · [`../rust-rewrite-assessment.md`](../rust-rewrite-assessment.md)

---

## Decision (2026-09-15): SSD-staging, with explicit ack/consistency semantics

**We keep the SSD reservoirs, like the Python impl.** With HCFS as the sole backend, the local-SSD stage earns its place for two reasons the pure-inline path can't give:
- **Write-availability decoupling:** HCFS down ⇒ PUTs still land on SSD and drain when it recovers (inline would fail the PUT).
- **Burst absorption:** ingest rate is decoupled from HCFS/Arion throughput.

And the cost is now modest: because **HCFS owns Arion/S3 dual-write, retry, and chain reporting**, the forwarder is *not* the Python Arion uploader — it's a small daemon. The SSD-landing side reuses `drain-core`'s pure bits + `localfs.rs` (the meta.json/flock contract).

**Flow:**
```
client PUT → encrypt → land ciphertext chunks on local SSD + meta.json → 200 to client
                                    │ (per-node forwarder)
                    meta.json complete → POST chunks to HCFS → on 200: mark replicated + clean up SSD
                    (re-POST is idempotent: HCFS content-dedup + S3-side refcount)
```

**Ack & read-after-write semantics — the real substance of this decision:**
- **Default buckets:** **fast-ack on the durable local-SSD write.** A GET before drain completes is **served from local SSD** — a *minimal* serve-pending path, NOT the Python cache/hydrate/peer system. This needs **node-sticky routing** during the pending window (or accept brief inconsistency).
- **Object-lock / WORM buckets (audit/evidence — e.g. sn85):** **sync-to-HCFS-before-ack.** No durability window; a `200` means durable in HCFS.
- This **per-bucket durability policy** gives latency + resilience by default and strong durability where it's required.

**This commits us to:** a per-SSD-node ingest **DaemonSet** (S3 API + local NVMe + forwarder) alongside the scalable API tier; a `staged_blobs` state table ([`12-schema-design.md`](./12-schema-design.md)); post-confirm SSD cleanup; and node-sticky routing for reads of not-yet-drained objects.

*The option comparison below is retained for the record; the decision above is the resolution.*

## Context (for readers coming in cold)

We're building a **greenfield Rust reimplementation of hippius-s3**: a new branch, its own separate Postgres DB with an optimized schema, separate pods, full vanilla-S3 parity. Existing data will be re-encrypted/migrated in later (so there's **no in-place ciphertext-compat constraint**).

Decisions already taken:
- **Go "direct"** (own the ingest/data plane) rather than making the S3 service a thin front-end.
- **HCFS is the backend** — durable storage is HCFS (which already does Arion/S3 dual-write, retry, and usage→chain reporting). **Not** Ceph.
- **Drop the read cache and the janitor/hydrate system** — reads go straight to HCFS. The SSD tier, if kept, is write-staging only.

That leaves exactly **one open architectural fork: how a PUT is written and acknowledged.** This doc lays out the two paths so we can pick one. It reshapes the build size, the deployment topology, and the durability guarantee, so it's worth getting right before we commit the plan.

> Note on the current system: the drain **no longer copies to CephFS** ("there is no pool side any more"). Today the flow is SSD-land → drain (verify + hash + enqueue) → a separate **uploader** reads the SSD copy and POSTs to Arion. So "Option 1" below = keep that shape but **retarget the uploader from Arion to HCFS**; "Option 2" removes the SSD/drain/uploader entirely.

## Not a differentiator (same in both)

- **Read path:** fetch straight from HCFS with Range. No local cache either way.
- **HCFS-side prerequisites** (needed regardless, ~4–8 weeks hcfs-side): blob **refcounting** (or dedup/versioned deletes drop live bytes), a raw-blob Range-capable API decoupled from hcfs's FileRecord/Manifest, an **attributed** service credential (today there's only one global, unattributed admin bearer), and streamed large PUT (the current single-PUT buffers the whole object → OOM risk).
- **Validation:** the SSD→HCFS mapping is already half-live — hippius-s3's gateway posts one 4 MiB cipher chunk per `POST /upload` to hcfs in production today (one chunk ⇄ one hcfs `file_id`).

## The two paths

### Option 1 — SSD-staging + async uploader → HCFS ("direct")
```
client PUT → S3 API encrypts → write ciphertext chunks to local SSD + meta.json → 200 to client
                                                     │  (async, per-node daemon)
                                     drain: verify completeness + SHA-256 + enqueue + record residency
                                                     │
                                     uploader: read SSD → POST chunk to HCFS → confirm → clean up SSD
```

### Option 2 — Inline write → HCFS
```
client PUT → S3 API encrypts → stream ciphertext chunks straight to HCFS → HCFS confirms → 200 to client
                               (HCFS does Arion/S3 dual-write + retry underneath)
```

## Side-by-side

| Dimension | Option 1 — SSD-stage + async | Option 2 — inline to HCFS |
|---|---|---|
| **PUT latency** | Fast — acked after a local SSD write | Slower — acked after the HCFS round-trip (HCFS acks after its S3 write; Arion retried async) |
| **Durability on `200`** | ⚠️ **Weaker than S3** — a `200` means "on one node's SSD", not yet in HCFS/Arion. Node/disk loss in that window = data loss | ✅ **S3-grade** — a `200` means HCFS has durably accepted the bytes; no single-node window |
| **Burst absorption** | ✅ SSD buffers spikes; ingest rate decoupled from backend rate | ❌ Client is backpressured at HCFS/Arion speed |
| **Build size** | **Large** | **Small** |
| **Components to build** | SSD-landing writer + `meta.json`/flock contract; the drain daemon (verify/hash/enqueue/residency); the **uploader (XL** — a digest-fenced `uploading→replicated` state machine, node-scoped routing, ON-CONFLICT-revive, billing-abort); a work queue; post-confirm cleanup; node identity/config | Encrypt → forward to HCFS. Stateless. |
| **Reuse from existing crates** | `drain-core` pure types/algorithms, `localfs.rs` (meta.json/flock), `disk`/`readiness`/`supervisor`; **rewrite `store.rs`** (~4.7k SQL lines → new schema) | Little needed — crypto/protocol/metadata is the whole build |
| **Deployment topology** | Scalable API **Deployment** + a **DaemonSet on every SSD ingest node** + node-local NVMe + queue infra | Scalable API **Deployment** only |
| **Failure modes** | Node loss before replication (data-loss window); SSD-full/pressure handling; state-machine correctness (`part_digest` fold must be byte-identical) | PUT fails if HCFS is down; HCFS latency sits in the PUT path |
| **Complexity / risk** | High | Low |
| **Read path** | Cold from HCFS (range) | Cold from HCFS (range) — *identical* |

## What actually distinguishes them

- **Option 1 buys two things:** fast write-ack latency, and **burst absorption** (SSD as a shock-absorber decoupling ingest rate from backend rate). It costs a per-node DaemonSet, the XL uploader state machine, and a **durability window** — you ack the client before the data is durable anywhere but one node's local disk.
- **Option 2 gives up the fast-ack and the buffer** in exchange for a **much smaller build, a stateless/scalable topology, and S3-grade durability on `200`.**

Two points that weigh heavily:

1. **Availability coupling is roughly equal, just deferred.** Both ultimately depend on HCFS. Option 1 lets a PUT *succeed* during an HCFS blip by parking on SSD (and risks losing it if that node also dies); Option 2 fails the PUT outright. Option 1 trades a hard failure for a soft data-loss risk.
2. **sn85 is an audit/evidence store.** Acknowledging a write before it's durable off-node is the wrong property for a write-once audit trail. If that customer is a driver, Option 1's durability window is a real strike — or it forces us to *not* ack until replicated, which erases the latency benefit while keeping all the build cost.

## When each wins

- **Option 1** if fast/consistent write-ack under load **or** high-burst ingest absorption is a hard requirement, *and* ack-before-durable is acceptable (or engineered around), *and* we'll build + operate the DaemonSet + XL uploader.
- **Option 2** if we want the smallest correct build, S3-grade durability on `200`, and a stateless scalable service — accepting that PUT latency is the HCFS write time. (Since reads are already cold, synchronous durable writes are a consistent posture.)

## Recommendation

> **⚠️ SUPERSEDED by the ratified decision at the top of this doc (2026-09-15, owner call): Option 1 (SSD-staging).** The analysis below is retained for the record but no longer the recommendation. The SSD buffer is kept for write-availability decoupling and burst absorption; the durability-window and per-node-read consequences are accepted as design requirements (see the register's Resolution log). The one prior reservation still worth carrying forward: serve-pending + node-sticky routing reintroduces a *per-node* read dependency in the pre-drain window — the node6-cache SPOF shape — so keep drain lag low (`S3rForwardBacklogAge` SLO) and offer sync-before-ack readily.

**~~Lean Option 2 (inline), unless there's a concrete write-latency or burst-throughput SLA that demands the SSD buffer.~~** Rationale (historical): the read cache is already gone (so the main historical justification for the SSD tier is moot), the ingest DaemonSet + XL uploader is the bulk of the remaining build, and an audit/evidence customer wants durability-on-ack. Option 2 is the smaller, safer, more S3-faithful build.

Option 1 is the right call **if** there's a real requirement the HCFS round-trip can't meet. *(This is the path chosen.)*

## The deciding question

> **Is there a hard write-ack-latency or burst-ingest requirement that the synchronous HCFS→(S3/Arion) write can't meet — and is acknowledging a client `200` before the write is durable off-node acceptable?**
>
> - **Yes** to the latency/burst need **and** comfortable with the durability window → **Option 1**.
> - Otherwise → **Option 2**.

To make this quantitative we'd want a number: measured/expected **HCFS PUT latency** (chunk write, S3-ack path) vs. the **write-ack SLA** the S3 product must hit. If someone can supply those, the call becomes data-driven rather than a judgment.
