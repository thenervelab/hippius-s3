# 17 — Workers & Background Tasks (greenfield Rust `hippius-s3`)

**Status:** first draft for review. Every ⚑ marks a decision or risk that needs input.
**Scope:** the background-task / worker fleet for the **new** Rust S3 product only.
**Reading map:** [`04-queues-and-workers.md`](./04-queues-and-workers.md) (the Python fleet + Redis queues we are replacing), [`10-write-path-decision.md`](./10-write-path-decision.md) (SSD-staging chosen; forwarder + cleanup), [`12-schema-design.md`](./12-schema-design.md) (`staged_blobs`, `blobs.refcount`, the §6 "async worker" list), [`02-storage-engine-schema.md`](./02-storage-engine-schema.md) / [`03-data-plane-cache-streaming.md`](./03-data-plane-cache-streaming.md) (why the read-cache/janitor plane is gone), [`13-hcfs-as-is-integration.md`](./13-hcfs-as-is-integration.md) (delete-not-idempotent; **refcount owned S3-side**).

---

## 0. Design frame

The new product is an S3-compatible front end that **stages ciphertext to node-local SSD, acks fast, and forwards to HCFS** (decision in [`10`](./10-write-path-decision.md)). HCFS is the **unchanged** backend and owns everything past its own front door:

- **Arion/S3 dual-write** (`store_buffered_to_both`, `hcfs backend.rs:1206`),
- the **failed-write retry** loop (`hcfs-retry-worker`, S3→Arion backfill),
- **usage→chain reporting** (`hcfs-chain-reporter`; a non-exempt tenant SS58's S3 bytes are reported automatically, [`13`](./13-hcfs-as-is-integration.md) §4).

So we do **not** rebuild the Python Arion uploader, the unpinner-as-Arion-deleter, the FS read-cache/janitor/hydrate plane, a chain reporter, or a pool allocator. What is left for us is the **edge write-path movers** and the **metadata-reclamation janitors** — small, because HCFS absorbs the hard storage work.

Four principles shape the fleet:

1. **Postgres queues, not Redis.** The Python system runs a 5-instance Redis fleet with upload/unpin lists, retry ZSETs, per-backend DLQs, and a Lua retry-mover ([`04`](./04-queues-and-workers.md) §1–§2). The greenfield product is a **separate DB with no migration-compat constraint** ([`12`](./12-schema-design.md) intro), so it drops that subsystem entirely and drains work with `SELECT … FOR UPDATE SKIP LOCKED`. This is already the shape the Python **purger** uses (`purge_jobs` claimed `SKIP LOCKED` + lease, [`04`](./04-queues-and-workers.md) §3.5) and the **usage_rollup** uses (`DELETE … RETURNING` exactly-once, [`04`](./04-queues-and-workers.md) §3.6) — we generalize it. ⚑ This assumes **no mixed Python/Rust fleet** shares a queue during cutover; doc 04's byte-for-byte Redis wire contract applies only to a like-for-like port, which the greenfield rewrite is not (see Open Questions).
2. **Scan-as-queue by default; a real queue only where retries need state.** Doc 12 §6 is explicit: "blob GC, MPU reaping, version reaping, lifecycle … [are] an **async worker driven by the indexes in §5.1** — not a trigger." Most janitors therefore claim domain rows **directly** off a partial index (`idx_blobs_gc WHERE refcount=0`, `idx_mpu_initiated`, `idx_ov_locked`, `idx_objects_deleted`) — no separate queue table. The **one** durable work queue is `staged_blobs` (it already carries `state`/`attempts`/`claimed_at`/`last_error`, [`12`](./12-schema-design.md) §7).
3. **Two topologies.** A worker either (a) touches node-local SSD → must run **per-node** (the ingest DaemonSet), or (b) touches only shared Postgres + HCFS → a **singleton** made safely concurrent by `SKIP LOCKED`. No leader-election infra; a singleton that must not double-run takes a Postgres row lock, exactly as `hcfs-chain-reporter` does (`chain_report_cursor` id=1 `FOR UPDATE`).
4. **Mirror the house worker shape.** One binary per worker; env-only config (refuse to start on a missing secret); `tracing` + OTel; sqlx pool; `tokio::select!` on a `CancellationToken`; bounded shutdown drain that fits `terminationGracePeriodSeconds`; store and await every `JoinHandle`. This is both the HCFS rule set and the Python `run_worker` contract ([`04`](./04-queues-and-workers.md) §5.2). No semaphores / rate caps in the data path.

---

## 1. Definitive worker inventory

| # | Worker | Topology | Trigger / cadence | Claim design | Core idempotency guarantee |
|---|--------|----------|-------------------|--------------|----------------------------|
| **W1** | **Ingest forwarder** (SSD → HCFS) — part of the ingest DaemonSet | **Per-node** | `NOTIFY` on stage + short poll backstop | `staged_blobs` queue, `node_id`-scoped, `SKIP LOCKED` + `claimed_at` lease | Re-POST is safe: HCFS content-dedups by `blake3(ciphertext)`; unique hcfs name ⇒ 1:1 record↔blob; `blobs.replication_state` guard |
| **W2** | **Post-confirm SSD cleanup** | **Per-node** | Post-confirm event + periodic sweep (~60 s) | Scan `staged_blobs WHERE node_id=me AND state='uploaded'` (+ stale-`.tmp` scan) | `unlink` of a gone path is a no-op; row deleted only after unlink |
| **W3** | **Staged reconciler / crash recovery** | **Per-node** | On boot, then periodic (~5 min) | Scan node's `staged_blobs` + lease-expiry reaper | Re-enqueue only (W1 idempotent); `.tmp` GC safe |
| **W4** | **MPU reaper** (abandoned multipart) | Singleton (SKIP-LOCKED-safe) | Periodic (~120 s) | `SELECT … FROM multipart_uploads WHERE initiated_at < now()-ttl FOR UPDATE SKIP LOCKED` (`idx_mpu_initiated`) | `DELETE FROM multipart_uploads` of a gone upload is a no-op; cascade decref is trigger-driven |
| **W5** | **Lifecycle + version reaper** | Singleton (SKIP-LOCKED-safe) | Periodic (hourly lifecycle; ~min version-reap) | Scan `buckets.lifecycle` targets + `idx_objects_deleted` / soft-deleted `object_versions` | Expiry writes are convergent (already-expired = no-op); hard-reap decref is trigger-driven |
| **W6** | **Blob-refcount GC** (calls HCFS delete at refcount 0) | Singleton, **single-flight per blob** | `NOTIFY` on refcount→0 + periodic sweep | `SELECT content_hash FROM blobs WHERE refcount=0 AND gc_not_before<now() FOR UPDATE SKIP LOCKED` (`idx_blobs_gc`) | State `durable→deleting→deleted`; HCFS `404`/`already_deleted` treated as success; re-check `refcount=0` under lock |
| **W7** | **Usage reconciler** | Singleton | Periodic (~300 s) | Recompute a rolling `bucket_usage` slice vs the O(objects) oracle | Idempotent recompute-and-set; report drift (expected 0) |
| **W8** | **Consistency / orphan checker** *(optional)* | Singleton | Low (~daily) | Three-way audit: Postgres ↔ HCFS ↔ SSD | Report-only by default; repairs route through W1/W2/W6 |

W1–W3 are the **write path + its recovery** (ingest DaemonSet). W4–W8 are **shared-state janitors** (ordinary singleton Deployments, `Recreate` strategy like `hcfs-retry-worker`).

---

## 2. Per-worker design

### W1 — Ingest forwarder *(SSD → HCFS)* — replaces the Python `arion-uploader`

**What.** Drains ciphertext blobs a PUT/UploadPart handler durably staged to this node's SSD and forwards each to HCFS. This is the Python uploader **retargeted Arion→HCFS and shrunk**: because HCFS owns the Arion/S3 dual-write + retry, the forwarder is no longer the XL digest-fenced state machine — it is a small daemon that POSTs a blob and records the outcome ([`10`](./10-write-path-decision.md) Decision).

**HCFS call** (zero-change integration, [`13`](./13-hcfs-as-is-integration.md) §1, §"Store"): `POST {HCFS}/upload`, `Authorization: Bearer <admin>`, first multipart field `account_ss58 = <tenant_ss58>`, then `file` with `filename = <unique blob name>` and a **mandatory** `Content-Length` equal to the ciphertext bytes. Keep blobs **≤ ~4 MiB** (HCFS caps the file part at 16 MiB, and `DefaultBodyLimit(16 MiB)` covers the whole request; ~4 MiB + 28 B AEAD is the live size). On `200`, persist `hcfs_file_id` (= `hex(blake3(name))`, returned as `Success.file_id`).

**Trigger / cadence.** The handler stages the row and `NOTIFY`s; W1 `LISTEN`s and also polls (~1 s) as a NOTIFY-loss backstop. Default (fast-ack) buckets ack the client on the SSD write and let W1 forward asynchronously. **WORM / object-lock buckets forward inline in the PUT and ack only on HCFS `200`** ([`10`](./10-write-path-decision.md) Decision) — that path does not go through W1's async queue.

**Topology.** **Per-node DaemonSet.** The bytes exist only on the node that staged them, so rows carry `node_id` and each node's forwarder filters `WHERE node_id = $me` (`staged_blobs` is keyed `(content_hash, node_id)`).

**Idempotency.** The forward is content-addressed end to end: `blobs.content_hash = blake3(ciphertext)` and HCFS's own storage key is `blake3(ciphertext)` (`hcfs backend.rs:988`). A crash after HCFS accepts but before we commit `replication_state='durable'` just re-POSTs — HCFS dedups, and the **unique-name-per-unique-blob** rule ([`13`](./13-hcfs-as-is-integration.md) §6) keeps it 1:1 (one hcfs record ⇄ one blob), so a re-POST never double-stores and never crosses blobs. ⚑ The Python **`part_digest` hand-off fence** ([`04`](./04-queues-and-workers.md) §3.1) is largely unnecessary here: a client UploadPart-retry that rewrites bytes produces a **different `content_hash`** → a different blob row, so stale bytes can't be silently forwarded under the old identity. Keep a completeness check (all chunks of a part present) but the digest CAS can likely go.

**Queue design.** `staged_blobs` **is** the queue (it already has `state ∈ {landed,uploading,uploaded,failed}`, `claimed_at`, `attempts`, `last_error`, and `idx_staged_blobs_work (state, landed_at) WHERE state IN ('landed','failed')`, [`12`](./12-schema-design.md) §7). Claim = §3 pattern with `node_id=$me`. On success: `blobs.replication_state='durable'`, `staged_blobs.state='uploaded'`. On transient failure: back off (`attempts++`, `next_attempt`), on `attempts >= max`: `state='failed'` (the DLQ-equivalent — alert; a blob we can't forward is a stuck user object, never a discard).

> ⚑ No throughput cap (project rule). Bound each forward with a per-transfer deadline (mirror `hcfs-retry-worker`'s `TRANSFER_DEADLINE`) so a hung HCFS call becomes a retryable attempt, not a wedged worker. `/healthz` flips to 503 on stalled progress.

### W2 — Post-confirm SSD cleanup — the shrunk `janitor` (SSD half only)

**What.** Frees SSD after a blob is durable in HCFS: deletes `staged_blobs.ssd_path` once `state='uploaded'` (or once the row is confirmed and slated for deletion), then removes the `staged_blobs` row. Also the janitor for stale `.tmp` staging files (see §4). This is the only surviving piece of the Python `janitor` — the FS **read-cache** GC, hydrate, `fs_cache:pressure`, and the queue-depth sampler are all **dropped** (no read cache; Postgres exposes queue depth via SQL/metrics).

**Trigger / cadence.** Event from W1 on confirm + periodic sweep (~60 s) for a short grace window (so an in-flight serve-from-SSD read on a not-yet-drained default-bucket object isn't yanked, [`10`](./10-write-path-decision.md) read semantics).

**Topology.** **Per-node.** Idempotent: `unlink` of a gone path is success; the row is removed only after the unlink.

**Absolute-durability gate (carried from Python janitor invariant, [`04`](./04-queues-and-workers.md) §3.3):** never delete an SSD copy whose blob is not yet `replication_state='durable'`, under any disk pressure. Pressure with no durable copy → log ERROR and do nothing (page, never lose the only copy).

### W3 — Staged reconciler / crash recovery — new (absorbs the drain's recovery role)

**What.** (a) On boot and periodically, re-drive this node's `staged_blobs` rows stuck in `landed`/`uploading`; (b) the **lease-expiry reaper** — reset rows whose `claimed_at` is older than the visibility timeout back to `landed` (crashed-mid-forward); (c) reconcile SSD residue vs rows (orphan `.tmp`/files with no row → W2; a row whose `ssd_path` is missing → mark `failed`, alert).

**Trigger / cadence.** On process start (crash recovery), then periodic (~5 min). **Per-node** — only this node sees its own SSD. Everything it does is a re-enqueue or a lease reset, both safe because W1 is idempotent.

### W4 — MPU reaper *(abandoned multipart)* — kept from Python `mpu_reaper`

**What.** S3 multipart lives entirely in **our** edge product — HCFS exposes no S3 MPU (its "s3-gateway" is single-shot `multipart/form-data`, ≤16 MiB; [`13`](./13-hcfs-as-is-integration.md) §"What the S3 service must own"). This worker aborts uploads idle past a TTL: `DELETE FROM multipart_uploads WHERE initiated_at < now()-ttl`, which **cascades** `upload_parts → upload_part_chunks` and **decrefs `blobs` via the §6 trigger** ([`12`](./12-schema-design.md) §2.13). Decrementing a staged blob to `refcount=0` hands it to W6.

**Cadence.** Periodic (~120 s; Python default). TTL per-bucket-rule with a global default. **Singleton**, `SKIP LOCKED`-safe. Claims via `idx_mpu_initiated`.

**Idempotency.** `DELETE` of a gone/aborted upload is a no-op. Doc 12 already **killed the abandoned-MPU-resurrects-a-deleted-key wart by construction** (MPU initiate never touches `objects`, [`12`](./12-schema-design.md) W3), so the reaper is pure staging cleanup — never a metadata mutation. ⚑ Give its DB pool both a client `command_timeout` and a server `statement_timeout`: the Python mpu-reaper once left a wedged statement pinning the xmin horizon for 7,377 s and blocking VACUUM fleet-wide ([`04`](./04-queues-and-workers.md) §3.4).

### W5 — Lifecycle + version reaper — Python `lifecycle` + `janitor`'s soft-delete hard-reap

**What.** Two passes of one singleton:
- **Lifecycle expiration:** evaluate `buckets.lifecycle` rules ([`12`](./12-schema-design.md) §2.4 — persisted as first-class JSON, unlike Python which acks-and-discards) and expire due object-versions per S3 semantics (add a delete marker on a versioned bucket; soft-delete otherwise). **Never expire an object-lock/legal-hold version** (`idx_ov_locked`, the delete-guard predicate, [`12`](./12-schema-design.md) §2.6/§2.12).
- **Version/metadata reaper:** hard-delete soft-deleted `object_versions`/`objects` past a grace window (`idx_objects_deleted`), cascading `parts → chunks → chunk_blobs`, which **decrefs `blobs`** via the §6 trigger and feeds W6.

**Cadence.** Lifecycle hourly (expiry is day-granular); version-reap on a shorter tick. **Singleton**, `SKIP LOCKED`-safe. It never calls HCFS — it only mutates our metadata and lets refcount decrements flow to W6.

**Idempotency.** Expiry writes are convergent (an already-expired version is a no-op); hard-reap is a plain cascading `DELETE` guarded by grace + the object-lock predicate.

### W6 — Blob-refcount GC *(the one that calls HCFS delete)* — replaces the Python `unpinner`

**What.** We own the blob refcount — **HCFS has none** ([`13`](./13-hcfs-as-is-integration.md) §6: `StorageBackend::delete` is unconditional, `hcfs backend.rs:774`; storage key is `blake3(ciphertext)` shared across identical-content rows; an orphaned blob is tolerated). Many object-versions/chunks can reference one content blob; `blobs.refcount` is maintained by trigger from `chunk_blobs` + `upload_part_chunks` ([`12`](./12-schema-design.md) §2.9, §6). When a blob reaches `refcount=0` (plus a grace window absorbing an immediate re-PUT of identical content), W6 deletes it from HCFS and drops the `blobs` row. This is doc 12 §6's "blob GC" and the reason the whole product owns refcounting.

**HCFS call.** `DELETE {HCFS}/delete/{tenant_ss58}/{hcfs_file_id}`, admin bearer.

**Idempotency — the sharp part.** ⚑ HCFS single delete is **effect-idempotent but status-non-idempotent**: first call `200`, repeat `404 file_not_found` (`hcfs file.rs:1102`, [`13`](./13-hcfs-as-is-integration.md) §3). So W6:
- runs a blob state machine `durable → deleting → deleted` (`if_version`-guarded), setting `deleting` **before** the HCFS call so a crash mid-delete resumes and re-issues safely;
- **treats HCFS `404` (and batch `already_deleted`) as success** — a repeat after crash-retry is expected, not an error (a naive "404 = retry forever" would make every GC a permanent DLQ resident);
- **re-checks `refcount=0` under the row lock** immediately before deleting — if a concurrent PUT re-referenced the content during the grace window, cancel and return the blob to `durable`;
- the **unique-name-per-blob** rule ([`13`](./13-hcfs-as-is-integration.md) §6) guarantees the HCFS delete removes exactly this blob's bytes and can never orphan a live reference (the failure mode that would exist if two names pointed at identical ciphertext).

⚑ **Prefer the single `DELETE`, not batch `POST /delete_files`.** The batch form is idempotent-per-item (`already_deleted`) but is `ExemptRoute::Denied` and **drive-scoped** ([`13`](./13-hcfs-as-is-integration.md) §3 caveat): an exempt tenant SS58 gets `403`. Because our GC is content/blob-oriented (not folder-scoped) and tenant exemption varies, the single delete + "404-is-success" is the robust default; batch only where we can guarantee a non-exempt, single-drive group.

**Claim design.** Scan-as-queue on `idx_blobs_gc (zero_refcount_at) WHERE refcount=0` with `FOR UPDATE SKIP LOCKED` for single-flight per blob. The grace gate is `zero_refcount_at` (doc 12 §2.9, set on the 1→0 transition, cleared on any 0→1 re-reference — the resurrection-race guard); `gc_not_before` here is just the derived cutoff `zero_refcount_at + grace`. Add `gc_attempts`/`last_error` for backoff (§3). `NOTIFY` on the refcount→0 transition + periodic sweep backstop.

> ⚑ **The Python purger's Redis high-water backpressure gate is dropped.** Its whole reason was that a mass purge once queued 1.29M unpin requests into the `noeviction` redis-queues instance and broke prod GETs ([`04`](./04-queues-and-workers.md) §3.5). With deletes expressed as Postgres refcount decrements + a scan, there is no shared `noeviction` store to flood — the failure mode is gone, so the gate is unnecessary. A bulk account/bucket purge (the purger's other job) becomes: soft-delete metadata → W5 hard-reaps → refcount decrements → W6. If a dedicated purge worker is still wanted for large deletes, it claims `purge_jobs` with `SKIP LOCKED` + lease exactly as Python already does.

### W7 — Usage reconciler — the shrunk `usage_rollup` (+ `account_cacher`/`plans_cacher` context)

**What.** Keeps `bucket_usage` honest. Doc 12 §2.14 deliberately **collapses Python's four-table apparatus** (`storage_delta_ledger` + `bucket_storage_usage` + `storage_usage_rollup_state` + `bucket_storage_verify_state`) into one trigger-maintained counter + a periodic reconciler against the O(objects) oracle. W7 is that reconciler: recompute a rolling bucket slice, correct drift, report (expected zero).

**Cadence.** ~300 s (Python RECONCILE interval). **Singleton**, small pool, runs on the primary. Idempotent recompute-and-set; a skipped cycle is lossless (the counter is trigger-maintained; reconcile is a safety net). ⚑ If per-bucket write contention on the counter row shows up, reintroduce the append-only ledger + `DELETE … RETURNING` compactor pattern ([`12`](./12-schema-design.md) §2.14 flag; [`04`](./04-queues-and-workers.md) §3.6) — flagged, not built.

**On `account_cacher`/`plans_cacher`:** these are **billing-plane** cachers (substrate credit rows + plan roll into redis-accounts, [`04`](./04-queues-and-workers.md) §3.8–§3.9), not storage janitors. If we let HCFS's write-gate + `/can_upload` price and gate tenants ([`13`](./13-hcfs-as-is-integration.md) §5), the new product needs **neither** — dropped from this plane. If we own billing (doc 05), they belong there, not here. Out of scope for this doc beyond the mapping in §3.

### W8 — Consistency / orphan checker *(optional)* — kept from Python `orphan_checker`

**What.** Three-way audit: our `blobs` ↔ HCFS existence ↔ SSD residue. Flags (a) blobs marked `durable` HCFS can't produce, (b) HCFS objects with no live reference (a lost delete), (c) SSD files with no `staged_blobs` row. The Python orphan-checker enqueues an unpin for on-chain files absent locally; our analogue enqueues a W6 delete for (b) and routes (c) to W2. **Report-only by default.**

**Cadence.** Low (~daily). **Singleton.** ⚑ Ship last, opt-in: it reads HCFS at scale and can self-DoS the backend. Bound it by cursor pagination + cadence, never a rate limiter in the data path. Class (c) needs per-node SSD visibility, so either fan out to node agents or scope W8 to Postgres↔HCFS and leave SSD orphans to W3 (leaning: the latter for v1).

---

## 3. The Postgres-backed queue pattern (Redis replacement)

Two idioms cover every worker. Neither needs Redis, retry ZSETs, a Lua mover, or per-backend DLQ lists.

### Idiom A — scan-as-queue (no queue table): W4, W5, W6, W7, W8

Claim domain rows directly off a partial index. This is what doc 12 §6 prescribes ("driven by the indexes in §5.1"). Retry state (`*_attempts`, `*_not_before`, `last_error`) is added **to the domain table** only where a row can fail and must back off (e.g. `blobs.gc_attempts`/`gc_not_before`).

```sql
-- W6 blob-refcount GC claim (single-flight per blob, with grace + backoff)
WITH picked AS (
    SELECT content_hash
    FROM   blobs
    WHERE  refcount = 0
      AND  replication_state = 'durable'
      AND  gc_not_before <= now()
    ORDER  BY created_at
    FOR UPDATE SKIP LOCKED
    LIMIT  $batch
)
UPDATE blobs b
SET    replication_state = 'deleting',   -- state machine; survives a crash mid-delete
       gc_attempts = gc_attempts + 1
FROM   picked
WHERE  b.content_hash = picked.content_hash
RETURNING b.content_hash, b.hcfs_file_id;
```

`SKIP LOCKED` gives N workers (and N tasks per worker) contention-free, no-double-processing draining — the Redis consumer-group replacement. `FOR UPDATE` on the blob row **is** the single-flight-per-blob guarantee W6 needs.

### Idiom B — the `staged_blobs` durable work queue: W1 (drained), W3 (reaped)

`staged_blobs` already has the queue columns ([`12`](./12-schema-design.md) §7). Enqueue is the PUT handler's `INSERT` — **in the same transaction** as the `blobs`/`chunk` rows, so work never exists without its intent (the core Postgres-over-Redis win; Redis enqueue-before-commit is why the Python upload queue is at-least-once, [`04`](./04-queues-and-workers.md) §4).

```sql
-- W1 claim: node-scoped, lease-based
WITH picked AS (
    SELECT content_hash, node_id
    FROM   staged_blobs
    WHERE  node_id = $me
      AND  state IN ('landed','failed')
      AND  next_attempt_at <= now()
    ORDER  BY landed_at
    FOR UPDATE SKIP LOCKED
    LIMIT  $batch
)
UPDATE staged_blobs s
SET    state = 'uploading', claimed_at = now(), attempts = attempts + 1
FROM   picked
WHERE  (s.content_hash, s.node_id) = (picked.content_hash, picked.node_id)
RETURNING s.*;
```

**Complete / retry / DLQ-equivalent / stuck-lock reaper:**

```sql
-- success (W1): blob durable, staging row slated for W2 cleanup
UPDATE blobs SET replication_state='durable', durable_at=now() WHERE content_hash=$h;
UPDATE staged_blobs SET state='uploaded', updated_at=now() WHERE (content_hash,node_id)=($h,$me);

-- transient failure: exponential backoff + jitter (the house formula, 04 §2.4)
UPDATE staged_blobs
SET    state = CASE WHEN attempts >= $max THEN 'failed' ELSE 'landed' END,
       next_attempt_at = now() + LEAST($cap_ms, $base_ms * power(2, attempts-1)) * (1 + 0.1*random()) * interval '1 ms',
       last_error = $err, claimed_at = NULL, updated_at = now()
WHERE  (content_hash,node_id) = ($h,$me);

-- W3 stuck-lock reaper (crash mid-forward): the visibility timeout a broker gives for free
UPDATE staged_blobs
SET    state='landed', claimed_at=NULL, updated_at=now()
WHERE  node_id=$me AND state='uploading' AND claimed_at < now() - $visibility_timeout;
```

- **Backoff** = `next_attempt_at` in the future; the claim's `next_attempt_at <= now()` filter enforces the wait with no timer thread. Reuse Python's `base·2^(attempt-1)` + 10% additive jitter, capped ([`04`](./04-queues-and-workers.md) §2.4).
- **DLQ-equivalent** = the `failed` terminal state (same table, excluded from the work index). Alert on `count(*) WHERE state='failed'`; requeue by flipping back to `landed`. No second system, and — unlike Redis — **no `noeviction` cap and no drop-newest**, because Postgres has no cache-fill-fails-all-writes mode. The Python DLQ cap and the purger high-water gate both **disappear**.
- **Stuck-lock reaper** = the message-broker visibility timeout, expressed as one indexed `UPDATE` (W3).

### ⚑ At-least-once, never exactly-once

`SKIP LOCKED` + a visibility timeout means a job can run twice (do the work, die before committing `uploaded`, get re-offered). Every consumer must be idempotent — which is why each worker's idempotency guarantee is a first-class column in §1. Content-addressing (`content_hash = blake3(ciphertext)`) makes this cheap: the forward, the refcount, and the HCFS store/delete are all keyed by content, so a replay converges.

---

## 4. Crash recovery — SSD-staged-but-not-forwarded parts

The dangerous window (accepted by the SSD-staging decision, [`10`](./10-write-path-decision.md) §Durability): bytes on a node's SSD, not yet in HCFS, node restarts. The staging protocol makes the recoverable cases deterministic.

**Durable-staging protocol (PUT / UploadPart handler, default buckets):**

1. Write ciphertext chunks to `.tmp` paths on SSD; `fsync` each file.
2. `fsync` the directory, then **rename** `.tmp` → final content-addressed path (atomic, same FS).
3. In **one transaction**: `INSERT blobs (replication_state='pending')` (`ON CONFLICT DO NOTHING`), the `chunk`/`chunk_blobs` (or `upload_part_chunks`) edges, and `staged_blobs (state='landed', node_id=me)`. This row is the **durable intent**.
4. **Only now** ack `200`/`ETag`, and `NOTIFY`.

Because the ack follows step 3, S3 semantics let us discard anything that didn't reach step 3 (the client never saw success). That yields three clean cases:

| Crash point | On-disk | DB | Recovery |
|-------------|---------|----|----------|
| During step 1–2 | partial/complete `.tmp`, no final file | no `staged_blobs` row | **Orphan `.tmp`** → W2/W3 delete `.tmp` older than a TTL with no row. Client got no ack → safe to drop. |
| After step 3, before forward confirmed | final file present | row `landed`/`uploading`, blob `pending` | **W3** re-drives (or the lease reaper resets an `uploading` row left by the dead forwarder); **W1** re-POSTs; HCFS dedups. |
| After HCFS accepted, before we wrote `durable` | final file present | row `uploading`, blob `pending` | Same — W1 re-POSTs, HCFS dedups, we set `durable`. Never double-stores. |

**Invariants that make "recover by re-running" always correct:**

- **A finalized (non-`.tmp`) file always has a committed `staged_blobs` row, and vice-versa** (row inserted only after rename). So "final file, no row" and "row, missing file" are both detectable orphan/error conditions W2/W3 resolve deterministically.
- **`staged_blobs` (Postgres) is the durability anchor, not the SSD** — a node restart never loses a staged-and-acked object's *intent*.
- **The forward is content-addressed + idempotent** (HCFS dedup + unique-name 1:1 + `replication_state` guard), so no bespoke resume logic.
- **In-flight MPUs** survive: each UploadPart stages its own `blobs`/`upload_part_chunks`/`staged_blobs` rows by the same protocol; a restart mid-MPU leaves completed parts recoverable, and W4 reaps the upload only if the client never completes it.

**WORM / object-lock buckets** have **no un-forwarded window**: they forward inline and ack on HCFS `200` ([`10`](./10-write-path-decision.md)). A crash mid-PUT simply fails the PUT (no ack); the client retries. Nothing to recover.

> ⚑ **Node loss ≠ crash.** All of the above recovers a **restart** (SSD intact). If the node's SSD is permanently lost while a default-bucket blob is still `pending` (the single on-node copy), that object is **lost** — the durability window the decision doc explicitly accepts for default buckets ([`10`](./10-write-path-decision.md) §"Durability on 200"). W3 will surface such rows (stuck `landed`/`uploading` on a node that never returns) for operator alerting; there is no second copy to re-drive from. WORM buckets are immune by construction.

---

## 5. Which Python workers are NOT needed (and why)

Mapping the real doc-04 fleet **plus** the six the brief named (uploader, unpinner, janitor, cache, allocator, chain-reporter). Verdicts: **dropped**, **HCFS-owned** (exists in the unchanged backend), **kept** (reborn above), **other-plane** (billing/ops, not storage).

| Python worker (source) | Verdict | Why / where it goes |
|---|---|---|
| **uploader** `arion-uploader` ([`04`](./04-queues-and-workers.md) §3.1) | **Kept → W1** | Retargeted Arion→HCFS and shrunk: HCFS owns dual-write + retry, so the XL digest-fenced state machine collapses to a small forwarder. |
| **unpinner** `arion-unpinner` (§3.2) | **Kept → W6** | Backend DELETE at refcount 0. Our `blobs.refcount` + `SKIP LOCKED` scan replaces the Redis unpin list + retry ZSET + shared DLQ. |
| **janitor** (§3.3) | **Split** | FS **read-cache** GC + hydrate + `fs_cache:pressure` → **dropped** (no read cache, [`10`](./10-write-path-decision.md)/[`12`](./12-schema-design.md) drop list). SSD-part cleanup → **W2**. Soft-delete hard-reap → **W5**. Queue-depth sampler → **dropped** (Postgres exposes depth via SQL/OTel; no Redis to sample). |
| **mpu_reaper** (§3.4) | **Kept → W4** | S3 MPU is edge-only; HCFS has no MPU. Now pure staging cleanup (resurrection wart killed by schema). |
| **purger** (§3.5) | **Kept, drastically simplified** | Bulk purge = soft-delete → W5 → refcount decref → W6. The Redis high-water backpressure gate is **dropped** (its `noeviction`-flood failure mode is gone). Claim still `purge_jobs` `SKIP LOCKED` + lease if a dedicated worker is kept. |
| **usage_rollup** (§3.6) | **Kept, shrunk → W7** | One `bucket_usage` counter + reconciler replaces the 4-table ledger apparatus ([`12`](./12-schema-design.md) §2.14). |
| **orphan_checker** (§3.7) | **Kept → W8** (optional) | Three-way audit; report-only, rate-bounded. |
| **account_cacher** (§3.8) | **Other-plane / dropped** | Substrate credit cache for the **billing gate**. Dropped if we use HCFS's gate ([`13`](./13-hcfs-as-is-integration.md) §5); else belongs to doc 05, not here. |
| **plans_cacher** (§3.9) | **Other-plane / dropped** | Plan-roll cache for billing. Same as account_cacher. |
| **migrator** (§3.10) | **Not a worker** | One-shot CLI/Job; doc 04 already says so. Greenfield does data migration-in separately. |
| **cachet_health_check** (§3.11) | **Other-plane (ops)** | External status-page pusher. Ops concern (doc 09), keep as-is or drop; not a storage janitor. |
| **drain agent** `hippius-drain-agent` ([`04`](./04-queues-and-workers.md) §1) | **Absorbed into W1 + ingest DaemonSet** | Its verify/hash/enqueue/residency role folds into the SSD-landing handler + `staged_blobs`; no separate Redis producer, no `cephor:*` lease/epoch fence. |
| **downloader** (open Q, [`04`](./04-queues-and-workers.md) Open Questions) | **Dropped** | Reads go straight from HCFS with Range ([`10`](./10-write-path-decision.md)/[`03`](./03-data-plane-cache-streaming.md)); no download worker/queue. |

**The brief's named six, explicitly:**
- **uploader → W1**, **unpinner → W6** (above).
- **janitor → split** (W2 + W5; read-cache half dropped) (above).
- **cache → dropped.** The read cache / hydrate / `RedisObjectPartsCache` plane is removed by decision ([`10`](./10-write-path-decision.md): "Drop the read cache and the janitor/hydrate system"; [`12`](./12-schema-design.md) drops `fs_cache_inventory`). Reads are cold from HCFS.
- **allocator → dropped (HCFS/validator-owned).** No pool/Ceph placement side any more ([`10`](./10-write-path-decision.md) note "there is no pool side"); byte placement + Reed-Solomon live behind HCFS/the validator roadmap.
- **chain-reporter → HCFS-owned.** `hcfs-chain-reporter` reports a non-exempt tenant's S3 bytes automatically ([`13`](./13-hcfs-as-is-integration.md) §4). We add nothing unless we need "reported but ungated", which requires owning billing (doc 05).

Net: **dropped** — read cache, allocator, FS-cache janitor, queue-depth sampler, downloader, drain (absorbed); **HCFS-owned** — Arion/S3 dual-write, storage retry, usage→chain; **kept/reshaped** — W1, W2, W4, W5, W6, W7, W8; **other-plane** — account/plans cachers, cachet, migrator.

---

## 6. Open questions

1. **Mixed-fleet cutover.** §0 assumes the greenfield Rust product runs on its **own** DB with **no** shared Redis queue, so doc 04's byte-for-byte wire contract does not apply. Confirm there is no window where Python and Rust workers drain the **same** queue (which would force us to keep the Redis contract). If migration is object-by-object into the new service, this holds.
2. **HCFS-side refcounting prerequisite (doc 10/11) vs. S3-side refcount (doc 13).** Doc 10 §"Not a differentiator" lists HCFS "blob refcounting" as a prerequisite; doc 13 concludes we own it S3-side against **unchanged** HCFS. This doc builds on **doc 13 (zero HCFS change)** — W6 owns `blobs.refcount` and uses the unique-name rule. Confirm we are **not** also asking HCFS to add refcounting (if HCFS grew it, W6's "unique name per blob" rule could relax, but nothing else changes). Which is the committed path?
3. **Batch vs single HCFS delete for W6.** Single delete + "404-is-success" is the robust default (batch is Denied + drive-scoped + 403-for-exempt, [`13`](./13-hcfs-as-is-integration.md) §3). Is there a non-exempt, single-drive tenant class where batch's amortized round-trip is worth a second code path?
4. **Grace-window length for W6.** Too short races a re-PUT of identical content; too long wastes HCFS-billed storage. Tie to observed re-PUT rate (default: minutes, configurable).
5. **`NOTIFY/LISTEN` vs poll-only for W1/W6.** `hcfs-retry-worker` is poll-only at 300 s; the forward path wants lower latency. Is `LISTEN` worth the pinned connection, or is a ~1 s poll on the partial index enough?
6. **Does W7 (usage reconciler) need the ledger apparatus on day one** ([`12`](./12-schema-design.md) §2.14 flag), or is the single counter + reconciler enough until per-bucket write contention is measured?
7. **Purge worker: keep or fold in?** With refcount-driven reclamation, is a dedicated `purge_jobs` worker still needed for large account/bucket deletes, or does soft-delete → W5 → W6 suffice (with W5 batching the hard-reap)?
8. **Node-drain procedure.** Per-node W1/W2/W3 mean a node cordon must drain its `staged_blobs` (forward all `landed`/`uploading` to HCFS) **before** the node leaves, or those objects hit the §4 node-loss window. Define the drain hook.
9. **✅ RESOLVED — Billing-plane ownership.** HCFS owns the per-tenant 402 gate + usage→chain (register; store under real ss58, non-exempt), so `account_cacher`/`plans_cacher` are **dropped** — this doc's 8-worker fleet is complete. Residual nuance only: the rewrite may keep a **thin fail-open credit cache** (register B5: HCFS's 402 is the backstop), which is a small read cache, not the Python plans-cacher plane.
10. **Admin-bearer blast radius** ([`13`](./13-hcfs-as-is-integration.md) OQ1). W1 and W6 hold the HCFS admin bearer (cross-tenant read/write/delete). Acceptable, or do we push for a scoped service bearer (an HCFS change)?
