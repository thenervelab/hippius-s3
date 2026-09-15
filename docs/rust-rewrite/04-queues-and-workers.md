# 04 — Async Work System: Redis Instances, Queues, and Background Workers

> **⚠️ This describes the LIVE Python fleet — the thing being REPLACED, not the rewrite's design.**
> The greenfield rewrite has **no Redis queues** (it uses Postgres `SELECT … FOR UPDATE SKIP LOCKED`),
> **8 workers not ~13** (doc 17), and does **not** build the Arion uploader/unpinner/janitor/allocator/
> chain-reporter — **HCFS owns** dual-write/retry/usage→chain. The byte-for-byte Redis wire contract
> below binds **only** a hypothetical mixed Python/Rust fleet draining one queue during cutover (doc 17
> OQ1 says that's not the plan). Read this as the current system + a migration reference; the rewrite's
> worker design is [`17-workers-and-background-tasks.md`](./17-workers-and-background-tasks.md).

Implementation-grade specification of the **current Python** async work subsystem: the 5 Redis
instances, the durable work queues / retry ZSETs / DLQ, and the ~13 background workers.

**Wire formats and Redis key layouts in this document are cross-language contracts.** The
Python API/workers, and the Rust drain agent (`crates/hippius-drain-agent`) already share
them; a Rust reimplementation of any worker must match them byte-for-byte so a Rust consumer
can drain a queue a Python producer filled, and vice-versa, during migration. The single
authoritative Python module is [`hippius_s3/queue.py`](../../hippius_s3/queue.py); the Rust
producer side is [`crates/hippius-drain-agent/src/enqueue.rs`](../../crates/hippius-drain-agent/src/enqueue.rs).
A golden JSON fixture (`tests/fixtures/upload_chain_request.golden.json`) pins both sides.

---

## 1. The 5 Redis instances

All five are configured in [`hippius_s3/config.py:233-247`](../../hippius_s3/config.py). Each
role is a **separate physical instance** (not a logical DB on one server) so a cache flush,
a rate-limit storm, or a queue backlog cannot take down an unrelated concern.

| # | Config field / env var | Default URL | Role | Persistence / eviction | Client factory | Used by |
|---|---|---|---|---|---|---|
| 1 | `redis_url` / `REDIS_URL` | (required, no default) | **Cache** + rate-limit-adjacent + metrics collector + `fs_cache:pressure` signal | Volatile cache (eviction allowed); **may be a Redis Cluster** (`cluster=true` / `redis-cluster` in URL) | `create_redis_client()` (auto-detects cluster vs standalone) | API read path (`RedisObjectPartsCache`), every worker's `initialize_cache_client` / `initialize_metrics_collector`, janitor pressure publisher |
| 2 | `redis_accounts_url` / `REDIS_ACCOUNTS_URL` | `redis://127.0.0.1:6380/0` | **Accounts** — substrate credit cache + S3 billing-plan roll | **Persistent, `noeviction`** (miss falls through to source, but treated as durable) | `redis.asyncio.Redis.from_url` | `account-cacher` (writes credit rows), `plans-cacher` (writes plan roll), API billing gate |
| 3 | `redis_queues_url` / `REDIS_QUEUES_URL` | `redis://127.0.0.1:6382/0` | **Durable work queues** — upload/unpin lists, retry ZSETs, DLQs, **plus the drain's `cephor:*` leader lease + epoch fence and `notify:*` pub/sub** | **Persistent, `noeviction`, 2 GB, CephFS-backed** — a dropped entry is a lost upload | `redis.asyncio.Redis.from_url` (standalone; **never** cluster) | Drain agent (producer), uploader, unpinner, purger, mpu-reaper, janitor (queue-depth sampler + DLQ protection), orphan-checker |
| 4 | `redis_rate_limiting_url` / `REDIS_RATE_LIMITING_URL` | `redis://127.0.0.1:6383/0` | **Rate limiting** + banhammer | Volatile | `redis.asyncio.Redis.from_url` | API middleware only (`app.state.redis_rate_limiting_client`, [`main.py:143`](../../hippius_s3/main.py)) — **no worker uses it** |
| 5 | `redis_acl_url` / `REDIS_ACL_URL` | `redis://redis-acl:6379/0` | **ACL cache** (formerly gateway-side); `acl_cache_ttl_seconds=300` | Volatile cache (TTL'd) | `redis.asyncio.Redis.from_url(..., decode_responses=True)` | API only (`app.state.redis_acl`, [`main.py:149`](../../hippius_s3/main.py)) — **no worker uses it** |

**Rust notes on the instances:**

- Only instances **1, 2, 3** matter for workers. Instances 4 and 5 are API-only.
- **Instance 3 (`redis-queues`) is the one that carries irreplaceable state.** It is
  `noeviction`; an uncapped DLQ, or a mass-purge unpin flood, can fill the 2 GB and fail
  *all* writes pipeline-wide (drain leases, work queues, pub/sub). Every producer must
  respect the DLQ cap and the purger's high-water backpressure (see §3).
- Instance 1 (cache) can be a cluster. The Python code uses a polymorphic
  `Union[Redis, RedisCluster]` and never touches cluster-only or standalone-only attributes
  ([`redis_utils.py:20-24`](../../hippius_s3/redis_utils.py)). In Rust, use a client that
  supports both, or detect the URL the same way (`create_redis_client`,
  [`redis_utils.py:27-51`](../../hippius_s3/redis_utils.py)). The queues instance is **always
  standalone** — the HA cutover uses `RedisReplication` + `Sentinel` behind a
  master-following service, still one logical endpoint (see
  [`docs/redis-queues-ha-cutover.md`](../redis-queues-ha-cutover.md)).
- The HA runbook's gating concern is the drain's `cephor:epoch` fence, not the queues; but a
  Sentinel failover can drop the last in-flight writes, so the queue producers rely on the
  drain's at-least-once re-enqueue for durability across a failover.

---

## 2. Queue wire formats

### 2.1 Payload models

All payloads are **JSON**, produced by pydantic `model_dump_json()` and consumed by
`model_validate` / `model_validate_json`. Two base rules drive the wire contract
([`queue.py:46-56`](../../hippius_s3/queue.py)):

```python
class RetryableRequest(BaseModel):
    model_config = ConfigDict(extra="ignore")   # tolerate unknown fields from older/newer producers
    request_id: str | None = None
    attempts: int = 0
    first_enqueued_at: float | None = None       # unix seconds (float)
    last_error: str | None = None
    ray_id: str | None = None
```

- `extra="ignore"` is load-bearing: **a Rust struct may omit fields it doesn't know, and a
  producer may add fields a consumer doesn't know, without breaking deserialization.** In
  Rust, use `#[serde(default)]` on every optional field and do **not** use
  `#[serde(deny_unknown_fields)]`.
- `first_enqueued_at` is unix epoch seconds as a float. The queue-depth sampler reads it for
  age gauges ([`queue_metrics.py:65-79`](../../hippius_s3/queue_metrics.py)).

#### `UploadChainRequest` ([`queue.py:58-78`](../../hippius_s3/queue.py))

| Field | Type | Notes |
|---|---|---|
| `address` | string | ss58 account (network prefix 42) |
| `bucket_name` | string | |
| `object_key` | string | |
| `object_id` | string | UUID |
| `object_version` | int | |
| `chunks` | list of `{ "id": int }` | `Chunk.id` is the **part number** |
| `upload_id` | string \| null | set for multipart; null for simple |
| `upload_backends` | list[string] \| null | set by API/producer at enqueue time; drives fan-out |
| `node_id` | string \| null | **ingest node whose SSD holds the part.** `None` = pool-era request (shared pool / global queue). Routes the queue name, the retry ZSET, and the DLQ re-queue. |
| `bypass_billing` | bool | operator escape (dlq_requeue `--bypass-billing`) |
| *(base)* `request_id`, `attempts`, `first_enqueued_at`, `last_error`, `ray_id` | | from `RetryableRequest` |

`.name` (log/identity, not on the wire): `multipart::{object_id}::{upload_id}::{address}`
or `simple::{object_id}::{address}`.

**The Rust producer struct** ([`enqueue.rs:64-84`](../../crates/hippius-drain-agent/src/enqueue.rs))
emits exactly: `address, bucket_name, object_key, object_id, object_version, chunks,
upload_id, upload_backends, node_id, request_id (null), attempts (0), first_enqueued_at,
bypass_billing`. It **omits** `last_error` and `ray_id` (both default on the Python side).
The golden fixture is `tests/fixtures/upload_chain_request.golden.json`;
`chunks` serializes as `[{"id": 1}]`, `object_version` as a number, `first_enqueued_at` as a
float. Keep a Rust reimplementation asserting against that golden.

#### `UnpinChainRequest` ([`queue.py:81-91`](../../hippius_s3/queue.py))

| Field | Type | Notes |
|---|---|---|
| `address` | string | ss58 |
| `object_id` | string | UUID |
| `object_version` | int \| null | **null = all versions** |
| `cid` | string \| null | **DEPRECATED**, transitional/in-flight compat only |
| `delete_backends` | list[string] \| null | set by API at enqueue time |
| *(base)* fields | | from `RetryableRequest` |

`.name`: `unpin::{cid or object_id}::{address}::{object_id}`.

### 2.2 Queue-name construction (contract functions)

| Concept | Function | Formula |
|---|---|---|
| Upload work list | `upload_queue_name(backend, node_id)` [`queue.py:94-104`](../../hippius_s3/queue.py) / [`enqueue.rs:90-95`](../../crates/hippius-drain-agent/src/enqueue.rs) | node-scoped: `{backend}_upload_requests:{node_id}` · global: `{backend}_upload_requests` |
| Upload retry ZSET | `_upload_retry_zset(backend, node_id)` [`queue.py:163-168`](../../hippius_s3/queue.py) | `{backend}_upload_retries:{node_id}` · `{backend}_upload_retries` |
| Upload DLQ list | `UploadDLQManager` [`upload_dlq.py:29`](../../hippius_s3/dlq/upload_dlq.py) | `{backend}_upload_requests:dlq` (one per backend; **not node-scoped**) |
| Unpin work list | inline [`queue.py:295`](../../hippius_s3/queue.py) | `{backend}_unpin_requests` |
| Unpin retry ZSET | `_unpin_retry_zset(backend)` [`queue.py:317-318`](../../hippius_s3/queue.py) | `{backend}_unpin_retries` (**never** node-scoped) |
| Unpin DLQ list | `UnpinDLQManager` [`unpin_dlq.py:19`](../../hippius_s3/dlq/unpin_dlq.py) | `unpin_requests:dlq` (single shared list, not per-backend) |

**Backend today:** `arion` is the only production backend, so the live keys are
`arion_upload_requests[:<node>]`, `arion_upload_retries[:<node>]`,
`arion_upload_requests:dlq`, `arion_unpin_requests`, `arion_unpin_retries`,
`unpin_requests:dlq`. The set of backends is `config.upload_backends` /
`config.delete_backends` (both default to `_storage_backends()`), so a new backend extends
the key set automatically ([`queue_metrics.py:43-62`](../../hippius_s3/queue_metrics.py)).

**Trap — the node-scoping asymmetry.** Upload lists **and** upload retry ZSETs are
node-scoped (`:<node_id>`), because only the uploader DaemonSet pod on that node can read the
bytes on that node's SSD. But the upload DLQ is a single per-backend list; the DLQ manager
re-derives the node-scoped queue name from `payload.node_id` on requeue
([`upload_dlq.py:34-41`](../../hippius_s3/dlq/upload_dlq.py)). Unpin has **no** node concept
at all (deletes hit the backend by identifier, readable from anywhere). A Rust worker must
replicate this exactly: a node-local part re-queued onto the global `arion_upload_requests`
would find no chunks and DLQ itself as a "missing" permanent error.

**Trap — normalization.** `_normalize_queue_name` strips whitespace and surrounding
quotes before every `brpop`/`lpush` on a *passed* queue name
([`queue.py:18-20`](../../hippius_s3/queue.py)) — this exists because queue names sometimes
arrive from config with stray quotes. Node-scoped names built by `upload_queue_name` are not
normalized (they're constructed, not passed).

### 2.3 Enqueue / dequeue mechanics

- **Enqueue = `LPUSH`, dequeue = `BRPOP`** (FIFO). Head-out is index `-1`; the sampler reads
  `LINDEX key -1` for oldest-age ([`queue_metrics.py:150-162`](../../hippius_s3/queue_metrics.py)).
- Upload enqueue fans out one `LPUSH` per effective backend
  ([`enqueue_upload_to_backends`, queue.py:107-140](../../hippius_s3/queue.py)). It stamps
  `request_id` (uuid4 hex), `first_enqueued_at`, `attempts=0` if unset, and resolves
  `upload_backends` via `compute_effective_backends(..., raise_on_empty=True)`.
- Unpin enqueue ([`enqueue_unpin_request`, queue.py:251-298](../../hippius_s3/queue.py)):
  either a single named queue, or fan-out to `{b}_unpin_requests` for each effective delete
  backend. If the caller requested backends but *all* are disallowed by config, it **logs an
  error and enqueues nothing** (`raise_on_empty=False` + explicit guard).
- Upload dequeue uses `BRPOP timeout=0.5` ([`queue.py:152`](../../hippius_s3/queue.py)); unpin
  dequeue `BRPOP timeout=3` default, with a short `0.05`/`0.25`-window value used by the batch
  assembler ([`queue.py:301-314`](../../hippius_s3/queue.py), unpinner `_ASSEMBLY_*`).

### 2.4 Retry ZSETs and backoff

**Structure:** each retry ZSET member is the *full JSON payload* (same shape as the work
item), scored by `next_attempt_unix_ts = now + delay_seconds`
([`enqueue_retry_request`, queue.py:171-192](../../hippius_s3/queue.py);
`enqueue_unpin_retry_request`, [queue.py:321-341](../../hippius_s3/queue.py)). Enqueuing a
retry increments `attempts`, sets `last_error`, and ZADDs.

**Backoff schedule** ([`compute_backoff_ms`, errors.py:420-424](../../hippius_s3/workers/errors.py)):

```
exp = base_ms * 2^(attempt-1)
jitter = uniform(0, exp * 0.10)          # 10% jitter, additive
delay = min(exp + jitter, max_ms)
```

| Path | base_ms | max_ms | max_attempts | Approx envelope |
|---|---|---|---|---|
| Upload | 500 (`HIPPIUS_UPLOADER_BACKOFF_BASE_MS`) | 60000 | 7 (`HIPPIUS_UPLOADER_MAX_ATTEMPTS`) | 0.5,1,2,4,8,16,32s ≈ 63s before DLQ |
| Unpin | 1000 (`HIPPIUS_UNPINNER_BACKOFF_BASE_MS`) | 60000 | 5 (`HIPPIUS_UNPINNER_MAX_ATTEMPTS`) | 1,2,4,8,16s |

**The retry mover (critical, shared, and race-sensitive).** Every uploader/unpinner replica
runs its own retry mover; a due member must move exactly once. This is done with a **Lua
script** so `ZREM` is the compare-and-swap ([`_CLAIM_DUE_RETRIES_LUA`, queue.py:205-229](../../hippius_s3/queue.py)):

```lua
local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, ARGV[2])
local moved = 0
for i = 1, #due do
  if redis.call('ZREM', KEYS[1], due[i]) == 1 then
    redis.call('LPUSH', KEYS[2], due[i])
    moved = moved + 1
  end
end
return moved
```

- Only the pod whose `ZREM` returns 1 does the `LPUSH` → no N-fold retry amplification (the
  bug that existed before, when N pods all saw the same due member and re-enqueued it).
- Lua (not Python-side ZREM-then-LPUSH) because a pod cancelled between the two commands would
  drop the member permanently — the ZSET is the *only* record the retry exists.
- `move_due_upload_retries(backend, node_id, now_ts, max_items=64)` moves onto
  `upload_queue_name(backend, node_id)`; `move_due_unpin_retries(backend, ...)` onto
  `{backend}_unpin_requests`. The mover runs on a `2.0s` `asyncio.sleep` loop, once per pod,
  off the per-request hot path ([`run_arion_uploader_in_loop.py:141-160`](../../workers/run_arion_uploader_in_loop.py);
  unpinner `_retry_mover`, [unpinner.py:711-717](../../hippius_s3/workers/unpinner.py)). The
  uploader mover uses `max_items=256`.

### 2.5 DLQ list keys and payloads

DLQ entries are `LPUSH`ed JSON envelopes wrapping the original payload
([`base.py:91-100`](../../hippius_s3/dlq/base.py)):

```json
{
  "payload": { "...model_dump() of the original request..." },
  "attempts": 0,
  "first_enqueued_at": 0.0,
  "last_attempt_at": 0.0,
  "last_error": "…",
  "error_type": "transient" | "permanent"
}
```

Subclasses add flat lookup fields alongside the envelope:
- Upload ([`upload_dlq.py:53-59`](../../hippius_s3/dlq/upload_dlq.py)): `object_id`,
  `upload_id`, `bucket_name`, `object_key`. Identifier = `object_id`.
- Unpin ([`unpin_dlq.py:28-34`](../../hippius_s3/dlq/unpin_dlq.py)): `cid`, `object_id`,
  `address`, `object_version`. Identifier = `cid or object_id`.

**DLQ cap (drop-newest).** `dlq_max_entries` (`HIPPIUS_DLQ_MAX_ENTRIES`, default 250000; 0 or
negative disables). Cap is enforced with a non-atomic `LLEN >= max` check before `LPUSH`
([`base.py:76-85`](../../hippius_s3/dlq/base.py)), so it can overshoot by up to the number of
concurrent pushers. **At the cap, `push()` is a no-op that drops the failure RECORD, never
object data** — the janitor's absolute replication gate still refuses to evict a
non-replicated chunk, and the durable trace is `object_versions.status='failed'`.

**Requeue semantics** ([`base.py:138-229`](../../hippius_s3/dlq/base.py)):
- Per-identifier lock via `SET NX PX 60000` on `dlq:requeue:lock:{dlq_key}:{identifier}`,
  released by a token-compare Lua ([`base.py:47-60`](../../hippius_s3/dlq/base.py)).
- `permanent` entries are refused unless `force=True`; otherwise pushed back.
- On requeue, `attempts` reset to 0 (unless `force`), `bypass_billing` optionally set, then
  `enqueue_func(payload)` puts it back on the primary queue. Upload requeue routes by
  `payload.node_id` ([`upload_dlq.py:34-48`](../../hippius_s3/dlq/upload_dlq.py)).
- `requeue_all` uses pipelined `RPOP` (FIFO drain) + bulk enqueue; permanent entries pushed
  back with `LPUSH`.

---

## 3. Workers

Every worker's process entrypoint is a `workers/run_*_in_loop.py` module that calls
`run_worker(factory, name, restart_on_crash=?)`
([`hippius_s3/workers/shutdown.py`](../../hippius_s3/workers/shutdown.py)). `run_worker` is
the graceful-shutdown supervisor (see §5). All workers `initialize_queue_client`
(redis-queues), `initialize_cache_client` (redis cache), and `initialize_metrics_collector`
during startup.

### 3.1 uploader (`arion-uploader`)

- **Entrypoint:** [`workers/run_arion_uploader_in_loop.py`](../../workers/run_arion_uploader_in_loop.py);
  shared engine [`hippius_s3/workers/uploader.py`](../../hippius_s3/workers/uploader.py).
- **Consumes:** `arion_upload_requests[:<node>]` (BRPOP). Queue chosen at startup by
  `NODE_NAME` env: set → node-scoped (DaemonSet on ingest node reading local SSD); unset →
  global (pool-reading Deployment) ([`run_arion_uploader_in_loop.py:131-132`](../../workers/run_arion_uploader_in_loop.py)).
- **Produces:** `chunk_backend` rows (the backend's claim to hold the acknowledged bytes);
  flips the drain's `cephor_replication_status` from `uploading` → `replicated`; retries →
  `arion_upload_retries[:<node>]`; failures → `arion_upload_requests:dlq`.
- **Loop:** bounded-dispatch — keep up to `HIPPIUS_UPLOADER_MAX_INFLIGHT` (default 4) requests
  processing concurrently ([`run_arion_uploader_in_loop.py:177-206`](../../workers/run_arion_uploader_in_loop.py)).
  Per-part chunk uploads bounded by `HIPPIUS_UPLOADER_MULTIPART_MAX_CONCURRENCY` (5); **all**
  in-flight Arion POSTs across the whole pod share one `HIPPIUS_ARION_UPLOAD_CONCURRENCY`
  (8) semaphore ([`uploader.py:99-102`](../../hippius_s3/workers/uploader.py)). DB pool
  `HIPPIUS_UPLOADER_DB_POOL_MAX` (12).
- **Singleton vs scaled:** **scaled** (many replicas + one DaemonSet pod per ingest node).
  Safe because CID assignment is content-deterministic and `insert_chunk_backend` is
  idempotent (`ON CONFLICT`). Scale via `MAX_INFLIGHT` + `ARION_UPLOAD_CONCURRENCY`, not just
  replicas.
- **Failure handling** ([`run_arion_uploader_in_loop.py:77-106`](../../workers/run_arion_uploader_in_loop.py)):
  classify via `classify_error` (upload classifier). `transient` and `attempts_next <=
  max_attempts` → `enqueue_retry_request`. Else → `_push_to_dlq` **and**
  `UPDATE object_versions SET status='failed'`. Billing (402) is not `transient`, so it DLQs
  immediately; a single 402 sets `billing_abort` to stop firing remaining chunks at Arion
  ([`uploader.py:354-376`](../../hippius_s3/workers/uploader.py)).
- **Invariants:**
  - **Skip deleted objects** before uploading (`is_object_deleted`).
  - **Hand-off fence (node-scoped requests):** a drain-published request is only actionable
    while its part row is `uploading` with a recorded `content_sha256`. The uploader polls for
    `draining` rows to clear (`_await_hand_off`, up to `uploader_hand_off_wait_seconds` ≈10s),
    re-schedules still-`draining` parts through the retry ZSET, drops stale parts, and
    computes a **part digest** over the bytes it actually sent, folding per-chunk sha256 in
    index order ([`part_digest.py`](../../hippius_s3/workers/part_digest.py) — **byte-for-byte
    identical to the Rust drain's `part_digest`**; golden-pinned by
    `tests/unit/test_part_digest.py`). It writes `chunk_backend` rows and flips to
    `replicated` **only if** the observed digest equals the drain's recorded digest
    ([`uploader.py:583-609`](../../hippius_s3/workers/uploader.py)). A mismatch means the SSD
    part was rewritten (an UploadPart retry) or the request outlived a re-drive → record
    nothing (`stale=True`).
  - **Confirm ordering:** flip `uploading→replicated` only after *every* chunk's
    `chunk_backend` row is written, and only when this backend is the whole required set
    (`upload_backends ∪ backup_backends`); with a backup backend the drain's sweep owns the
    flip ([`uploader.py:408-443`](../../hippius_s3/workers/uploader.py)). Flipping early would
    let the evictor discard the only copy of a part the backend doesn't hold yet.
  - This queue is the **only retry layer for transport failures**; the Arion client
    deliberately does not also retry connect errors (would multiply the budget).

### 3.2 unpinner (`arion-unpinner`)

- **Entrypoint:** [`workers/run_arion_unpinner_in_loop.py`](../../workers/run_arion_unpinner_in_loop.py)
  (`restart_on_crash=True`); shared engine [`hippius_s3/workers/unpinner.py`](../../hippius_s3/workers/unpinner.py).
- **Consumes:** `arion_unpin_requests` (BRPOP). **Produces:** backend DELETEs +
  `chunk_backend.deleted=true, deleted_at=now()` (soft delete); retries →
  `arion_unpin_retries`; failures → `unpin_requests:dlq`.
- **Loop:** bounded-dispatch up to `HIPPIUS_UNPINNER_MAX_INFLIGHT` (8). Two modes:
  - **Per-file** (legacy): one request → N chunk identifiers, DELETEs bounded by shared
    `HIPPIUS_UNPINNER_PARALLELISM` (5) semaphore.
  - **Batch** (`HIPPIUS_UNPINNER_BATCH_DELETE_ENABLED`): a `0.25s` assembly window
    (`_ASSEMBLY_WINDOW_SECONDS`, drain BRPOP `0.05s`) coalesces requests sharing
    `(address, folder_hash)` into one `POST /delete_files` of deduped `file_id`s, capped at
    `HIPPIUS_UNPINNER_BATCH_MAX_FILES` (1000) and hard-capped at `_HCFS_BATCH_HARD_CAP=1000`
    ([`unpinner.py:39-45,628-707`](../../hippius_s3/workers/unpinner.py)). A batch endpoint 404
    (`BatchEndpointUnavailable`) falls the whole group back to per-file.
- **Singleton vs scaled:** **scaled**. Atomic BRPOP dequeue + idempotent DELETE (404
  tolerated) + idempotent soft-delete make cross-pod concurrency safe.
- **Failure handling / invariants:**
  - **Invariant A9:** a chunk is soft-deleted **only** when its `file_id`'s backend DELETE
    succeeded *and* the soft-delete DB write succeeded; a request is acked only when *all* its
    file_ids clear both gates. On any failure the request routes to retry/DLQ and **does
    not** soft-delete — soft-deleting a still-pinned object would strand the pin forever
    (nothing retries a soft-deleted row) ([`unpinner.py:399-442`](../../hippius_s3/workers/unpinner.py),
    `process_unpin_batch` A9 gate [`unpinner.py:250-316`](../../hippius_s3/workers/unpinner.py)).
  - **No-rows retry:** a request whose `chunk_backend` rows don't exist yet (pin commit not
    landed) is retried up to **6** times then dropped ([`unpinner.py:363-391`](../../hippius_s3/workers/unpinner.py)).
  - Error classifier is the **unpin** variant: **404 is transient** (pin commit pending),
    unlike upload/download.
  - DB pool sized `parallelism + max_inflight`, capped at `HIPPIUS_UNPINNER_DB_POOL_MAX` (16),
    with a deadlock-safe floor of `parallelism + 1` ([`unpinner.py:515-542`](../../hippius_s3/workers/unpinner.py)).
  - **Shutdown ordering:** in-flight requests are cancelled *before* the shared backend client
    is closed (they hold it for their Arion DELETEs) ([`unpinner.py:781-793`](../../hippius_s3/workers/unpinner.py)).

### 3.3 janitor (`janitor`) — FS cache GC + queue-depth sampler host

- **Entrypoint:** [`workers/run_janitor_in_loop.py`](../../workers/run_janitor_in_loop.py)
  (large, 2500+ lines).
- **Consumes:** the redis-queues DLQ lists (read-only, for **DLQ protection**) and samples
  every queue/ZSET/DLQ depth. **Produces:** deletes stale/aged/orphan FS parts; hard-deletes
  soft-deleted objects/versions; publishes `fs_cache:pressure` on the cache Redis; publishes
  `queue_depth` / `queue_oldest_age_seconds` OTel gauges.
- **Loop cadence:** one cycle then `asyncio.sleep`: **600s normal**, `janitor_pressure_sleep_seconds`
  under disk pressure ([`run_janitor_in_loop.py:2403-2530`](../../workers/run_janitor_in_loop.py)).
  The FS walk is **sharded** (`HIPPIUS_JANITOR_WALK_SHARDS`, 64) — one hash-shard per cycle, a
  full sweep every `shards` cycles; walk concurrency `HIPPIUS_JANITOR_CONCURRENCY` (32). The
  **QueueDepthSampler** runs as its own task on a fixed `30s` interval, off the cycle path
  ([`queue_metrics.py:166-176`](../../hippius_s3/queue_metrics.py),
  [`run_janitor_in_loop.py:2338-2339`](../../workers/run_janitor_in_loop.py)).
- **Singleton vs scaled:** **strict singleton.** Pressure hysteresis is in-process
  (`_prev_pressure_mode`), it hosts the single-source-of-truth queue sampler, and its census
  gauges assume one writer.
- **Failure handling:** each phase wrapped in try/except that logs and continues; a Redis blip
  in the sampler keeps prior gauge values (zeroing would read as "all queues drained" — the
  opposite of the truth).
- **Invariants:**
  - **Absolute replication gate:** a chunk not backed up to *every* required backend is
    **never** deleted, under any disk pressure. Critical pressure with no replicated parts →
    log ERROR and do nothing (page an operator, never lose data).
  - **DLQ protection (fail-closed, A15):** `get_all_dlq_object_ids` reads *all* upload+unpin
    DLQ lists ([`run_janitor_in_loop.py:871-905`](../../workers/run_janitor_in_loop.py)); if
    any read fails it raises `DLQProtectionUnavailable`. The stale-reap path skips entirely on
    that failure (fail-closed); the age-GC path degrades to replication-gate-only (C1). This
    is why the DLQ list layout is a contract the janitor also depends on.

### 3.4 mpu_reaper (`mpu-reaper`)

- **Entrypoint:** [`workers/run_mpu_reaper_in_loop.py`](../../workers/run_mpu_reaper_in_loop.py)
  (`restart_on_crash=True`); logic [`hippius_s3/services/mpu_cleanup.py`](../../hippius_s3/services/mpu_cleanup.py).
- **Consumes:** DB (`list_abandoned_versions`, `list_orphan_replication_versions`) + reads DLQ
  lists for protection. **Produces:** marks abandoned/leaked `cephor_replication_status` rows
  terminal (`failed`) and deletes `multipart_uploads` headers; enqueues nothing.
- **Cadence:** `HIPPIUS_MPU_REAPER_INTERVAL_SECONDS` (120s). Two passes per cycle:
  `reap_abandoned_uploads` (window `mpu_stale_seconds`=172800/2d) and
  `sweep_orphan_replication_versions` (window `mpu_sweep_grace_seconds`=172800/2d, the A21 leak
  backstop) ([`mpu_cleanup.py:175-266`](../../hippius_s3/services/mpu_cleanup.py)).
- **Singleton vs scaled:** **singleton** (pure DB+Redis, no per-node FS).
- **Failure handling:** `run_reaper_cycle` never raises. DB pool has **both**
  `command_timeout` (client) and server `statement_timeout` (`mpu_reaper_statement_timeout_seconds`)
  — a wedged statement pins the xmin horizon and blocks VACUUM database-wide (real incident:
  one query survived its SIGKILLed pod 7,377s).
- **Invariants:** never reaps a DLQ-protected object; the sweep only *marks* (never deletes)
  so it's resilient per-version; `sweep_grace_seconds` MUST match the janitor's
  aged-pending-orphan gauge grace.

### 3.5 purger (`purger`) — account purge

- **Entrypoint:** [`workers/run_purger_in_loop.py`](../../workers/run_purger_in_loop.py);
  logic [`hippius_s3/workers/purger.py`](../../hippius_s3/workers/purger.py).
- **Consumes:** `purge_jobs` DB table (claimed with `SKIP LOCKED` + lease). **Produces:**
  soft-deletes objects/buckets and **fans out `UnpinChainRequest`s** into the unpin queues
  ([`purger.py:90-98`](../../hippius_s3/workers/purger.py)); drops sub-token scopes + gateway
  cache entries.
- **Cadence:** poll every `HIPPIUS_PURGER_INTERVAL_SECONDS` (10s) when idle; batch
  `HIPPIUS_PURGER_BATCH_SIZE` (500); lease `HIPPIUS_PURGER_LEASE_SECONDS` (600s).
- **Singleton vs scaled:** single replica suffices; **claiming is `SKIP LOCKED` race-safe
  regardless**, so multiple replicas are safe.
- **Failure handling:** a top-level catch marks the job `state='failed'` (never
  forever-`running`); `refuse_destructive_operation` gate is re-checked here (a job can predate
  the allowlist) and its raise lands the job as `failed`, never retried.
- **Invariant — backpressure:** before every unpin-enqueue batch, `_wait_for_unpin_headroom`
  blocks until **every** `{b}_unpin_requests` depth < `HIPPIUS_PURGER_UNPIN_QUEUE_HIGH_WATER`
  (50000), advancing the job heartbeat while parked
  ([`purger.py:24-41`](../../hippius_s3/workers/purger.py)). A mass purge once queued 1.29M
  unpin requests and broke prod GETs on redis-queues — this gate exists so a purge can never
  flood the `noeviction` queues instance again. **Any Rust reimplementation of the purger MUST
  keep this gate.** Purge is resume-safe: counters continue from the claimed row.

### 3.6 usage_rollup (`usage-rollup`)

- **Entrypoint:** [`workers/run_usage_rollup_in_loop.py`](../../workers/run_usage_rollup_in_loop.py);
  logic in `hippius_s3/services/storage_rollup_service`.
- **Consumes/produces:** folds `storage_delta_ledger` (trigger-populated) into
  `bucket_storage_usage`. No Redis. Two jobs, one loop:
  - **COMPACT** every `HIPPIUS_USAGE_ROLLUP_LOOP_SLEEP` (5s): `DELETE … RETURNING` claims
    ledger rows and adds to the rollup — exactly-once by construction (rows leave the ledger in
    the same txn that applies them).
  - **RECONCILE** every `HIPPIUS_USAGE_RECONCILE_INTERVAL_SECONDS` (300s): recompute a rolling
    bucket slice, report drift (expected zero).
- **Singleton vs scaled:** **singleton** (writes the PRIMARY; pool size 2). Runs on the
  PRIMARY, not the replica the plans-cacher uses, with a server `statement_timeout`.
- **Failure handling:** `run_cycle` never raises; the ledger is insert-only so a skipped cycle
  is lossless. **Ordering constraint:** must not run before the backfill
  (`backfill_bucket_storage_usage.py`); `usage_service` refuses to serve a non-backfilled
  rollup.

### 3.7 orphan_checker (`orphan-checker`)

- **Entrypoint:** [`workers/run_orphan_checker_in_loop.py`](../../workers/run_orphan_checker_in_loop.py)
  (`restart_on_crash=True`).
- **Consumes:** chain via `HippiusApiClient.list_files` per account. **Produces:** for each
  on-chain `s3-*` file whose CID is absent from local `cids`/`part_chunks`, enqueues an
  `UnpinChainRequest` with the deprecated `cid` set and synthetic
  `object_id="00000000-…"`, `object_version=0` ([`run_orphan_checker_in_loop.py:95-103`](../../workers/run_orphan_checker_in_loop.py)).
- **Cadence:** `ORPHAN_CHECKER_LOOP_SLEEP` (7200s / 2h); 60s backoff after a failed cycle.
  Batch `orphan_checker_batch_size`; optional account whitelist.
- **Singleton vs scaled:** singleton (uses a single `asyncpg.connect`, not a pool).
- **Failure handling:** cycle wrapped; errors recorded and retried after 60s.

### 3.8 account_cacher (`account-cacher`)

- **Entrypoint:** [`workers/run_account_cacher_in_loop.py`](../../workers/run_account_cacher_in_loop.py).
- **Consumes:** substrate chain (`SubstrateCacher`, `substrate_url`). **Produces:** main-account
  credit rows into **redis-accounts** (`redis_accounts_url`) — not redis-queues.
- **Cadence:** every **300s** (`asyncio.sleep(300)`), no config knob.
- **Singleton vs scaled:** singleton.
- **Failure handling:** a failed pass logs and retries next cycle; no crash-restart.

### 3.9 plans_cacher (`plans-cacher`)

- **Entrypoint:** [`workers/run_plans_cacher_in_loop.py`](../../workers/run_plans_cacher_in_loop.py).
- **Consumes:** `GET /api/s3/plans/accounts/` from api.hippius.com (paginated, `MAX_PAGES=500`)
  + an indexed SUM over `bucket_storage_usage` per plan account (on the **read replica**,
  `database_readonly_url`). **Produces:** the plan roll + catalog into **redis-accounts**
  (`plans_cache.publish_plan_roll`).
- **Cadence:** `HIPPIUS_PLANS_LOOP_SLEEP` (120s); 60s backoff after a failed cycle.
- **Singleton vs scaled:** singleton; **all-or-nothing publish** — if any page or any usage
  count fails, the exception propagates before publish and the last-known-good roll keeps
  serving (partial publish would silently drop accounts to pay-as-you-go and 402 them).
- **Failure handling:** `run_cycle` never raises; caches have **no TTL** (staleness measured
  explicitly via `plans_stale_after_seconds`); crash (not cycle failure) exits for the kubelet.
- **Ordering:** `require_rollup_ready` gates the whole cycle (cheap local SELECT first) so
  pre-backfill cycles don't do the full upstream fetch and throw it away.

### 3.10 migrator (`run_migrator_once`)

- **Entrypoint:** [`workers/run_migrator_once.py`](../../workers/run_migrator_once.py).
- **Shape:** **not a loop** — a one-shot job (Kubernetes Job / manual). It shells out to
  `python -m hippius_s3.scripts.migrate_objects` with `--bucket`/`--key`/`--dry-run` from
  `MIGRATE_BUCKET`/`MIGRATE_KEY`/`MIGRATE_DRY_RUN` env, and exits with the subprocess's code.
- No Redis, no queue interaction directly; migrates object storage backends. In Rust this is a
  CLI subcommand, not a long-running worker.

### 3.11 cachet_health_check (`cachet-health-checker`)

- **Entrypoint:** [`workers/cachet_health_check.py`](../../workers/cachet_health_check.py).
- **Consumes:** `GET http://gateway:8080/health`. **Produces:** PUTs component status to Cachet
  (1=operational, 3=partial, 4=major) + a metric.
- **Cadence:** every **60s**. Singleton. No queue/Redis-queues involvement. Note: this one runs
  `asyncio.run` directly, **not** through `run_worker` (no graceful-shutdown wrapper).

---

## 4. Idempotency & ordering guarantees

| Queue / worker | Delivery | Idempotency mechanism | Ordering |
|---|---|---|---|
| Upload queue | at-least-once (drain enqueues *before* `mark_uploading` commit; a crash → re-drain → duplicate) | `insert_chunk_backend` is `ON CONFLICT` (idempotent); content-deterministic CIDs; the **part-digest hand-off fence** rejects stale/rewritten bytes | FIFO `LPUSH`/`BRPOP`, but **not relied upon** — each request is a single part; the digest fence handles UploadPart-retry races |
| Upload retry ZSET | exactly-once move | Lua `ZREM`-as-CAS: only the winning pod `LPUSH`es | score = due time |
| Unpin queue | at-least-once | backend DELETE tolerates 404; soft-delete is idempotent; A9 gate never acks a partial | batch assembler coalesces by `(address, folder_hash)` — order within a batch irrelevant |
| Unpin retry ZSET | exactly-once move | same Lua CAS | score = due time |
| DLQ | at-most-once record (drop-newest at cap) | per-identifier `SET NX PX` lock on requeue | `RPOP` FIFO on `requeue_all` |
| purge_jobs | exactly-once claim | `SELECT … FOR UPDATE SKIP LOCKED` + lease + resume-safe counters | one job at a time per claimant |
| usage ledger | exactly-once | `DELETE … RETURNING` in the applying txn | per-bucket fold order irrelevant |
| plans/account cache | last-writer-wins, all-or-nothing publish | no TTL; shrink-guard refuses a smaller roll unless the key is `DEL`'d | full replace per cycle |

**Key global invariant:** `replicated` (drain flip) must happen *after* all `chunk_backend`
rows for the part are written — the flip is what authorizes the SSD evictor. `redis-queues`
is `noeviction` and durable precisely because a lost upload request is a lost upload; every
producer (drain, purger, orphan-checker, DLQ requeue, retry mover) writes there and must not
be allowed to flood it.

---

## 5. Rust implementation notes

### 5.1 The `redis` crate patterns (mirror the drain)

The drain already uses `redis::aio::ConnectionManager` with `redis::AsyncCommands`
([`enqueue.rs`](../../crates/hippius-drain-agent/src/enqueue.rs)). Mirror it:

- **`ConnectionManager`** is a cheap-to-clone multiplexed handle; clone it per operation.
  `lpush(&queue, &payload)` is one round-trip per backend.
- **Fan-out:** loop over `backends`, `upload_queue_name(backend, node_id)`, `LPUSH` the same
  serialized payload — exactly `enqueue_upload_to_backends`.
- **Dequeue:** `BRPOP key timeout` returns `Option<(String, String)>` (key, payload); parse
  with serde. Match the Python timeouts (upload 0.5s, unpin 3s default / 0.05s assembly).
- **Retry ZSET:** `ZADD key {member: score}` where member is the payload JSON and score is
  `now + delay`. **The retry mover MUST use the exact Lua script** from
  [`queue.py:205-215`](../../hippius_s3/queue.py) via `redis::Script` / `EVAL` (2 keys:
  zset, target list; 2 argv: now_ts, max_items) — a Python-side ZREM-then-LPUSH is a
  correctness bug (drops members on cancel; N-fold amplification).
- **DLQ lock:** `SET key token NX PX 60000`; release via the token-compare Lua from
  [`base.py:57-58`](../../hippius_s3/dlq/base.py).
- **Reconnect:** the Python `with_redis_retry` retries `BusyLoadingError` / `ConnectionError` /
  `TimeoutError` with reconnect ([`redis_utils.py:54-110`](../../hippius_s3/redis_utils.py));
  `ConnectionManager` reconnects automatically, but wrap `BRPOP`/`LPUSH` in a bounded retry so
  a Sentinel failover blip is absorbed (the HA doc relies on consumer reconnect).

### 5.2 Structuring each worker as a Rust binary with graceful shutdown

The Python `run_worker` / `_supervise` contract
([`shutdown.py`](../../hippius_s3/workers/shutdown.py)) maps directly to
`tokio_util::sync::CancellationToken` (which HCFS already mandates for background loops):

- One binary per worker (`arion-uploader`, `arion-unpinner`, `janitor`, `mpu-reaper`,
  `purger`, `usage-rollup`, `orphan-checker`, `account-cacher`, `plans-cacher`,
  `cachet-health-checker`; migrator is a one-shot subcommand).
- Install SIGTERM/SIGINT handlers (`tokio::signal`) that `cancel()` the token. The main loop
  `tokio::select!`s between `token.cancelled()` and its work.
- **Bounded drain on shutdown:** Python bounds the drain at
  `HIPPIUS_WORKER_DRAIN_TIMEOUT_SECONDS` (default 20s), which **must fit inside
  `terminationGracePeriodSeconds`** or the kubelet SIGKILLs mid-drain. In Rust, wrap the
  in-flight drain in `tokio::time::timeout`.
- **Always close pools/clients in a `finally`/drop:** the Python comments are emphatic — an
  uncancelled worker keeps its Postgres backend alive and pins the xmin horizon (the
  mpu-reaper 7,377s incident). Store and await all spawned task handles (mirrors HCFS's
  "store and await all JoinHandles" rule).
- **Crash policy:** Python's `restart_on_crash` distinguishes in-process restart (invisible to
  pod alerting) from letting the crash exit (kubelet restarts, `restart_count` rises). Prefer
  letting the process exit on an unexpected crash; only the uploader-adjacent loops
  (`arion-unpinner`, `mpu-reaper`, `orphan-checker`) set `restart_on_crash=True`. Never restart
  on a shutdown signal.
- **Bounded-dispatch loop** (uploader/unpinner): keep a `JoinSet`/set of in-flight tasks;
  gate new dequeues on `len < max_inflight`; reap completed tasks; run the retry mover as one
  separate task on a 2s tick. This is exactly
  [`run_arion_uploader_in_loop.py:162-215`](../../workers/run_arion_uploader_in_loop.py).

### 5.3 Traps in matching key names / payload encodings (migration-critical)

1. **`upload_queue_name` node-scoping must match byte-for-byte.** `{backend}_upload_requests`
   vs `{backend}_upload_requests:{node_id}`. The drain, Python uploader, and any Rust uploader
   must agree, or requests strand on a list nobody reads. Test the same way the drain does
   (`the_queue_name_is_node_scoped_only_when_a_node_is_given`,
   [`enqueue.rs:246-253`](../../crates/hippius-drain-agent/src/enqueue.rs)).
2. **Retry ZSETs are node-scoped for upload, never for unpin. DLQ is per-backend for upload,
   single shared `unpin_requests:dlq` for unpin.** Don't over-generalize.
3. **Payload = pydantic `model_dump_json`.** `object_version` is a JSON number,
   `first_enqueued_at` a float, `chunks` a list of `{"id": <int>}`. Use `serde` with
   `#[serde(default)]` on every optional field and **no `deny_unknown_fields`** — the wire
   contract explicitly tolerates unknown fields in both directions (`extra="ignore"`). Assert
   against `tests/fixtures/upload_chain_request.golden.json`.
4. **Producer emits a field subset.** The Rust drain deliberately omits `last_error` and
   `ray_id`; a Rust consumer must default them. Conversely a Rust producer should not emit
   fields the golden lacks.
5. **DLQ envelope ≠ payload.** The DLQ list stores `{payload, attempts, first_enqueued_at,
   last_attempt_at, last_error, error_type}` **plus** flat lookup fields (`object_id`,
   `upload_id`, …). Anything scanning DLQs for protection reads `entry["object_id"]` at the
   top level ([`run_janitor_in_loop.py:892-898`](../../workers/run_janitor_in_loop.py),
   [`mpu_cleanup.py:165-171`](../../hippius_s3/services/mpu_cleanup.py)) — a Rust DLQ writer
   must keep those flat fields or it silently breaks janitor/reaper protection.
6. **`error_type` is a string** (`"transient"` / `"permanent"`; also `"billing"` / `"unknown"`
   from the classifier), and `requeue` refuses `"permanent"` without `--force`. Match the
   classifier taxonomy in [`errors.py`](../../hippius_s3/workers/errors.py) exactly, including
   the **404-is-transient-on-unpin** divergence and the backoff formula (10% additive jitter).
7. **`part_digest` is a cross-language golden.** Fold = `sha256(TAG || u64_le(count) ||
   for each hash: u32_le(len) || ascii(hex_hash))`, `TAG =
   b"hippius-drain/part-digest/v1\n"` ([`part_digest.py`](../../hippius_s3/workers/part_digest.py),
   Rust `hippius-drain-core::redrive::part_digest`). Any Rust uploader recomputing the
   hand-off fence must reproduce this exactly (pinned by `tests/unit/test_part_digest.py`).
8. **`_normalize_queue_name`** strips quotes/whitespace off *passed* queue names before Redis
   ops. If a Rust worker reads a queue name from config, apply the same normalization.
9. **DLQ cap is `noeviction`-protective, not optional.** Enforce `LLEN >= max_entries` →
   drop-newest before `LPUSH` (default 250000; ≤0 disables). Dropping the *record* is
   acceptable; the object's durable state lives in `object_versions.status`.

---

## Open questions

- **Downloader:** there is no download worker (the read path fetches straight from the backend
  into memory, `hippius_s3/reader/backend_fetch.py`). But `errors.py` has a full
  `classify_download_error`, and `queue_metrics.py`'s 2026-07 audit note references a real
  `ovh_download_requests` queue that accumulated 136k payloads. **Confirm whether
  `*_download_requests` queues / a download worker still exist for any non-arion backend**
  before deciding the Rust rewrite can omit them entirely. The comment in `_claim_due_retries`
  ([`queue.py:195-204`](../../hippius_s3/queue.py)) says "every uploader, unpinner **and
  downloader** replica runs its own retry mover", implying a download path still exists
  somewhere.
- **`cephor:*` / `notify:*` keys on redis-queues** are the drain's leader lease/epoch fence and
  pub/sub. They are out of scope for this subsystem doc (drain-owned) but **share the
  redis-queues instance**; a Rust worker rewrite must not assume it owns that keyspace. See the
  drain crate + [`docs/redis-queues-ha-cutover.md`](../redis-queues-ha-cutover.md).
- **`unpinner_folder_hash`:** the batch delete keys on a single config
  `unpinner_folder_hash` (default `""`); the code notes it's "effectively constant". Confirm
  whether multi-folder-hash batching is ever exercised, or whether the Rust batcher can assume
  one group per address.
- **`arion` is the only production backend.** `upload_backends`/`delete_backends`/
  `backup_backends` are lists, and the key layout is per-backend, but only `arion` is live.
  Confirm whether the Rust rewrite must support multi-backend fan-out on day one or can start
  arion-only with the key scheme kept generic.
- **Rate-limit (4) and ACL (5) Redis instances** are API-only today. Confirm no worker in the
  target design needs them (the banhammer, for instance, could plausibly move worker-side).
