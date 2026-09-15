# 09 — OPS: Deployment, Configuration, Observability & the Python→Rust Cutover

**Subsystem owner:** OPS
**Scope:** the deployment substrate, the full configuration surface, the observability contract, the build/CI pipeline, and the cutover strategy for the from-scratch Rust reimplementation of `hippius-s3`.

> **⚠️ Rewrite delta:** the greenfield build is **not** operationally interchangeable with the Python
> fleet — it is a **separate namespace (`hippius-s3r`), own Postgres, no Redis, no Ceph, no
> janitor/uploader/unpinner**, with **Postgres-queued** workers (doc 17). So §5's "read-compat before
> write / same schema / dbmate stays authoritative / shadow-alongside-Python" cutover model and the
> Redis-HA / shared-schema open questions are **superseded** by the re-encrypt, per-bucket migration in
> [`20-migration-backfill.md`](./20-migration-backfill.md) + IMPLEMENTATION-PLAN §12–13. The env/metric/
> image references here are a useful **Python-system reference**; the rewrite's deployment is
> IMPLEMENTATION-PLAN §13 (migrations are **sqlx-owned**, own DB).

This section documents the **current Python** ops surface as a reference. Every rule here is derived
from the live Python code, manifests, and runbooks (all paths under `/Users/camden/Source/hippius-s3/`);
the existing Rust drain (`crates/`) is treated as the reference implementation for "how a Rust service
in this system is built, shipped, and observed."

> **Guiding constraint (from CLAUDE.md and the drain-direct rollout):** the server never sees plaintext, the server is the source of truth, and there are **no artificial throughput caps in the data path**. The Rust rewrite inherits both the config compatibility burden and the "fail loud, never silently degrade" posture that pervades this config surface.

---

## 0. The two deployment substrates

There are two ways the system runs, and the Rust rewrite must support both:

| Substrate | Used for | Composed from |
|---|---|---|
| **docker-compose** | local dev, `e2e`, and single-host prod-style runs | `docker-compose.yml` (base) + `.prod.yml` / `.e2e.yml` / `.monitoring.yml` overlays |
| **k8s / kustomize** | staging + production clusters | `k8s/base/` + `k8s/{staging,production}/` overlays, applied with `kubectl apply -k` |

The system is **mid-migration itself** at two levels: a monolith→drain-direct topology change (base `api` Deployment → `api-local` DaemonSet + Rust drain fleet), and a Python→Rust language migration (the drain crates are already Rust). The Rust rewrite is the continuation of the second; this doc's cutover playbook (§5) is modeled directly on how the drain was cut over.

---

## 1. Configuration surface

Config is loaded by `hippius_s3/config.py` (a single frozen `@dataclasses.dataclass Config`, memoized via `get_config()`). There is **no `config.py` at repo root** — the sole config module is `hippius_s3/config.py` (994 lines). Env resolution order: `.env.defaults` (base) → `.env` / `.env.example` (overrides) → real process env; k8s uses `configmap-defaults.yaml` (`hippius-s3-defaults`) + `configmap-environment.yaml` + the `hippius-s3-secrets` Secret.

### 1.1 Config-load invariants the Rust rewrite must preserve

These are load-bearing behaviors of `get_config()` (lines 933-994), not incidental:

- **Fail-fast on missing required vars.** `env("X")` with no default raises `KeyError` → `ValueError("X environment variable is required...")`. `ENVIRONMENT` is explicitly required and rejected if blank.
- **Keystore DSN fallback:** `HIPPIUS_KEYSTORE_DATABASE_URL` falls back to `DATABASE_URL` when empty.
- **Read-only DSN fallback:** `DATABASE_READONLY_URL` falls back to `DATABASE_URL` when empty (local/test/e2e have no replica).
- **`enable_bypass_credit_check` is clamped to `False` unless `ENVIRONMENT=test`** — a production safety clamp.
- **KMS validation:** `HIPPIUS_KMS_MODE` must be exactly `required` or `disabled`; when `required`, all five OVH KMS vars must be present at startup or the pod crashes; when `disabled`, `HIPPIUS_AUTH_ENCRYPTION_KEY` is mandatory (used for local KEK wrapping).
- **Boolean parsing is strict.** The billing-plans switch uses `_parse_bool` (`hippius_s3/config.py:83-106`): accepts `true/1/yes/y/on` (and their false counterparts), and **raises on anything else** — a typo fails the pod rather than silently disabling a billing feature. (Older flags still use the lax `x.lower() == "true"`; new Rust code should follow the strict form.)
- **Service-account allowlist is fail-closed and validated.** `HIPPIUS_SERVICE_ACCOUNT_IDS` is parsed by `_parse_service_accounts` against SS58 network format **42**; a malformed address raises rather than silently demoting an internal account to "billed."
- **Two secrets pinned in code, NOT env-driven:** `STORAGE_BACKENDS = ("arion",)` and `HIPPIUS_SS58_FORMAT = 42` (`config.py:36,45`). The storage-backend set is the replication contract and is duplicated in the Rust drain (`crates/hippius-drain-agent/src/config.rs`). **The Rust rewrite must keep the same constant in both languages in sync** — a second backend is a coordinated code change, not a config flip.
- **Env-specific billing switch:** `_parse_enable_billing_plans` prefers `HIPPIUS_ENABLE_BILLING_PLANS_<STAGING|PROD>` selected by the pod's own `ENVIRONMENT` (note `production`→`PROD`, not `PRODUCTION`), falling back to the unsuffixed `HIPPIUS_ENABLE_BILLING_PLANS`. The safety property: a prod pod can only ever read the prod value.

### 1.2 Categorized env-var table (the important surface)

Defaults shown are the code defaults from `config.py`/`.env.defaults`; **no secret values are reproduced.**

#### Database
| Var | Meaning | Default / notes |
|---|---|---|
| `DATABASE_URL` | primary Postgres DSN (app schema, owned by dbmate) | required |
| `HIPPIUS_KEYSTORE_DATABASE_URL` | keystore DSN (KEK material) | falls back to `DATABASE_URL` |
| `DATABASE_READONLY_URL` | replica DSN for lag-tolerant readers (plans-cacher) | falls back to `DATABASE_URL` |
| `API_DB_POOL_MIN_SIZE` / `_MAX_SIZE` | asyncpg pool bounds (per process) | 5 / 20 (k8s: 3 / 15) |
| `API_DB_POOL_MAX_QUERIES` / `_MAX_INACTIVE_LIFETIME` / `_COMMAND_TIMEOUT` / `_ACQUIRE_TIMEOUT` | pool recycling + timeouts | 50000 / 300 / 30 / 5.0 |
| `HIPPIUS_UPLOADER_DB_POOL_MAX` / `HIPPIUS_UNPINNER_DB_POOL_MAX` / `KEK_DB_POOL_MIN_SIZE`/`_MAX_SIZE` | per-worker pool caps (multiplied by replicas vs `max_connections`) | 12 / 16 / 1 / 10 |

> **Pool math is a cluster-wide constraint.** `UVICORN_WORKERS=4` means a pod holds 4× its asyncpg pool. Prod Postgres runs `max_connections=1000` (`docker-compose.prod.yml`). The Rust rewrite must budget its connection pools against the same ceiling (see the inline comments at `config.py:459-464`).

#### Redis (five logical instances)
| Var | Role | Default |
|---|---|---|
| `REDIS_URL` | general short-lived cache | `redis://redis:6379/0` (prod: cluster URL) |
| `REDIS_ACCOUNTS_URL` | persistent account/credit + plan cache | `redis://127.0.0.1:6380/0` |
| `REDIS_QUEUES_URL` | **work queues + `notify:*` pub/sub + drain `cephor:*` lease/epoch fence + DLQ** | `redis://127.0.0.1:6382/0` |
| `REDIS_RATE_LIMITING_URL` | rate limiting + banhammer | `redis://127.0.0.1:6383/0` |
| `REDIS_ACL_URL` | ACL cache | `redis://redis-acl:6379/0` |

> `REDIS_QUEUES_URL` is the highest-value stateful dependency: losing it loses uploads **and** the drain's leader lease/epoch fence (split-brain risk). It runs `--maxmemory-policy noeviction` in prod (fail-loud). See the HA cutover, §5.4.

#### Storage backends, Arion, Substrate, Hippius API
| Var | Meaning | Default |
|---|---|---|
| `HIPPIUS_ARION_BASE_URL` / `HIPPIUS_ARION_VERIFY_SSL` | Arion storage backend | `https://arion.hippius.com/` / `true` |
| `ARION_SERVICE_KEY` / `ARION_BEARER_TOKEN` / `ARION_BILLING_BYPASS_KEY` / `ARION_RATE_LIMITING_PROXY_BYPASS_KEY` | Arion auth/bypass | secrets |
| `HIPPIUS_SUBSTRATE_URL` | chain RPC | `wss://rpc.hippius.network` |
| `HIPPIUS_VALIDATOR_REGION` | validator region marker | `decentralized` |
| `HIPPIUS_API_BASE_URL` | Hippius platform API | `https://api.hippius.com/api` |
| `HIPPIUS_SERVICE_KEY` | Hippius service credential | secret |
| `HIPPIUS_AUTH_ENCRYPTION_KEY` | local KEK-wrapping key (KMS disabled mode) | secret |
| `STORAGE_BACKENDS` | **not env — pinned to `("arion",)` in code** | — |

#### KMS (OVH KMS / KEK wrapping)
| Var | Meaning | Default |
|---|---|---|
| `HIPPIUS_KMS_MODE` | `required` (prod/staging/e2e) or `disabled` (dev) | `disabled` |
| `HIPPIUS_OVH_KMS_ENDPOINT` / `_OKMS_ID` / `_DEFAULT_KEY_ID` | KMS instance + default key | required when `required` |
| `HIPPIUS_OVH_KMS_CERT_PATH` / `_KEY_PATH` / `_CA_PATH` | mTLS client cert / key / (optional) CA | required when `required` |
| `HIPPIUS_OVH_KMS_TIMEOUT_SECONDS` / `_MAX_RETRIES` / `_RETRY_BASE_MS` / `_RETRY_MAX_MS` | KMS RPC tuning | 30.0 / 3 / 500 / 5000 |
| `HIPPIUS_OVH_KMS_KEEPALIVE_EXPIRY` / `KEK_CACHE_TTL_SECONDS` | keep mTLS warm / KEK cache TTL | 300 / 300 |

#### Security / HMAC / feature flags
| Var | Meaning | Default |
|---|---|---|
| `FRONTEND_HMAC_SECRET` | sub-token / frontend HMAC | secret |
| `HIPPIUS_ADMIN_HMAC_SECRET` | `/admin/*` endpoints (empty = admin API disabled) | empty |
| `HIPPIUS_AUTH_PROBE_SECRET` / `HIPPIUS_INTERNAL_PEER_SECRET` | ATS auth-probe / peer-fetch shared secrets (empty = feature/route not mounted) | empty (`repr=False`) |
| `API_SIGNING_KEY` | presigned-URL signing key | random UUID if unset |
| `ENVIRONMENT` | `production`/`staging`/`test`/`development` | **required** |
| `ENABLE_AUDIT_LOGGING` / `ENABLE_API_DOCS` / `ENABLE_REQUEST_PROFILING` | feature flags | true / true / false |
| `HIPPIUS_BYPASS_CREDIT_CHECK` | credit-check bypass (**forced false unless `ENVIRONMENT=test`**) | false |
| `HIPPIUS_READ_ONLY_MODE` / `HIPPIUS_ENABLE_PUBLIC_READ` / `HIPPIUS_LIST_OBJECTS_SQL_ROLLUP` | operational levers | false / true / false |
| `HIPPIUS_SERVICE_ACCOUNT_IDS` | billing-exempt internal SS58 accounts (CSV, validated fmt 42, fail-closed) | empty |
| `HIPPIUS_ENABLE_BILLING_PLANS[_STAGING\|_PROD]` | billing-plans master switch (strict bool) | false |

#### Tuning knobs (uploader / unpinner / janitor / streaming / peer tier)
Large family; the operationally significant ones:

- **Uploader:** `HIPPIUS_UPLOADER_MAX_ATTEMPTS` (7), `_BACKOFF_BASE_MS` (500), `_BACKOFF_MAX_MS` (60000), `HIPPIUS_UPLOADER_MAX_INFLIGHT` (4), `HIPPIUS_ARION_UPLOAD_CONCURRENCY` (8), `HIPPIUS_UPLOADER_MULTIPART_MAX_CONCURRENCY` (5).
- **Unpinner:** `HIPPIUS_UNPINNER_MAX_INFLIGHT` (8), `HIPPIUS_UNPINNER_PARALLELISM` (5), `HIPPIUS_UNPINNER_MAX_ATTEMPTS` (5), `HIPPIUS_UNPINNER_BATCH_DELETE`/`_BATCH_MAX_FILES` (false / 1000).
- **Janitor (FS cache GC):** `HIPPIUS_JANITOR_CONCURRENCY` (32), `_WALK_CONCURRENCY` (8), `_WALK_BUDGET_SECONDS` (480), `_WALK_SHARDS` (64), `_ELEVATED_WALK_SHARDS` (8), `_SQL_PAGE_SIZE` (1000), `_SQL_MAX_DELETES_PER_CYCLE` (50000), `HIPPIUS_FS_CACHE_HOT_RETENTION_SECONDS` (14400), `HIPPIUS_JANITOR_CEPH_MGR_METRICS_URL`/`_CEPH_POOLS` (pool-fullness probe).
- **Streaming / read path:** `HIPPIUS_STREAM_FIRST_CHUNK_TIMEOUT_SECONDS` (25 — **must stay below the client read timeout**), `HIPPIUS_STREAM_CHUNK_TIMEOUT_SECONDS` (300), `HTTP_STREAM_PREFETCH_CHUNKS` (16), `HIPPIUS_CHUNK_SIZE_BYTES` (4194304), `HIPPIUS_READ_BACKEND_FETCH_CONCURRENCY` (32), `HIPPIUS_CRYPTO_POOL_WORKERS` (4).
- **Peer tier (node-local reads):** `HIPPIUS_PEER_FETCH_ENABLED` / `HIPPIUS_PEER_SERVE_ENABLED` (false; set true on `api-local`), `HIPPIUS_PEER_FETCH_TIMEOUT_SECONDS` (0.5), `_DEADLINE_SECONDS` (2.0), `_MAX_INFLIGHT`/`HIPPIUS_PEER_SERVE_MAX_INFLIGHT` (16), `HIPPIUS_OBJECT_CACHE_PROMOTE_ON_READ` (false; true on `api-local`), needs `NODE_NAME` + `POD_IP`.
- **can_upload billing gate (request path):** `CAN_UPLOAD_TIMEOUT_SECONDS` (3.0), `CAN_UPLOAD_TRANSIENT_RETRIES` (2), `CAN_UPLOAD_CACHE_TTL_SECONDS` (10).
- **Reapers / rollup / purger:** `HIPPIUS_MPU_REAPER_INTERVAL_SECONDS` (120), `HIPPIUS_MPU_REAPER_STATEMENT_TIMEOUT_SECONDS` (60), `HIPPIUS_USAGE_ROLLUP_LOOP_SLEEP` (5), `HIPPIUS_PURGER_*`, `HIPPIUS_REPLICATION_SLA_SECONDS` (900).
- **DLQ:** `HIPPIUS_DLQ_DIR` / `_ARCHIVE_DIR`, `HIPPIUS_DLQ_MAX_ENTRIES` (250000, cap protects the shared `redis-queues`).
- **FS cache dirs:** `HIPPIUS_OBJECT_CACHE_DIR` (`/var/lib/hippius/object_cache`), `HIPPIUS_OBJECT_CACHE_FALLBACK_DIR`.

#### Observability / status
| Var | Meaning | Default |
|---|---|---|
| `ENABLE_MONITORING` | master gate for OTel init (both Python and Rust) | false |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | collector OTLP gRPC endpoint | `http://otel-collector:4317` |
| `OTEL_EXPORTER_OTLP_PROTOCOL` | `grpc` | set in `start-worker.sh` |
| `OTEL_SERVICE_NAME` / `OTEL_RESOURCE_ATTRIBUTES` | per-service name / resource attrs (incl. `service.instance.id`) | per service |
| `SENTRY_DSN` / `SENTRY_TRACES_SAMPLE_RATE` | Sentry (empty DSN = no-op) | empty / 0.1 |
| `LOG_LEVEL` / `LOKI_URL` / `LOKI_ENABLED` | logging | INFO / — / false |
| `CACHET_API_URL` / `CACHET_API_KEY` / `CACHET_COMPONENT_ID` | status-page health push (all three required or skipped) | empty / — / 0 |
| `DISCORD_WEBHOOK_URL` | **dev-Grafana-only**; no code consumer (prod pages Mattermost) | — |
| `GF_SERVER_ROOT_URL` | Grafana root URL | `https://s3.hippius.com` |

#### Rust drain env surface (the `CEPHOR_*` family — the template for new Rust services)
Read by `crates/hippius-drain-{agent,allocator}/src/config.rs`. **Required (crash-loop if missing):** `CEPHOR_DATABASE_URL`, `CEPHOR_SSD_ROOT`, `CEPHOR_NODE_ID` (agent), `CEPHOR_ALLOCATOR_INSTANCE_ID` (allocator), `REDIS_QUEUES_URL`. All others have documented `DEFAULT_*` consts. Full enumerated set (from the crates):

`CEPHOR_{RECONCILE,DRAIN,ENQUEUE,EVICT,RECLAIM,REDRIVE,HEARTBEAT,ALLOCATION,ALLOCATOR_TICK,LANDED,UPLOAD_SWEEP,FAILED_RECLAIM}_POLL_SECS`, `CEPHOR_{DEFER_BACKOFF,DEFER_BACKOFF_CAP,GRACE,ORPHAN_RECLAIM_GRACE,RECLAIM_GRACE,STATUS_RETENTION,DECAY_HALF_LIFE,CLAIM_LEASE_TTL,LEADER_LEASE_TTL,HEARTBEAT_TTL,ALLOCATION_TTL}_SECS`, `CEPHOR_{DRAIN_CONCURRENCY,EVICT_BATCH,REDRIVE_MAX_ATTEMPTS,UPLOAD_REDRIVE_MAX_ATTEMPTS}`, `CEPHOR_{FLOOR_RATE,MAX_DRAIN_RATE}_BPS`, `CEPHOR_ALLOC_*` (AIMD: `INITIAL_TOTAL_BPS`, `MIN_TOTAL_BPS`, `MAX_TOTAL_BPS`, `ADDITIVE_INCREASE_BPS`, `DECREASE_PERMILLE`, `BASE_RESERVE_PERMILLE`, `CRITICAL_PRESSURE_BPS`, …), `CEPHOR_CEPH_{MGR_METRICS_URL,POOLS,NEARFULL_RATE_BPS,FULL_BPS,CEILING_BPS,NEARFULL_BPS,PROBE_TIMEOUT_SECS}`, `CEPHOR_EVICT_{RESERVE,HEADROOM}_PERMILLE`, `CEPHOR_{LIVENESS,READINESS}_FILE`, `RUST_LOG`, `ENABLE_MONITORING`, `OTEL_EXPORTER_OTLP_ENDPOINT`, `OTEL_RESOURCE_ATTRIBUTES`.

**Config-loader pattern worth copying into the rewrite** (`crates/hippius-drain-agent/src/config.rs`): `Config::from_env()` delegates to `Config::from_lookup(|k| std::env::var(k).ok())` so tests inject a fixture map instead of the process-global env; a typed `#[non_exhaustive] ConfigError` (thiserror) distinguishes `Missing` / `Invalid` / `NonPositive` / `BelowFloor` / `OutOfRange`; **present-but-unparsable is a loud error, missing is a documented default, zero rates are rejected.**

---

## 2. Process topology / pod & binary inventory

### 2.1 Images (all under `ghcr.io/thenervelab/hippius-s3/`)

| Image | Dockerfile | Base | Contents / entrypoint |
|---|---|---|---|
| `base` | `Dockerfile.base` | `python:3.11-slim` | `pip install -e .` — dep layer; **app images only COPY source, so a new dep needs `base` rebuilt** |
| `api` | `Dockerfile` | `${BASE_IMAGE}` | + dbmate; `CMD ["/start-api.sh"]` |
| `workers` | `workers/Dockerfile` | `${BASE_IMAGE}` | `CMD ["/start-worker.sh"]` (selected by `WORKER_SCRIPT`) |
| `drain` | `Dockerfile.drain` | multi-stage Rust (`rust:1.95.0-slim-bookworm` → `debian:bookworm-slim`) | **both** `hippius-drain-agent` + `hippius-drain-allocator`; `ENTRYPOINT ["/usr/bin/tini","--"]`, k8s picks via `command:` |

### 2.2 The API process (uvicorn)

`start-api.sh` runs `python -m hippius_s3.scripts.migrate` then `exec uvicorn ... --factory hippius_s3.main:factory`. The verbatim invocation:

```
exec uvicorn \
    --host=$UVICORN_HOST --port=$UVICORN_PORT \   # 0.0.0.0 : 8000
    --workers=$UVICORN_WORKERS \                   # default 1; k8s configmap sets 4
    --loop=uvloop --log-level=$UVICORN_LOG_LEVEL --access-log \
    --timeout-graceful-shutdown="$UVICORN_GRACEFUL_TIMEOUT" \  # 25
    --timeout-keep-alive="$UVICORN_KEEP_ALIVE" \               # 75
    --factory $RELOAD_FLAG hippius_s3.main:factory
```

Load-bearing details the Rust rewrite must reproduce (comments in `start-api.sh`):

- **`exec` is mandatory** — uvicorn becomes PID 1 so the kubelet's SIGTERM reaches it and it drains in-flight requests. Guarded by `tests/unit/test_start_scripts_exec.py`. (Rust: use `tini` as PID 1, as the drain image does, or handle SIGTERM directly.)
- **`--timeout-keep-alive=75` must stay above the edge pool keepalive** (ATS/HAProxy, peer fetchers). The server closing a pooled socket first turns an idle connection into a failed request. This is the single most important uvicorn-parity knob (see §6).
- **`--timeout-graceful-shutdown=25 < terminationGracePeriodSeconds (45) − preStop sleep (10)`** or the kubelet SIGKILLs mid-drain.
- **Worker recycling OFF** (`UVICORN_MAX_REQUESTS=0`): recycling churns a worker while the pod stays Ready → the Service routes into the restart window → 502.

### 2.3 The worker launcher

Every worker container runs the same `workers` image and `start-worker.sh`; **`WORKER_SCRIPT` is the sole selector** (script exits 1 if unset). It exports OTLP env (`OTEL_EXPORTER_OTLP_PROTOCOL=grpc`, endpoint default `http://otel-collector:4317`, `OTEL_SERVICE_NAME` default `hippius-worker`) then, gated on `ENABLE_MONITORING`, wraps with `opentelemetry-instrument ... python "$WORKER_SCRIPT"` (or plain `exec python`). `ENABLE_WATCHFILES=true` adds dev hot-reload.

Worker scripts (`workers/`):

| `WORKER_SCRIPT` | Role |
|---|---|
| `run_arion_uploader_in_loop.py` | upload chunks to Arion; BRPOP `arion_upload_requests` (or `:<node>` when `NODE_NAME` set) |
| `run_arion_unpinner_in_loop.py` | delete chunks from Arion; `arion_unpin_requests` |
| `run_janitor_in_loop.py` | FS cache eviction/GC on `object_cache`; Ceph-pool-fullness gated |
| `run_account_cacher_in_loop.py` | warm credit cache into `redis-accounts` |
| `run_plans_cacher_in_loop.py` | scrape billing-plan roll; reads `DATABASE_READONLY_URL`; **replicas must stay 1** |
| `run_usage_rollup_in_loop.py` | compact `storage_delta_ledger`→`bucket_storage_usage` + reconcile; writes primary; **replicas must stay 1** |
| `run_purger_in_loop.py` | purge suspended-account data |
| `run_orphan_checker_in_loop.py` | detect chain orphans, enqueue unpin |
| `run_mpu_reaper_in_loop.py` | reap abandoned multipart uploads; mark drain rows terminal |
| `run_migrator_once.py` | one-shot object storage-version migration (see §5.5) |
| `cachet_health_check.py` | status-page health push (probes `gateway:8080/health`) |

### 2.4 Production pod / process inventory (k8s)

This is the inventory a Rust rewrite's binary/pod layout must mirror. Fields drawn from `k8s/base/*.yaml` + `k8s/production/*.yaml`.

| # | Workload | Kind | Replicas / placement | Command / selector | Resources (req → lim) | Health probe |
|---|---|---|---|---|---|---|
| 1 | **api-local** | DaemonSet | node1–5 (`s3-prod-local-ingest=true`), `priorityClassName: s3-ingest-priority`, `maxUnavailable:1` | uvicorn 4 workers, :8000 | 1000m/2Gi → 4000m/8Gi | httpGet `/health`:8000 (startup/live/ready) |
| 2 | api | Deployment | **scaled to 0 at cutover** | uvicorn | 1000m/2Gi → 4000m/8Gi | httpGet `/health`:8000 |
| 3 | arion-uploader | Deployment | 10 (prod) | `run_arion_uploader_in_loop.py` (**global** queue) | 1000m/2Gi → 2000m/4Gi | exec python-cmdline |
| 4 | **arion-uploader-local** | DaemonSet | node1–5 | same script + `NODE_NAME` → BRPOP `arion_upload_requests:<node>` | 500m/1Gi → 2000m/4Gi | exec python-cmdline |
| 5 | arion-unpinner | Deployment | 3 | `run_arion_unpinner_in_loop.py` | 500m/1Gi → 2000m/1Gi | exec |
| 6 | janitor | Deployment | 1 | `run_janitor_in_loop.py` | 250m/512Mi → 1000m/3Gi | exec |
| 7 | account-cacher | Deployment | 1 | `run_account_cacher_in_loop.py` | 250m/512Mi → 1000m/2Gi | exec |
| 8 | plans-cacher | Deployment | **1 (must stay 1)** | `run_plans_cacher_in_loop.py` | 100m/256Mi → 500m/512Mi | exec |
| 9 | usage-rollup | Deployment | **1 (must stay 1)** | `run_usage_rollup_in_loop.py` | 50m/128Mi → 500m/512Mi | exec |
| 10 | purger | Deployment | 1 | `run_purger_in_loop.py` | 250m/512Mi → 1000m/2Gi | exec |
| 11 | cachet-health-checker | Deployment | 1 | `cachet_health_check.py` | 50m/64Mi → 100m/128Mi | exec |
| 12 | mpu-reaper | Deployment | 1 (singleton) | `run_mpu_reaper_in_loop.py` | 50m/128Mi → 500m/512Mi | exec |
| 13 | **drain-allocator** | Deployment | **1 singleton, `Recreate`, leader-elected** — owns `cephor_*` schema (deploy first) | `/usr/local/bin/hippius-drain-allocator` | 50m/64Mi → 250m/256Mi | exec: `/tmp/hippius-drain-allocator.alive` mtime<30s |
| 14 | **drain-agent** | DaemonSet | node1–5 | `/usr/local/bin/hippius-drain-agent` | 100m/128Mi → 1000m/1Gi | live: `.alive`<40s; ready: `.ready`<45s |
| 15 | otel-collector | Deployment | 1 | `otel/opentelemetry-collector-contrib:0.96.0` | 250m/512Mi → 1/2Gi | httpGet `/`:13133 |
| 16 | redis-{accounts,queues,rate-limiting,acl} | StatefulSet | 1 each (+ external `redis` cache ExternalName) | `redis:7-alpine` | see §2.6 | redis-cli ping |
| 17 | postgres | CNPG cluster | `postgres-nvme-*` (prod) / `postgres-rw` (base) | — | — | CNPG-managed |
| 18 | db-migrations | Job | one-shot, `backoffLimit:3`, `ttl:7200` | `python -m hippius_s3.scripts.migrate` (image `api`) | — | — |

**Ingest-node topology (drain-direct):** nodes `k8s-v3-node1..node5` carry a dedicated NVMe at hostPath `/s3-data` (`type: Directory` — pod won't start if unmounted), mounted as `/var/lib/hippius/local_object_cache`. On each such node the co-located triple is **api-local (writes parts to SSD) + drain-agent (reconciles SSD, copies to pool, publishes `arion_upload_requests:<node>`) + arion-uploader-local (drains that node queue)**. `NODE_NAME`/`CEPHOR_NODE_ID` must match on both sides — a mismatch announces to a queue nothing drains (`docker-compose.e2e.yml:20-27`).

### 2.5 Services & routing

- `api` Service: selector `app: api`, `trafficDistribution: PreferSameNode`, 8000→8000.
- `gateway` Service (alias kept through the gateway/api merge): selector `app: api`, 8080→8000.
- **At cutover** both Service selectors are JSON6902-patched to `app: api-local` (gateway also requires `hippius.io/edge: "true"`), and the base `api` Deployment is scaled to 0.
- `redis` Service is an `ExternalName` → `redis.redis.svc.cluster.local` (separate namespace).

### 2.6 Redis topology

Four single-replica StatefulSets in `k8s/base/redis-statefulsets.yaml` (all `redis:7-alpine`, headless Service `clusterIP: None`:6379, CephFS PVC):

| StatefulSet | maxmemory / policy | req → lim |
|---|---|---|
| redis-accounts | save 60/300/900, AOF everysec | 100m/2Gi → 1000m/4Gi |
| **redis-queues** | `--maxmemory 4gb --maxmemory-policy noeviction`, AOF everysec | 100m/4Gi → 1000m/6Gi |
| redis-rate-limiting | `--maxmemory 1gb` | 50m/1Gi → 500m/2Gi |
| redis-acl | maxmemory 2gb, allkeys-lru, no AOF | 100m/2Gi → 1000m/4Gi |

An HA variant (`k8s/base/redis-queues-ha.yaml`, opstree RedisReplication clusterSize 3 + RedisSentinel clusterSize 3, quorum 2, image `quay.io/opstree/redis:v7.4.8`) is **staged but not wired into any kustomization**; it is cut over by runbook (§5.4).

---

## 3. Observability contract

The Rust rewrite must be telemetry-compatible: same OTLP transport, same metric names + bounded labels, same health endpoints. Everything is **pushed** — the app exposes **no `/metrics`** endpoint; the otel-collector's Prometheus exporter (`:8889`) is what Prometheus scrapes.

### 3.1 OTel wiring (from `hippius_s3/otel_setup.py`)

- **Transport: OTLP over gRPC, `insecure=True`, to `OTEL_EXPORTER_OTLP_ENDPOINT` (default `http://otel-collector:4317`).**
- **Metric export interval: 10 s** — `PeriodicExportingMetricReader(..., export_interval_millis=10000)`. The Rust drain matches this exactly (`EXPORT_INTERVAL = Duration::from_secs(10)`, `PeriodicReader`).
- **`service.instance.id = f"{socket.gethostname()}:{os.getpid()}"`** (`otel_setup.py:74`). This is **load-bearing**: without a per-process identity the multi-worker counters collide in the collector (historically `http_requests_total` logged 260k resets/h). The collector's `resource_to_telemetry_conversion.enabled: true` turns it into a distinguishing Prometheus label. **The Rust rewrite must give each process a unique `service.instance.id` (host:pid or pod:pid).**
- **`service.name`** is per-service; the collector's `resource` processor uses `action: insert` (not upsert), so a producer's own `service.name` survives. The Rust drain sets its own (`hippius-drain-agent`/`-allocator`).
- **Gate:** all OTel init is skipped unless `ENABLE_MONITORING ∈ {true,1,yes}`.
- **Auto-instrumentations (Python):** FastAPI, Redis, asyncpg, HTTPX. Logs go to **Loki via Promtail**, not OTel (`OTEL_PYTHON_LOGGING_AUTO_INSTRUMENTATION_ENABLED=false`).
- **Histogram buckets (SECONDS, must be reproduced):**
  - `http_request_duration_seconds`: `(0.01,0.025,0.05,0.1,0.25,0.5,1.0,2.5,5.0,10.0,30.0,60.0,120.0,300.0,600.0)`
  - `http_request_ttfb_seconds`, `http_pre_handler_duration_seconds`: `(0.005,0.01,0.025,0.05,0.1,0.25,0.5,1.0,2.5,5.0,10.0,30.0)`

**Collector config** (`k8s/base/otel-collector.yaml` + `otel-config.yaml`): OTLP receivers gRPC `:4317` / HTTP `:4318`; `memory_limiter` (768 MiB) → `resource` (insert `service.name`) → `batch` (1s / 1024) → exporters `prometheus :8889` (`resource_to_telemetry_conversion.enabled`, `metric_expiration:180m`, `enable_open_metrics`), `otlp/tempo` (traces), `loki` (logs); `health_check` on `:13133`. The ConfigMap is content-hashed by kustomize `configMapGenerator` so a config change rolls the pod (it reads config once at startup). Scraped via `ServiceMonitor` `hippius-s3-otel` (`k8s/base/servicemonitor.yaml`, port `prometheus`, interval 30s, path `/metrics`).

### 3.2 Metric names + labels the dashboards/alerts depend on

Authoritative registry: `hippius_s3/monitoring.py`. **Cardinality rule (must hold in Rust):** every attribute becomes a Prometheus label — keep them bounded; **never** account ids, bucket names, object keys, paths, or error strings as labels (those go on spans). High-cardinality identity goes on trace span attrs: `hippius.account.main`, `hippius.account.sub`, `aws.s3.bucket`, `aws.s3.key`.

The Rust rewrite of the API/workers must emit these exact names/labels (abridged to the contract set):

- **HTTP/request path:** `http_requests_total{method,handler,status_code}`, `http_request_duration_seconds` (dur buckets), `http_request_ttfb_seconds` (ttfb buckets), `http_pre_handler_duration_seconds{method,status_code}` (ttfb buckets), `http_request_bytes_total{...,direction="in"}`, `http_response_bytes_total{...,direction="out"}`, `gateway_overhead_seconds`. `handler` falls back to `"unknown"`, **never the path**.
- **S3/data:** `s3_operations_total{operation,success}`, `s3_errors_total{error_type,operation}`, `s3_bytes_uploaded_total{operation}`, `s3_bytes_downloaded_total{operation}` (+ legacy aliases `gateway_bytes_received_total`/`gateway_bytes_sent_total`).
- **Cache/auth:** `cache_hits_total`/`cache_misses_total{operation}`, `auth_cache_hits_total`/`_misses_total`, `seed_auth_cache_*`.
- **SSD read-tier / drain-direct (closed-set Literal labels):** `chunk_reads_by_tier_total{tier=local|peer|pool|backend}`, `peer_fetch_shed_total{reason=…}`, `promotion_skipped_total{reason=disk_pressure|residency_failed}`, `chunk_aead_failures_total{tier,outcome}`, `landed_announce_failures_total{outcome}`, `fs_cache_shed_total{reason,pressure_mode}`.
- **Billing/plans:** `billing_bypass_total{surface}`, `plan_gate_total{outcome=allow|deny|catalog_miss|unavailable|shadow_*}` (plan_id **not** a label), `plans_cache_age_seconds` (gauge).
- **Storage-usage rollup:** `storage_rollup_drift_bytes` (hist), `storage_rollup_{reconciled,compacted_rows,drifted_buckets}_total`, gauges `storage_rollup_ledger_depth`/`_ledger_lag_seconds`/`_negative_buckets`.
- **Uploader/unpinner:** `uploader_requests_total{success,backend,status_code}`, `uploader_dlq_total{error_type,backend}`, `uploader_duration_seconds`, and the parallel `unpinner_*`.
- **Worker loops:** `mpu_reaper_*`, `purger_*`, `orphan_checker_*`, `account_cacher_*`, `backup_*`.
- **DLQ:** `dlq_pushed_total{queue,error_type}`, `dlq_requeued_total{queue}`, `dlq_dropped_total{queue,error_type}`.
- **Cachet:** `cachet_health_checks_total{status}`, `cachet_updates_total{success}`.
- **Observable gauges:** `redis_memory_used_bytes`, `redis_memory_max_bytes`, `hippius_queue_length{queue_name}`, `db_pool_size`/`_free_connections`/`_used_connections`; infra gauges `fs_cache_disk_used_bytes`, `fs_cache_pressure_mode`, `fs_cache_hot_parts`, `fs_store_parts_on_disk`, `fs_store_oldest_age_seconds`.
- **Drain (Rust, already emitted):** `drain_parts_replicated_total`, `drain_ssd_backlog_bytes`, `drain_ssd_pressure`, `drain_ssd_free_bytes`, `drain_breaker_open`, `drain_pending_oldest_age_seconds`; allocator `drain_leader`, `drain_leader_epoch`, `drain_fleet_estimate_bps`.

**Alert-query invariants that constrain emission** (`docs/grafana-alerting.md`):
- `hippius_queue_length` must be aggregated with `max()` not `sum()` — all api pods export the same LLEN under identical labels.
- `noDataState: OK` on every otel-sourced rule (a collector blackout must not storm).
- Prod alert **rules live in the external `thenervelab/hippius-otel` repo** (`alerting/rules/hippius-s3.yaml`) and page **Mattermost**; 24 rules (10 critical / 14 warning / 5 `outage:"true"`), 8 of which are the s3-2.1 drain/Ceph/ingest additions (e.g. `S3CephPoolFull`, `S3DrainAgentMissing`, `S3DrainBreakerOpen`, `S3DrainAllocatorNotLeading`). This repo's `monitoring/grafana/provisioning/alerting/` is **local-dev only**.

### 3.3 Sentry (`hippius_s3/sentry.py`)

`init_sentry(service_name, *, is_worker=False)`: no-op if `SENTRY_DSN` empty; `environment=$ENVIRONMENT` (default `development`); `traces_sample_rate=$SENTRY_TRACES_SAMPLE_RATE` (default 0.1); `server_name=service_name`; `send_default_pii=False`; integration is `AsyncioIntegration` (workers) or `FastApiIntegration` (services). Rust equivalent: `sentry` + `sentry-tracing` crates, same DSN/env/sample-rate contract.

### 3.4 Health, Cachet, Discord

- **App health: `GET /health` → `{"status":"healthy"}` (200)** (`hippius_s3/main.py:617`). No `/healthz`/`/readyz`/`/livez` in the app. `/health` and `/metrics` are bypassed by every middleware (read-only, ACL, suspension, audit, tracing) and suppressed from access logs. k8s api probes are `httpGet /health :8000`.
- **Cachet:** `workers/cachet_health_check.py` loops every 60 s: `GET http://gateway:8080/health` → Cachet status `1` operational / `3` partial / `4` major → `PUT {CACHET_API_URL}/api/components/{id}` with `Authorization: Bearer`. Skipped unless all three Cachet vars set. Emits `cachet_health_checks_total` / `cachet_updates_total`.
- **Discord:** injected as a pod env var but **has no code consumer** — it is the local-dev Grafana→Discord sink only. Prod alerting is Mattermost via hippius-otel. The rewrite emits nothing for Discord.
- **Rust services' health = file-freshness + k8s `exec` probes** (no HTTP server): the drain touches `/tmp/hippius-drain-agent.alive` each heartbeat (liveness) and `.ready` while progressing (readiness `ReadinessTracker` reports NotReady when there is undrained work but the processed counter hasn't advanced within a stall window). Configurable via `CEPHOR_LIVENESS_FILE`/`CEPHOR_READINESS_FILE`. **A rewritten Rust API, by contrast, serves real HTTP and should expose `GET /health` returning `{"status":"healthy"}` for compatibility with the existing Service probes.**

---

## 4. Build & CI

### 4.1 Image build

- **Python images:** `base` built from `Dockerfile.base`; `api`/`workers` `FROM ${BASE_IMAGE}` and COPY source. Prod deploy builds `base` first, exports its SHA tag, then builds `api`+`workers` via a matrix (`.github/workflows/production-deploy.yaml`, `matrix.service: [api, workers]`).
- **Rust `drain` image:** `Dockerfile.drain`, self-contained 2-stage build (no base image):

```
FROM rust:1.95.0-slim-bookworm AS builder
COPY rust-toolchain.toml ./          # pins toolchain
COPY Cargo.toml Cargo.lock ./ ; COPY crates ./crates
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/build/target \
    cargo build --release --locked -p hippius-drain-agent -p hippius-drain-allocator \
    && cp target/release/hippius-drain-agent target/release/hippius-drain-allocator /usr/local/bin/
FROM debian:bookworm-slim AS runtime
RUN apt-get install -y ca-certificates tini
COPY --from=builder /usr/local/bin/hippius-drain-agent /usr/local/bin/hippius-drain-allocator /usr/local/bin/
ENTRYPOINT ["/usr/bin/tini","--"]
CMD ["/usr/local/bin/hippius-drain-agent"]
```

**This is the template for new Rust service images:** BuildKit cache mounts, `--locked`, one image carrying multiple binaries selected by k8s `command:`, `tini` PID 1, `ca-certificates`, no TLS dev libs (reqwest is TLS-less; sha2/nix pure-Rust). New Rust services (e.g. `rust-api`) should follow the same shape; if the API needs TLS to Arion, add the TLS dev deps and feature.

### 4.2 CI structure

`.github/workflows/test-and-lint.yml` (push to main/staging, PRs). Python jobs: `lint-and-format` (ruff), `type-check` (ty), `dep-audit` (pip-audit), `unit-integration` (postgres+redis services, dbmate migrations, `pytest tests/{unit,integration}`), `e2e` (see §4.3). **Rust jobs (the template):**

- **`rust`** — services `postgres:17` + `redis:7-alpine`; env `DATABASE_URL`, `CEPHOR_TEST_REDIS_URL`; toolchain via `dtolnay/rust-toolchain@master` pinned `1.95.0` (clippy+rustfmt), `Swatinem/rust-cache@v2`; steps:
  - `cargo fmt --all -- --check`
  - `cargo clippy --workspace --all-targets --all-features -- -D warnings`
  - `cargo test --workspace --all-features --locked -- --include-ignored` (runs redis-gated `#[ignore]`d coordination tests; `#[sqlx::test]` cases auto-create per-test DBs)
- **`rust-deny`** — `cargo deny check` (advisories + licenses + bans + sources; supply-chain gating is via `cargo-deny`, not `cargo audit`).

Deploy workflows (`production-deploy.yaml` / `staging-deploy.yaml`) add:
- **`rust-gate`** — the fast pre-image gate (fmt + clippy + cargo-deny) that **blocks `build-drain` and thus the deploy**.
- **`build-drain`** (`needs: rust-gate`) — `docker/build-push-action@v5`, `file: ./Dockerfile.drain`, tags `type=sha,format=short` + `latest`, `cache-from/to: type=gha`.
- **`deploy-*`** — `kustomize edit set image .../{api,workers,drain}:${SHORT_SHA}`, `kubectl apply -k k8s/{production,staging}`, then rollout gating: **allocator-first**, then drain-agent DaemonSet (≥1 Ready), then api/api-local. Cross-env `concurrency.group` serializes staging vs prod against the shared cluster.

**Toolchain/lint config files (the template):** `rust-toolchain.toml` (`channel="1.95.0"`, minimal profile), `rustfmt.toml` (`max_width=150`, `edition="2024"`), `deny.toml` (`advisories: yanked=deny`; license allowlist MIT/Apache-2.0/BSD/ISC/Unicode-3.0/Zlib; `bans: wildcards=deny`; `sources: unknown-registry/git=deny`). Workspace lints forbid `unsafe_code` and deny `unwrap_used`/`panic`/`todo`/`print_std*`/`await_holding_lock`/`exit`.

### 4.3 E2E harness (`docker-compose.e2e.yml`)

CI `e2e` job: `docker compose --profile build-base -f docker-compose.yml -f docker-compose.e2e.yml build base` → `... build` → `pytest tests/e2e --maxfail=1`. The stack brings up: `api` (drain-direct topology, writes to node-local SSD), the worker set (`arion-uploader`, `arion-unpinner`, `purger`, `account-cacher`, `plans-cacher`, `janitor`), the five redis instances, the **mocks** (`mock-arion`, `mock-kms`, `mock-hippius-api`), `toxiproxy` (fault injection on the Arion hop), and the **full Rust drain stack** (`drain-allocator` + `drain-agent`, both from `Dockerfile.drain`, with tight `CEPHOR_*_POLL_SECS` and effectively-unlimited `CEPHOR_*_RATE_BPS` so parts replicate inside test windows). The `drain-agent`'s `CEPHOR_NODE_ID` must equal the api's `NODE_NAME` (`e2e-node`).

`tests/smoke/` is the **remote/staging-aware** suite (endpoint-driven via `HIPPIUS_ENDPOINT`, real SigV4). It is the de-facto staging E2E but currently `workflow_dispatch`-only (`docs/staging-e2e-testing-state.md`); the recommended gate is a `smoke-staging` job chained on the deploy. **For the Rust rewrite, `tests/smoke/` is the correct black-box conformance harness** — it is client-driven and language-agnostic, so it validates a Rust API against the same contract as the Python one. `tests/e2e/` is not portable (hardcoded `localhost`, toxiproxy, direct-DB asserts).

---

## 5. The CUTOVER playbook

### 5.1 The reusable template distilled from `docs/drain-direct-rollout.md`

The drain-direct cutover is the canonical example of how this system does a **hard, no-feature-flag cutover of a producer** and is the template for the Python→Rust migration. Its mechanics:

1. **No feature flag, no fallback mode. A hard cut, made safe by deploy ORDER, not a toggle.** The new producer (drain) was deployed **first**; the old producer (api PUT-enqueue) stopped **second**. Order matters because the reverse opens a correctness gap (a part the old drain marks `replicated` during the window is never enqueued).
2. **Overlap is tolerated because the consumer is idempotent.** During the window both old and new producers may enqueue the same part — harmless because the uploader is idempotent (`skip_if_exists` + `chunk_backend ON CONFLICT`). **Idempotent consumers are the precondition that makes a flagless dual-run safe.**
3. **A read-only backstop query to detect and re-drive stragglers.** Before/after cut, a read-only SQL query enumerates `replicated`-but-unenqueued parts; re-drive by resetting to `pending` so the single authoritative producer (the drain) re-claims — **prefer re-driving through the new path over hand-enqueuing.**
4. **Constants pinned identically on both sides.** The backend set is pinned in code in both Python (`config.py STORAGE_BACKENDS`) and Rust (`config.rs`) so both producers stamp identical requests. **Cross-language constant parity is a cutover invariant.**
5. **Rollout is per-environment, staging first, prod after the gating test passes** (see the redis HA runbook's explicit gating-test discipline, §5.4).

### 5.2 Adapting it into a phased Python→Rust migration (API/workers)

The Rust rewrite is riskier than the drain (the drain was a single producer with an idempotent consumer; the API is the request path). Apply the same primitives — **order over flags, idempotent overlap, read-only verification, per-node/per-bucket blast-radius control, read-compat-first** — but in graduated phases with fast rollback at each step.

**Phase 0 — Read-compatibility & shadow (no client-visible change).**
- Build the Rust services to read the **same config surface** (§1) and the **same schema** (dbmate app schema + drain `cephor_*` schema; the Rust API must not own migrations — leave dbmate authoritative, gate on `wait_for_migrations.py` equivalent).
- Emit the **same metrics/labels/health** (§3) so dashboards/alerts work against Rust pods unchanged.
- Stand a Rust API pod on an ingest node behind **no Service selector** (or a shadow Service). Mirror a copy of read traffic (or use `tests/smoke/` continuously) and diff responses against Python for byte-parity on GET/HEAD/list. **Reads first, because a read bug is recoverable and a write bug corrupts durable state.**

**Phase 1 — Per-node canary (reads).** Because `api-local` is a **DaemonSet**, a node is the natural blast-radius unit. Drain one ingest node's Python `api-local` pod and run the Rust API pod there instead (same hostPath SSD, same `NODE_NAME`, `trafficDistribution: PreferSameNode` keeps that node's clients on it). Watch the SSD read-tier metrics (`chunk_reads_by_tier_total`, `chunk_aead_failures_total`) and 5xx/latency on that instance's `service.instance.id`. Roll back = restore the Python pod on that node (seconds; no data migration — the SSD and DB are shared).

**Phase 2 — Per-node writes, with the drain as the idempotent safety net.** Enable PUT on the Rust node. Writes land on the same node-local SSD; the **existing Rust drain** reconciles and replicates them regardless of which language wrote them (the drain reads the SSD + DB, not an API-internal path). This is the drain-direct idempotency property reused: the Rust API and Python API can both produce parts on different nodes simultaneously and the single drain fleet consumes both. Verify with the drain-direct backstop query (§5.1.3) that every part the Rust node landed reaches `chunk_backend` coverage.

**Phase 3 — Per-bucket / per-account gating (optional finer grain).** If node-granularity is too coarse for a risky endpoint (e.g. MPU-complete, CopyObject, versioning), gate that endpoint's Rust path on an allowlist read from config (an `HIPPIUS_RUST_*` flag family), defaulting closed — the strict-bool pattern (`_parse_bool`) so a typo fails loud. This is the one place a flag is warranted: an endpoint-level, per-account canary is finer than the DaemonSet can express.

**Phase 4 — Fleet cut & Python scale-to-0.** Once every ingest node runs Rust `api-local` and smoke + alerts are clean for a soak window, patch the Service selectors (already `app: api-local`) and scale any remaining Python `api` Deployment to 0 — the identical move the drain-direct cutover made (base `api` → replicas 0). **Keep the Python image deployable (scaled 0, not deleted)** for 24–48 h as the rollback target.

**Workers** migrate independently and more cheaply: each is a single `WORKER_SCRIPT` process behind an idempotent, retry-safe queue. Migrate one worker at a time by swapping its Deployment's image+command to the Rust binary (the `drain` image pattern: one Rust image, `command:` selects the worker). The singletons (`plans-cacher`, `usage-rollup`, `mpu-reaper`, `drain-allocator`) must **stay at replicas 1 / `Recreate`** through the swap — never run a Python and Rust instance of a singleton concurrently.

### 5.3 Cutover-phases summary

| Phase | Unit | Traffic | Rollback | Gate to advance |
|---|---|---|---|---|
| 0 Read-compat + shadow | cluster | none (shadow) | delete shadow pod | byte-parity on GET/HEAD/list; metrics/health emit correctly |
| 1 Read canary | one node | that node's reads | restore Python pod (seconds) | clean 5xx/latency/AEAD on the node's `service.instance.id`, soak |
| 2 Write canary | one node | that node's writes | restore Python pod | backstop query shows full `chunk_backend` coverage; drain healthy |
| 3 Endpoint gating | per-bucket/account (flag) | risky endpoints | flip flag closed | endpoint parity for the allowlisted set |
| 4 Fleet cut | all nodes | all | scale Python `api` back from 0 | smoke green + alerts clean over soak (24–48 h) |
| Workers | one worker | that worker's queue | restore Python Deployment | queue drains, DLQ flat, singleton never doubled |

### 5.4 Prior migration mechanics to reuse: `redis-queues-ha-cutover.md`

The redis HA runbook adds primitives worth reusing for any stateful cutover:

- **Stand the new thing up non-disruptively first (Phase 0), leave the old running throughout, cut over by repoint (Phase 3), decommission only after soak.** Rollback is a repoint back — instant, no data loss because both sides are durable.
- **A GATING TEST on staging before prod** (Phase 1): kill the master, assert the invariant (`cephor:epoch` monotonic, `leader_count==1`, no lost upload); **if it fails, STOP — do not proceed to prod.** The Rust cutover's analog is the Phase-2 backstop-coverage check and the Phase-1 soak.
- **Data seeding gotcha:** the obvious `replicaof`+promote FAILED because the operator flushed the seed; the working method was a logical `MIGRATE COPY REPLACE` of keys. Lesson: **an operator/controller fights manual topology changes; use ordinary client-level operations it won't reconcile away.**
- **The durable source of truth vs. the live patch:** `REDIS_QUEUES_URL` is a **plaintext literal in the deploy workflow** (`kubectl create secret ... --from-literal=...`), recreated on every deploy — a live `kubectl patch` is silently reverted on the next deploy. **Any config change must be made in the deploy workflow literal (durable) AND applied live (immediate), or it evaporates.** The prod secret is created wholesale in `production-deploy.yaml`'s "Update secrets" step; the Rust rewrite's new env vars must be added there too.

### 5.5 Object migration mechanics (`docs/object-migration.md`)

Storage-layout migration is a separate, already-built mechanism the Rust rewrite inherits rather than replaces: it selects objects below `HIPPIUS_TARGET_STORAGE_VERSION`, creates an advisory-locked `version_type='migration'` version, streams+re-encrypts per part, and CAS-swaps `objects.current_object_version` atomically on success (failed versions are marked and swept by a separate cleanup with an age guard). It runs as a worklist-driven CLI (`workers/run_migrator_once.py` / `hippius_s3/scripts/migrate_objects.py`, `--resume`, `--state-file`, `--concurrency`) or the `migrator/` container. The relevant OPS property: **it is idempotent, resumable, and abort-safe (writer gates refuse work on a `failed` version)** — the same properties that make a language cutover safe. A Rust reimplementation must preserve the CAS-swap-on-finalize + cleanup-is-sole-unpin-authority contract.

---

## 6. Rust implementation notes

### 6.1 HTTP server & uvicorn-parity tuning (new work for the API rewrite)

The drain has **no HTTP server**; a Rust API does. Use **axum** (on hyper/tokio). Reproduce the uvicorn knobs that are load-bearing here:

- **Keep-alive header timeout must stay above the edge pool keepalive (75 s in Python).** hyper's default idle keep-alive is effectively unbounded per connection but the graceful-shutdown and header-read timeouts matter; configure the connection so the **server does not close a pooled socket before the ATS/HAProxy edge does** (the exact failure `start-api.sh` warns about). Set an explicit keep-alive/idle policy ≥ the edge's.
- **Body backpressure / high-water:** the Python code widens uvicorn's per-connection body buffer from 64 KiB to ~1 MiB (`HIPPIUS_UVICORN_HIGH_WATER_LIMIT`, `uvicorn_tuning.raise_receive_high_water`) so a streaming PUT rides through the handler's off-loop hops instead of pausing every 64 KiB. In axum/hyper, stream the request body (`axum::body::Body` / `http-body`) without buffering and size the read granularity to ~1 MiB; do not collect the body. Peak memory ≈ chunk size × in-flight uploads — bound it the same way the config does (`HIPPIUS_WRITE_QUEUE_MAXSIZE`, `HIPPIUS_WRITE_PIPELINE_LOOKAHEAD`).
- **Worker model:** uvicorn runs `UVICORN_WORKERS=4` OS processes (per-process asyncpg pools, per-process `service.instance.id`). Tokio is a single multi-threaded runtime in one process, so the Rust API is **one process with a work-stealing runtime** — but it must still budget its `sqlx`/`deadpool` connection pool against `max_connections=1000` accounting for pod count (no longer ×4 per pod). Give the process a unique `service.instance.id` (pod:pid) all the same, and **do not use in-process request recycling** (the 502-churn reason is gone with a single long-lived process, but the config default `UVICORN_MAX_REQUESTS=0` documents the intent).
- **Crypto off the runtime:** the Python path runs AES-GCM on a dedicated thread pool (`HIPPIUS_CRYPTO_POOL_WORKERS`). In Rust, run per-chunk encrypt/decrypt on `tokio::task::spawn_blocking` (or a rayon pool) so the async runtime is never blocked — this matches the CLAUDE.md "never block the runtime" rule.

### 6.2 Tracing / OpenTelemetry crates (must match the collector)

Use the exact stack the drain already validated against this collector (all `=`-pinned in the workspace `Cargo.toml`):

- `opentelemetry = "0.31"` (`metrics`), `opentelemetry_sdk = "0.31"` (`metrics`, `rt-tokio`), `opentelemetry-otlp = "0.31"` (`grpc-tonic`, `metrics`) — **OTLP/gRPC via tonic to `:4317`**, NOT http-proto.
- `tracing = "0.1"`, `tracing-subscriber = "0.3"` (`env-filter`, honors `RUST_LOG`).
- Metric provider setup mirrors `crates/hippius-drain-agent/src/metrics.rs`: `MetricExporter::builder().with_tonic().with_endpoint(env OTEL_EXPORTER_OTLP_ENDPOINT)`, `PeriodicReader` at **10 s**, `Resource` with `service.name` + a unique `service.instance.id`, `global::set_meter_provider(...)`, gated on `ENABLE_MONITORING`, exporter-build failure logged non-fatal.
- **For the API specifically (unlike the drain), also export traces and request-path histograms** with the exact bucket boundaries from §3.1 (opentelemetry supports explicit-bucket histogram views). Add Sentry via `sentry` + `sentry-tracing` matching §3.3.
- **Do NOT add a Prometheus `/metrics` endpoint** — the contract is push-to-collector; the collector owns the scrape surface.

### 6.3 Structured shutdown (copy the drain's supervisor)

Reproduce `crates/hippius-drain-agent/src/supervisor.rs`:

- `#[tokio::main]` returning `ExitCode` (`std::process::exit` is lint-denied).
- A root `tokio_util::sync::CancellationToken`; each task gets `root.child_token()`; tasks in a `JoinSet`.
- `shutdown_signal()` = `tokio::select!` over SIGINT + SIGTERM (`SignalKind::terminate()` — what k8s sends); a failed signal install parks on `pending()` so it can never fake a shutdown.
- On signal: `root.cancel()`, then `timeout(grace, drain tasks)`, then `abort_all()` for stragglers. Map an unclean drain / unexpected worker exit to `ExitCode::FAILURE` (k8s restarts + alerts); clean SIGTERM → `SUCCESS`.
- Grace default 30 s (`CEPHOR_GRACE_SECS`), and the pod's `terminationGracePeriodSeconds` (40) is deliberately larger. **For the API, the grace must be < `terminationGracePeriodSeconds − preStop sleep`** (the uvicorn 25 < 45 − 10 rule) so in-flight requests drain before SIGKILL. Use `tini` as PID 1 in the image (as `Dockerfile.drain` does) or install signal handlers directly.
- **Store and await every `JoinHandle`** (CLAUDE.md rule; the drain's `JoinSet` does this — dropped handles swallow panics). Use `tokio::spawn` fire-and-forget only for non-critical side effects (billing pushes).

### 6.4 Health for the Rust services

- **Rust API:** serve `GET /health → {"status":"healthy"}` (200) so the existing k8s `httpGet /health :8000` probes and the Cachet checker (`gateway:8080/health`) work unchanged. Bypass all middleware for `/health` and `/metrics` (the Python precedent).
- **Rust workers/daemons (no HTTP):** use the drain's **file-freshness exec-probe** pattern — touch `CEPHOR_LIVENESS_FILE`/`CEPHOR_READINESS_FILE` on each tick; k8s probes are `exec` shell one-liners checking mtime freshness. Readiness should report NotReady on a wedged-but-alive loop (a `ReadinessTracker` that trips when there is pending work but the processed counter hasn't advanced within a stall window), so a wedge sheds traffic without a crash-restart.

### 6.5 Config loader

Follow `crates/hippius-drain-agent/src/config.rs`: parse from an injectable lookup closure (testable without process env), typed `#[non_exhaustive]` `thiserror` `ConfigError`, present-but-unparsable = loud error, missing = documented `DEFAULT_*` const, zero rates rejected. **Read the Python var names verbatim** (§1) so a Rust pod is a drop-in for a Python pod under the same `hippius-s3-secrets` + `hippius-s3-defaults`. Preserve the strict-bool semantics (`_parse_bool` equivalent) and the fail-closed SS58 allowlist (validate format 42).

---

## Open questions

1. **API worker model & connection budget.** The Python API runs 4 uvicorn processes/pod (×4 asyncpg pools); a Tokio API is 1 process. The per-pod→cluster connection accounting against `max_connections=1000` needs re-derivation before Phase 4 — the ×4 assumption disappears but pod count and pool sizing change. Not answerable from config alone.
2. **Does the rewrite keep the `api`/`gateway` split or the merged form?** The manifests show a completed gateway→api merge (the `gateway` Service is an alias selecting `app: api`). Assumed merged; confirm no separate gateway binary is intended.
3. **Migrations ownership under Rust.** Python `api` runs dbmate migrations in `start-api.sh`; workers gate on `wait_for_migrations.py`; the Rust drain owns `cephor_*` via its own sqlx. If the Rust API replaces the Python api, **who runs the dbmate app-schema migration** — a dedicated Job (already exists: `k8s/base/migration-job.yaml`) or the Rust API at boot? Recommend the Job stays authoritative and the Rust API gates, not migrates. Needs a decision.
4. **Trace export for the API.** The drain exports metrics only (no traces). The Python API exports traces to Tempo via the collector; the Rust API should too — but the exact span set / sampling for parity with dashboards that query Tempo is not fully specified in-repo (rules live in the external hippius-otel repo).
5. **Prod alert rules are external.** The 24 prod rules live in `thenervelab/hippius-otel` (not this repo), so the Rust metric contract in §3.2 is derived from `monitoring.py` + `docs/grafana-alerting.md`; the exact `for`/threshold expressions (and the 8 drain/Ceph rules) must be cross-checked against that repo before declaring metric parity complete.
6. **`redis` primary cache in k8s** is an `ExternalName` to another namespace (`redis.redis.svc.cluster.local`); its own topology/HA is out of this repo's manifests and not covered here.
7. **HA redis cutover is staged but not executed in-repo.** `redis-queues-ha.yaml` exists but is wired into no kustomization; whether the Rust cutover should assume single-replica `redis-queues` or the HA set depends on when §5.4 completes.
