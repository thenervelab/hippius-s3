# Hippius-S3 — Architecture Component Inventory

> Research snapshot, 2026-06-17. Map of every deployed pod/service/worker and how
> they string together. Cross-checked against `k8s/` manifests and worker entry
> points. Two items here are **drift from the main CLAUDE.md** (flagged below):
> the **ATS edge tier** and the **orphan-checker** worker.

The system is split into **edge → gateway → API → workers**, backed by
**Postgres + 4 Redis instances + an FS chunk cache**. Two FastAPI services
(gateway is public, API is internal); heavy/slow work is pushed to a fleet of
queue-draining workers.

---

## Request-path components (synchronous)

| Component | Kind | Port | Prod replicas | Role |
|---|---|---|---|---|
| **ATS** (Apache Traffic Server) | Edge proxy — **out of cluster** | — | per-region | Regional read-only cache for hot/public objects. Gateway sends `PURGE` on writes + `Cache-Control` headers. Optional (no-op when `ATS_CACHE_ENDPOINT` unset). |
| **Ingress (nginx)** | Ingress | — | — | `s3.hippius.network` → gateway. Body-size 0, streaming, 600s timeouts. `k8s/base/ingress.yaml`. |
| **gateway** | Deployment | 8080 | 5 | Only internet-exposed service. Auth (5 methods) → ACL → rate-limit → audit → forwards to API with trusted `X-Hippius-*` headers. Streaming proxy. `gateway/main.py:42`. |
| **api** | Deployment | 8000 | 5 | Core S3 logic. Chunks + encrypts, writes to FS cache + DB, enqueues backend work. Decryption always flows through here (envelope crypto). `hippius_s3/main.py:242`. |

### ATS (Apache Traffic Server) — the new edge tier

**Not documented in the main CLAUDE.md.** This is the edge tier from the S3 2.0
cache redesign. It sits *in front* of the gateway on edge/CDN nodes (out of
cluster) and is meant to relieve the node6 cache SPOF.

- **Config:** `ATS_CACHE_ENDPOINT` (CSV) — `k8s/base/gateway-deployment.yaml:134-138`, parsed in `gateway/config.py:125-130`. Empty → all cache logic is a safe no-op.
- **PURGE invalidation:** on writes/deletes the gateway fires HTTP `PURGE` to every configured ATS endpoint (fire-and-forget, parallel). `gateway/services/ats_cache_client.py`.
- **Cache-Control headers:** public buckets get `public, max-age=2592000` (30d); private objects get `X-Hippius-Object-Visibility: private` and ATS demotes Cache-Control to `no-cache, no-store`. `gateway/middlewares/cache_control.py`.
- **Anonymous-read flag:** `gateway/middlewares/acl.py:160` sets `request.state.anonymous_read_allowed` so the cache middleware avoids a Redis round-trip when ATS is disabled.
- **Auth-probe subrequests:** ATS can send `X-Hippius-Auth-Probe` requests; the gateway's auth-probe middleware returns 200 immediately so ATS can gate cache hits without full request processing.
- **Docs:** `docs/drain-direct-rollout.md` — SSD→Ceph drain-direct rollout & cutover (the current ingest architecture).
- **Tests:** `tests/unit/gateway/test_ats_*.py`, `test_cache_control_middleware.py`, `test_anonymous_read_flag.py`, `test_auth_probe_middleware.py`.

---

## Worker fleet (async, queue-driven)

All workers run via `start-worker.sh` with `WORKER_SCRIPT` selecting the entry point.
Image: `ghcr.io/thenervelab/hippius-s3/workers:latest`.

| Worker | Entry point | Queue / trigger | Prod replicas | Role |
|---|---|---|---|---|
| **arion-uploader** | `workers/run_arion_uploader_in_loop.py` | `arion_upload_requests` | **40** | Reads chunks from FS → uploads to Arion → records `chunk_backend` → publishes to Hippius chain. Recreate strategy. |
| **arion-downloader** | `workers/run_arion_downloader_in_loop.py` | `arion_download_requests` | 10 | Cache-miss path: fetches chunks from Arion → FS cache → pub/sub notifies waiting streamers. Also runs in the cache tier. |
| **arion-unpinner** | `workers/run_arion_unpinner_in_loop.py` | `arion_unpin_requests` | 3 | Soft-deletes `chunk_backend` rows + DELETEs from Arion. |
| **janitor** | `workers/run_janitor_in_loop.py` | ~5-min loop | 1 | FS cache GC. Replication-gated (never evicts un-replicated chunks), hot-retention + disk-pressure modes. |
| **orphan-checker** | `workers/run_orphan_checker_in_loop.py` | ~2-hr loop | 1 | Scans chain for on-chain files missing from DB → enqueues unpin. **Not in the CLAUDE.md worker list.** |
| **account-cacher** | `workers/run_account_cacher_in_loop.py` | ~5-min loop | 1 | Caches Substrate account credit/role state into redis-accounts (10-min TTL). |
| **cachet-health-checker** | `workers/cachet_health_check.py` | poll | 1 | Reports gateway health to external Cachet status page. |
| **migrator** | `workers/run_migrator_once.py` | K8s Job | one-shot | Data migrations (e.g. v4→v5 storage version). |

### Rust drain fleet (SSD→Ceph ingest, s3-2.1)

Not queue-draining Python workers — a separate Rust workspace (`crates/hippius-drain-*`) deployed
as k8s manifests, now the **sole producer** of backend upload requests. Prod cutover completed
2026-07-20–27 (`k8s/production/kustomization.yaml`).

| Component | Manifest | Kind | Role |
|---|---|---|---|
| **drain-agent** | `k8s/production/drain-agent-daemonset.yaml` | DaemonSet (one per ingest node) | Replicates each complete part from api-local SSD → CephFS pool, writes `cephor_replication_status`, then `LPUSH`es one `UploadChainRequest` per part to `arion_upload_requests` as it replicates. |
| **drain-allocator** | `k8s/production/drain-allocator-deployment.yaml` | Deployment (singleton, leader-elected) | Owns the `cephor_*` schema; reads fleet heartbeats + Ceph ceiling and writes per-node drain budgets. |
| **mpu-reaper** | `k8s/production/mpu-reaper-deployment.yaml` | Deployment (single replica) | Reaps abandoned in-flight MPU parts. |

---

## State / infra

- **Postgres (CNPG)** — `k8s/base/postgres-cluster.yaml`. Primary + 1 standby, 50Gi each, unsupervised failover, WAL archived to OVH via barman. Prod adds **dedicated NVMe clusters** (`k8s/production/postgres-nvme-*.yaml`) — related to the postgres-nvme OOM incident.
- **4 Redis StatefulSets** — `k8s/base/redis-statefulsets.yaml`:

  | Service | Persistence | Mem cap | Purpose |
  |---|---|---|---|
  | `redis-accounts` | AOF | 2Gi | Account credit/role cache |
  | `redis-queues` | AOF, LRU | 2Gi | Work queues + chunk pub/sub |
  | `redis-rate-limiting` | ephemeral | 1Gi | Rate-limit counters |
  | `redis-acl` | ephemeral, LRU | 2Gi | ACL permission cache |

  The old 32Gi `redis-download-cache` (:6385) is decommissioned (2026-04-21). `redis-chain`
  (:6381) is decommissioned (2026-06-30) — it was wired up but never read or written.
- **FS chunk cache** — `/var/lib/hippius/object_cache` on NVMe/CephFS PVC (9728Gi in prod). The actual chunk store; Redis is only pub/sub + queues. PVCs in `k8s/base/pvc-shared.yaml`.
- **SSD-ingest tier** (s3-2.1) — the api runs on node-local SSD (`api-local`, one pod per labeled ingest node) with the Rust drain fleet replicating SSD→CephFS. Manifests live in `k8s/production/` (`api-local-deployments-production.yaml`, `drain-*`, `mpu-reaper-*`). The former `k8s/cache/` regional read-only deployments no longer exist.
- **Observability** — in-cluster `otel-collector` (`k8s/base/otel-collector.yaml`, image `otel/opentelemetry-collector-contrib:0.96.0`) fans out to Prometheus/Tempo/Loki/Grafana/Alloy in the separate `monitoring` namespace (helm, `k8s/otel/`).
- **DB migration Job** — `k8s/base/migration-job.yaml`, runs `python -m hippius_s3.scripts.migrate` before deploys.

---

## How it strings together

### PUT
```
client → ATS (edge) → ingress → gateway(auth/ACL) → api(chunk+encrypt+FS write to node-local SSD+DB)
   → drain-agent replicates each part SSD→CephFS (writes cephor_replication_status)
   → drain-agent LPUSHes arion_upload_requests (sole producer) → arion-uploader → Arion + chain
                                                       ↑ janitor won't GC until replicated
   → gateway fires PURGE to ATS to invalidate edge caches
```
Note: the api no longer enqueues the backend upload at PUT/MPU-complete (s3-2.1 drain-direct,
`hippius_s3/api/s3/multipart.py:1193`); the drain is the sole `arion_upload_requests` producer.

### GET
```
client → ATS (hit? serve) → ingress → gateway(auth/ACL) → api
   → FS cache hit?  → stream directly
   → FS cache miss? → SET NX coalesce lock → enqueue arion_download_requests
                      → wait on redis-queues pub/sub notify:{chunk_key}
   arion-downloader → fetch from Arion → FS cache → notify → api streams to client
```

### DELETE (unpin)
```
client → gateway(auth/ACL) → api(soft-delete) → enqueue arion_unpin_requests
   → arion-unpinner → soft-delete chunk_backend + Arion DELETE
   → janitor hard-deletes FS chunks once unpins confirmed on all backends
```

---

## Queue / DLQ topology (redis-queues)

- `arion_upload_requests`, `arion_upload_requests_retry_{delay_ms}`
- `arion_download_requests`
- `arion_unpin_requests`
- `{backend}_upload_dlq`, `{backend}_unpin_dlq` — dead-letter queues (scanned by janitor to avoid evicting in-flight data)

Queue names are `{backend}_*` so additional backends (beyond `arion`) get parallel queues via `HIPPIUS_UPLOAD_BACKENDS` / `HIPPIUS_DOWNLOAD_BACKENDS` / `HIPPIUS_DELETE_BACKENDS`.

---

## Data-flow matrix

| Component | Postgres | Redis (general) | Redis (queues) | FS cache | Arion | Substrate/chain |
|---|---|---|---|---|---|---|
| gateway | R | R/W (metrics) | — | — | — | — |
| api | R/W | R/W (cache+pubsub) | R (DLQ) | R/W | — | — |
| arion-uploader | R/W | — | R (pop) | R | W (upload) | W (tx) |
| arion-downloader | R | W (pub/sub) | R (pop) | W | R (download) | — |
| arion-unpinner | R/W | — | R (pop) | — | W (delete) | — |
| janitor | R | R | R (DLQ scan) | R/D | — | — |
| orphan-checker | R/W | — | W (enqueue) | — | — | R |
| account-cacher | — | — | — | — | — | R |

---

## Drift from CLAUDE.md (worth reconciling)

1. **ATS edge tier** — entire Apache Traffic Server caching layer (gateway integration, PURGE, cache-control, auth-probe) is undocumented in the main CLAUDE.md. Part of the S3 2.0 cache redesign.
2. **orphan-checker worker** — present as an entry point and referenced in the subsystem index, but missing from the section-5/section-7 worker descriptions.
