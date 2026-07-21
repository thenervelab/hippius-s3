# Drain-direct upload enqueue — rollout & cutover runbook

This PR makes the Rust drain the **sole producer** of backend upload requests (the api no
longer enqueues at PUT/MPU-complete) and deletes the upload-promoter + its
`pg_notify`/sweep machinery. It is a hard cutover with **no feature flag and no Mode-A
fallback**, so the order things deploy in matters. This document is the rollout plan the
PR review (P2/P3) asked for.

## Deploy order (per environment)

Deploy the **drain-agent first**, then the api that stops enqueuing:

1. Roll out the `drain` image (allocator + agent DaemonSet + mpu-reaper). The new drain
   enqueues each part as it replicates.
2. Then roll out the `api`/`workers` images from this PR (the api stops enqueuing at PUT).

During the overlap both the old api and the new drain may enqueue the same part — that is
**harmless**: the uploader is idempotent (`skip_if_exists` + `chunk_backend ON CONFLICT`).
The reverse order (api first) opens a gap: a part the *old* drain marks `replicated`
during the window is never enqueued (the new drain's `AlreadyReplicated` fast path returns
before enqueue, and the reconciler leaves `replicated` rows alone), and the promoter sweep
that used to catch these is gone.

## Cutover backstop — re-drive replicated-but-unenqueued parts

If the order above could not be guaranteed (or to be safe), run a one-time re-drive for
parts that are `replicated` on Ceph but were never enqueued to a backend — i.e. a
`cephor_replication_status` row in `replicated` whose chunks have **no** `chunk_backend`
row for the upload backend. This is exactly what the deleted `promoter_sweep_unpromoted`
query found. To enumerate them (read-only) before deciding to re-enqueue:

```sql
-- part_chunks links to parts via part_id; chunk_backend links via chunk_id = part_chunks.id
-- (see hippius_s3/sql/queries/count_chunk_backends.sql).
SELECT crs.object_id, crs.version
FROM cephor_replication_status crs
WHERE crs.status = 'replicated'
  AND NOT EXISTS (
        SELECT 1
        FROM parts p
        JOIN part_chunks pc ON pc.part_id = p.part_id
        JOIN chunk_backend cb ON cb.chunk_id = pc.id
        WHERE p.object_id = crs.object_id::uuid
          AND p.object_version = crs.version
          AND cb.deleted = false
      );
```

For each row, the object's parts can be re-enqueued by resetting the part to `pending`
(the drain re-claims, finds the Ceph copy already present, and enqueues), or by enqueuing
an `UploadChainRequest` directly via `hippius_s3.queue.enqueue_upload_to_backends`. Prefer
resetting to `pending` so the single producer (the drain) stays authoritative.

## Production manifests (not in this PR — staging-only)

Only `k8s/staging/*` carries the drain stack today. Before prod can run the drain, the
prod overlay needs (mirroring `k8s/staging/`, with **prod** node labels / hostPaths /
PVCs):

- the `drain` image added to `k8s/production/kustomization.yaml` `images:`,
- `drain-allocator-deployment.yaml` (the cephor_* schema owner + allocator),
- `drain-agent-daemonset.yaml` — must set `CEPHOR_SSD_ROOT` to the prod api-local cache
  path, `CEPHOR_POOL_ROOT` to the Ceph pool mount, **`REDIS_QUEUES_URL`**, and
  **`HIPPIUS_UPLOAD_BACKENDS`** (the drain is now the producer). `REDIS_QUEUES_URL` is
  `required()` — a missing var crash-loops the agent (fail-fast, not silent), but uploads
  stop until corrected, so wire it before the api cutover.
- `mpu-reaper-deployment.yaml` (DB + redis-queues only; single replica).
- prod ingest-node labels so the DaemonSet schedules exactly where api-local pods land.

Staging hardcodes `HIPPIUS_UPLOAD_BACKENDS=arion` on the daemonset; for prod confirm it
matches the api/uploader fleet (e.g. `arion,ovh`) — the value the drain stamps onto every
`UploadChainRequest.upload_backends`.

## bypass_billing (P4)

The drain hardcodes `bypass_billing: false` on every `UploadChainRequest`
(`enqueue.rs`). This matches the prior producers: the api's PUT/MPU enqueue
(`hippius_s3/writer/queue.py::enqueue_upload`) never set it, so the Python default
(`False`) already applied. No upload flow required `true`, so the cutover preserves the
billing behavior.
