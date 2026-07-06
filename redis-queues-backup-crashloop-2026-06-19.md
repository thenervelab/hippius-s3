# Prod incident — `backup`/`cleanup` worker crash-loop from a transient Redis blip

**Date:** 2026-06-19 · **Window:** ~12:28 UTC (start) → recovering by ~12:35 UTC · **Severity:** Low (background workers only) · **Status:** Self-recovering

---

## TL;DR

A momentary stall on `redis-queues-0` (~12:28 UTC) caused every `backup` and `cleanup` worker in `hippius-s3-prod` to crash simultaneously and enter `CrashLoopBackOff`. The workers do a blocking `BRPOP` against `redis-queues` and **treat a Redis read timeout as fatal** (exit code 1, no retry), so a single ~3-second Redis hiccup turned into a multi-minute outage of the backup/cleanup queues.

- **User-facing S3 was NOT affected.** `api` (5/5) and `gateway` (5/5) stayed up with 0 restarts and 3d21h uptime.
- **Redis never actually went down** — `redis-queues-0` has 0 restarts / 80d uptime and is healthy now. It had one transient unresponsive moment (readiness probe timed out once).
- **Workers are recovering** on their own as they restart and reconnect.

---

## Impact

| Component | Affected? | Detail |
|---|---|---|
| `api` (S3 internal) | ❌ No | 5/5 ready, 0 restarts, 3d21h uptime |
| `gateway` (S3 public) | ❌ No | 5/5 ready, 0 restarts, 3d21h uptime |
| `backup` (~10 pods) | ✅ Yes | Crash-loop, exit 1, restarts 1–3 |
| `cleanup` | ✅ Yes | Crash-loop, exit 1 |
| `hydrator` (~20 pods) | ⚠️ Partial | Up and running; noisy `otel-collector` export failures (cosmetic only) |
| `redis-queues-0` | ⚠️ Transient | One ~3s probe timeout; healthy now, never restarted |

Customer impact: none on the read/write S3 data path. Delayed backup + cleanup queue processing for the duration of the crash-loop window.

---

## Root cause

1. **~12:28 UTC** — `redis-queues-0` briefly became unresponsive. Its readiness probe failed once:
   > `Readiness probe failed: command timed out: "sh -c redis-cli ping | grep -q PONG" timed out after 3s`
2. The `backup`/`cleanup` workers were blocked in a `BRPOP` on the queue. The stall surfaced as:
   ```
   File "/app/s3_backup/workers/backup.py", line 592, in run_backup_worker
       result = await redis_client.brpop(settings.backup_queue_name, timeout=settings.brpop_timeout_seconds)
   ...
   redis.exceptions.TimeoutError: Timeout reading from redis-queues:6379
   ```
3. The workers **do not catch this** — the exception propagates and the process exits with code 1 (`reason: "Error"`). All backup/cleanup pods died at roughly the same instant.
4. Kubernetes restarted them → `CrashLoopBackOff`. The ~30 simultaneous image pulls then tripped a secondary, transient `ErrImagePull: pull QPS exceeded` on the nodes (a symptom, not the cause).

### Why this is fragile
A single transient Redis blip → mass simultaneous crash of every backup/cleanup worker. The fail-fast-on-timeout behaviour amplifies a 3-second dependency hiccup into a multi-minute worker outage.

---

## Evidence

**Worker crash (previous container, `backup-5b56c9c786-9svvz`):**
```
lastState: terminated, exitCode 1, reason "Error"
startedAt  2026-06-19T12:28:47Z
finishedAt 2026-06-19T12:29:07Z      # ran ~20s, then crashed
```
Traceback terminates in:
```
redis.exceptions.TimeoutError: Timeout reading from redis-queues:6379
```

**`redis-queues-0` is healthy (post-event):**
```
restarts=0   age=80d
used_memory_human=93.30M   maxmemory=2.00G   policy=allkeys-lru
connected_clients=670   blocked_clients=34
instantaneous_ops_per_sec=201
evicted_keys=0   rejected_connections=0
ping latency ~0 ms
node=k8s-v3-node3  (MemoryPressure=False, Ready=True)
```

**User-facing path unaffected:**
```
api-5694bf9878-*       1/1 Running  0 restarts  3d21h   (x5)
gateway-6dbfb4987-*    1/1 Running  0 restarts  3d21h   (x5)
```

---

## Not the cause (ruled out)

- **`ErrImagePull` / "pull QPS exceeded"** — secondary effect of ~30 pods restarting and pulling the same image at once. Cleared on its own.
- **Image change** — none. `ghcr.io/thenervelab/s3-backup:ea5afe3` is unchanged since Jan/Feb; one affected pod had been stably running since 2026-06-14 until it died at 12:28.
- **`postgres-nvme` flapping** — `postgres-nvme-1/2/3` show high restart counts (57/54/10), but the last restarts were 50–126 min *before* the event and all terminated gracefully (`Completed`, exit 0). This is a separate, pre-existing nvme-stall issue, **not** today's trigger.
- **`otel-collector` export errors** in `hydrator` logs — cosmetic; metrics export only, not the data path.

---

## Remediation

**P1 — Make the workers tolerate a Redis timeout (the real fix).**
`backup`/`cleanup` should catch `redis.exceptions.TimeoutError` (and `ConnectionError`) around the `BRPOP` loop and retry with backoff, instead of exiting 1. This converts a momentary Redis stall into a brief no-op instead of a crash-loop of the entire worker fleet. *(Code lives in the `s3-backup` image, not this repo.)*

**P2 — Investigate why `redis-queues-0` stalled at 12:28 UTC.**
Likely a brief blocking operation, an AOF/RDB rewrite fork pause, or momentary contention on `k8s-v3-node3`. Check Redis `latest_fork_usec` / slowlog and node3 around 12:28 UTC.

**P3 — Loosen the readiness probe and/or stagger restarts.**
A 3s `ping` timeout with no tolerance is tight; one slow tick ejects the pod. Pairing P1 with a slightly more forgiving probe avoids the cascade.

**Separate track — `postgres-nvme` stalls** remain an open, ongoing item (chronic restarts under load); tracked independently of this incident.

---

## Current status

- `redis-queues-0`: healthy, never restarted.
- `backup`/`cleanup`: back to `Running`, restart counts settling (1–3) as they reconnect; expected to stabilize since the dependency recovered.
- No action required for the S3 data path.
