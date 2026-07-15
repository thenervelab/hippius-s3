# "redis-0 unreachable" is a phantom monitoring target — not an app bug

## Summary
Prod health snapshots report a Redis instance named `redis-0` as **unreachable** every run,
while `redis-cluster` (12/12 pods) is fully healthy. Investigation confirms **there is no bug in
this repo's code or manifests** — `redis-0` is a stale target in the external health-check probe.
No code change is required here; recorded for posterity and to stop it being re-triaged as a Redis
outage. The probe itself has been corrected (see "Resolution").

## Why there is no `redis-0` pod
The only Kubernetes objects that create numbered pods (`-0`, `-1`, …) are StatefulSets. The Redis
StatefulSets defined in this repo are:
- `redis-accounts`, `redis-queues`, `redis-rate-limiting`, `redis-acl` (`k8s/base/redis-statefulsets.yaml`)
- `redis-cluster` — 12 shards (`k8s/redis-cluster/production.yaml`), pods `redis-cluster-0..11`

There is **no StatefulSet named `redis`**, so Kubernetes never creates a pod `redis-0`.

The general-purpose cache (`REDIS_URL`, logical port 6379) is a **`type: ExternalName` Service**
named `redis` (`k8s/base/services.yaml`), patched in prod to alias `redis-cluster`
(`k8s/production/services-patch.yaml` → `redis-cluster.hippius-s3-prod.svc.cluster.local`). An
ExternalName Service is a DNS alias with **no pods and no selector** — it cannot produce a `redis-0`.
So general-cache traffic terminates on `redis-cluster`, which is exactly the healthy instance the
snapshots already report.

This matches the root `CLAUDE.md` §5.3 table, which lists `redis :6379` as the *logical* general
cache role — it never promised a standalone `redis` StatefulSet.

## Impact
- None on the service. `server_errors=0`; the general cache is healthy via `redis-cluster`.
- Monitoring-only: a permanent false "1 instance unreachable" that dilutes real Redis signal.

## Resolution
The bug is in the **health-check probe**, not this repo. The probe's instance list
(`s3-prod-health/scripts/lib/redis.sh`) hard-coded `redis-0` as if a `redis` StatefulSet existed and
`kubectl exec`'d into a non-existent pod. Fixed by removing `redis-0` from the probed instances — the
general cache is already covered by `redis-cluster-0`. No application or manifest change is needed.

## Action for repo maintainers
Nothing to merge as code. This doc exists so the next person who sees "redis-0 unreachable" knows it
is expected and where the real (monitoring-side) fix lives.
