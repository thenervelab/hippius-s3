# Staging trial: node-local-SSD ingest tier (s3-2.0 prototype)

Prototype the [s3-2.0](../../s3-2.0.html) upload flow on staging: run the **entire staging `api`
fleet on node-local SSD** (3 pods spread across general workers), served through the **normal `api`
Service / gateway front door**, while all workers stay on CephFS exactly as today.
**Infra wiring only — the replicator daemon is a separate follow-up; uploads complete e2e only once
it is installed.**

## What this adds / changes (all under `k8s/staging/`)

| File | What |
|---|---|
| `api-local-deployments-staging.yaml` | `api-local` Deployment (**3 replicas**) writing local (`HIPPIUS_OBJECT_CACHE_DIR=local_object_cache`) with ceph read-fallback. **This is the whole api fleet.** |
| `resource-limits.yaml` (modified) | Base (ceph) `api` deployment scaled to **0** — no ceph api pods. |
| `kustomization.yaml` (inline patch) | `api` Service selector switched `app: api` → `app: api-local`, so the gateway routes to the local pods. |
| `replicator-daemonset-staging.yaml` | **Placeholder** for the replicator (NOT in kustomization; daemon not built yet). |

Untouched / still on ceph: `gateway`, `arion-uploader/downloader/unpinner`, `janitor`, the
`s3-backup` stack (`backup`/`hydrator`/`cleanup`), the shared configmap, `object-cache-pvc`.

## Storage: hostPath, self-provisioning (no manual node prep)

Each pod mounts a `hostPath` volume `type: DirectoryOrCreate` at the node path
`/var/lib/hippius/local_ingest` → container `/var/lib/hippius/local_object_cache`. The kubelet
**creates the dir** on whatever node the pod lands on, and a root initContainer (`prepare-ingest-dir`)
`chmod 0777`s it so the (possibly non-root) api can write. **No manual `mkdir` on nodes, so this
deploys cleanly via CI.**

> This replaced an earlier `local`-PV-per-node design. A `local` PV requires the path to pre-exist on
> the node — the kubelet fails the mount otherwise, and that prep can't run in a CI deploy. It also
> pinned each pod to one node, and one of those nodes was at its 110-pod cap (unschedulable). The
> hostPath + anti-affinity approach avoids both. `local` PV capacity is not enforced anyway; hostPath
> likewise uses the node root disk — keep trial data bounded (no janitor on this tier yet).

## Scheduling

`api-local` is restricted to general workers (`k8s-v3-node1..5`) and uses **preferred** pod
anti-affinity, so the 3 replicas spread across distinct nodes when capacity allows (and still
schedule if a node is full). Never lands on node6-cache or the psql nodes.

## How routing works (the "proper" front door)

The gateway forwards to `http://api:8000` (the `api` Service) and blocks on an init `wait-for-api`
(`nc -z api 8000`) until that Service has a ready endpoint. This trial scales base ceph `api` to 0
and switches the `api` Service selector to `app: api-local`, so the gateway's backend is the 3 local
pods — normal front door, no separate target. Prod is unaffected (its overlay omits these patches).

## ⚠️ Uploads need the replicator (by design, for now)

Every api pod writes chunks to its **node-local** disk; the workers read **ceph**. With no ceph api
pods, **every** PUT stages locally and — until the replicator copies local→ceph — that object:
- is **invisible to `arion-uploader` / `backup`** → never uploaded to Arion/OVH → stays `pending`;
- **404s / hangs** on a GET that lands on a different-node api pod.

This is the agreed state: stand up the stack now, install + test the replicator when it's ready. Keep
trial traffic bounded until then.

## Deploy & verify

```bash
kubectl kustomize k8s/staging | less          # review first
kubectl apply -k k8s/staging                  # or via the staging-deploy pipeline
kubectl -n hippius-s3-staging rollout status deploy/api-local --timeout=5m   # 3/3 Ready
kubectl -n hippius-s3-staging get pods -l app=api-local -o wide              # 3 pods, spread
kubectl -n hippius-s3-staging get deploy api                                 # base api 0/0
kubectl -n hippius-s3-staging get endpoints api                              # 3 endpoint IPs (local pods)

# watch a local pod's ingest dir fill on PUT; same object_id is ABSENT on ceph until the replicator runs
kubectl -n hippius-s3-staging exec deploy/api-local -c api -- sh -c 'ls -R /var/lib/hippius/local_object_cache | head'
```

After the replicator lands: confirm it copies local→ceph (meta-last) + rings `notify:`, the uploader
completes to Arion (`uploaded`→`published`), and a cross-node GET returns the object. Measure lag.

## Teardown

```bash
# in kustomization.yaml: remove the api-local-deployments resource + the inline api Service-selector
# patch (restores selector to app: api); in resource-limits.yaml: set base api replicas back to 2.
kubectl apply -k k8s/staging
# optionally clear the node dirs: rm -rf /var/lib/hippius/local_ingest/* on each node via a privileged pod
```

## Notes / risks

- Staging shares the `hippius` cluster with **prod** — keep ingest off node6-cache / psql (handled by
  the nodeAffinity allow-list + psql taints).
- The cache env vars are set **per-deployment**, never in the shared configmap — flipping the
  configmap would break any ceph pod (none run during the trial; relevant on teardown).
