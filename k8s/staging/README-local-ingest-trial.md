# Staging trial: per-node local-SSD ingest tier (s3-2.0 prototype)

Prototype the [s3-2.0](../../s3-2.0.html) upload flow on staging: run the **entire staging `api`
fleet on node-local SSD** (3 pods, one per local-SSD node), served through the **normal `api`
Service / gateway front door**, while all workers stay on CephFS exactly as today.
**Infra wiring only — the replicator daemon is a separate follow-up; uploads complete e2e only once
it is installed.**

## What this adds / changes (all under `k8s/staging/`)

| File | What |
|---|---|
| `pv-local-ingest-staging.yaml` | One node-local `PersistentVolume` (100Gi, `Retain`) per node — `k8s-v3-node1/2/3`, path `/var/lib/hippius/local_ingest`. |
| `pvc-local-ingest-staging.yaml` | One statically-bound PVC per PV. |
| `api-local-deployments-staging.yaml` | `api-local-node1/2/3` — single-replica api pods pinned to each node, writing local (`HIPPIUS_OBJECT_CACHE_DIR=local_object_cache`) with ceph read-fallback. **These are the whole api fleet.** |
| `resource-limits.yaml` (modified) | Base (ceph) `api` deployment scaled to **0** — no ceph api pods. |
| `kustomization.yaml` (inline patch) | `api` Service selector switched `app: api` → `app: api-local`, so the gateway routes to the 3 local pods. |
| `replicator-daemonset-staging.yaml` | **Placeholder** for the replicator (NOT in kustomization; daemon not built yet). |

Untouched / still on ceph: `gateway`, `arion-uploader/downloader/unpinner`, `janitor`, the
`s3-backup` stack (`backup`/`hydrator`/`cleanup`), the shared configmap, `object-cache-pvc`.

## How routing works (the "proper" front door)

The gateway forwards to `http://api:8000` (the `api` Service). This trial scales the base ceph `api`
to 0 and switches the `api` Service selector to `app: api-local`, so the Service's endpoints are
exactly the 3 local-ingest pods. The gateway round-robins across them with no special target. Each
local deployment keeps a unique selector (`app: api-local` + `ingest-node`) so it manages only its
own pod. Prod is unaffected (its overlay doesn't include these staging patches).

## ⚠️ Uploads need the replicator (by design, for now)

Every api pod writes chunks to its **node-local** disk; the workers read **ceph**. With no ceph api
pods, **every** PUT stages locally and — until the replicator copies local→ceph — that object:
- is **invisible to `arion-uploader` / `backup`** → never uploaded to Arion/OVH → stays `pending`;
- **404s / hangs** on a GET that lands on a different-node api pod.

This is the agreed state: stand up the stack now, install + test the replicator when it's ready. Keep
trial traffic bounded until then.

## Prerequisite: create the local dir on each node

`local` PVs do not create their path. On each of `k8s-v3-node1/2/3`:

```bash
# one-shot privileged pod per node (repeat with the right nodeName)
kubectl -n hippius-s3-staging run mkdir-ingest-node1 --rm -it --restart=Never \
  --image=busybox --overrides='{"spec":{"nodeName":"k8s-v3-node1","containers":[{"name":"m","image":"busybox","command":["sh","-c","mkdir -p /host/var/lib/hippius/local_ingest && chmod 0777 /host/var/lib/hippius/local_ingest && ls -la /host/var/lib/hippius"],"volumeMounts":[{"name":"h","mountPath":"/host"}]}],"volumes":[{"name":"h","hostPath":{"path":"/"}}]}}'
```

## Deploy

```bash
kubectl kustomize k8s/staging | less          # review the rendered output first
kubectl apply -k k8s/staging                  # or via the staging-deploy pipeline
kubectl -n hippius-s3-staging get pods -l app=api-local -o wide   # expect 3, one per node, Running
kubectl -n hippius-s3-staging get deploy api                      # expect base api 0/0
kubectl -n hippius-s3-staging get pvc | grep local-ingest         # expect 3 Bound
kubectl -n hippius-s3-staging get endpoints api                   # expect 3 endpoint IPs (the local pods)
```

## Verify the flow / demonstrate the dependency

```bash
# watch a local pod's ingest dir fill on PUT
kubectl -n hippius-s3-staging exec deploy/api-local-node1 -c api -- \
  sh -c 'ls -R /var/lib/hippius/local_object_cache | head'
# the same object_id is ABSENT on ceph until the replicator runs:
kubectl -n hippius-s3-staging exec deploy/api-local-node2 -c api -- \
  sh -c 'ls /var/lib/hippius/object_cache/<object_id> 2>&1'
```

After the replicator lands: confirm it copies local→ceph (meta-last) + rings `notify:`, the uploader
completes to Arion (`uploaded`→`published`), and a cross-node GET returns the object — proving
cross-node visibility via ceph. Measure replication lag.

## Teardown

```bash
# in kustomization.yaml: remove the 3 trial resources + the inline api Service-selector patch
# (restores selector to app: api); in resource-limits.yaml: set base api replicas back to 2. Then:
kubectl apply -k k8s/staging
# PVs are Retain — delete them + clear the node dirs manually:
kubectl delete pv local-ingest-pv-node1 local-ingest-pv-node2 local-ingest-pv-node3
# (rm -rf /var/lib/hippius/local_ingest/* on each node via a privileged pod)
```

## Notes / risks

- Staging shares the `hippius` cluster with **prod** — these PVs live on general-worker root disks
  (`node1/2/3`), never node6-cache or the psql nodes.
- `local` PV capacity (100Gi) is nominal — not enforced; the pod can fill the node root disk. Keep
  trial data bounded (no janitor runs on this local tier yet).
- The cache env vars are set **per-deployment**, never in the shared configmap — flipping the
  configmap would break any ceph pod (none run during the trial, but keep this in mind on teardown).
