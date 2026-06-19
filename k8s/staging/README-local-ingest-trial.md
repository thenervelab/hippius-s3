# Staging trial: node-local-SSD ingest tier (s3-2.0 prototype)

Prototype the [s3-2.0](../../s3-2.0.html) upload flow on staging: run the **entire staging `api`
fleet on node-local SSD** (3 pods spread across general workers), served through the **normal `api`
Service / gateway front door**, while all workers stay on CephFS exactly as today.
**Infra wiring only — the replicator daemon is a separate follow-up; uploads complete e2e only once
it is installed.**

## What this adds / changes (all under `k8s/staging/`)

| File | What |
|---|---|
| `api-local-deployments-staging.yaml` | `api-local` Deployment (**2 replicas**, one per ingest node) writing local (`HIPPIUS_OBJECT_CACHE_DIR=local_object_cache`) with ceph read-fallback. **This is the whole api fleet.** |
| `resource-limits.yaml` (modified) | Base (ceph) `api` deployment scaled to **0** — no ceph api pods. |
| `kustomization.yaml` (inline patch) | `api` Service selector switched `app: api` → `app: api-local`, so the gateway routes to the local pods. |
| `ingest-node-labels-staging.yaml` | **The ONE list** of staging ingest nodes — applying it labels them `s3-staging-local-ingest=true`. |
| `replicator-daemonset-staging.yaml` | **Placeholder** for the replicator (NOT in kustomization; daemon not built yet). |

Untouched / still on ceph: `gateway`, `arion-uploader/downloader/unpinner`, `janitor`, the
`s3-backup` stack (`backup`/`hydrator`/`cleanup`), the shared configmap, `object-cache-pvc`.

## Storage: hostPath, self-provisioning (no manual node prep)

Each pod mounts a `hostPath` volume `type: DirectoryOrCreate` at the node path
`/var/lib/hippius/local_ingest_staging` → container `/var/lib/hippius/local_object_cache`. The kubelet
**creates the dir** on whatever node the pod lands on, and a root initContainer (`prepare-ingest-dir`)
`chmod 0777`s it so the (possibly non-root) api can write. **No manual `mkdir` on nodes, so this
deploys cleanly via CI.**

> **Path denominator:** staging uses `…/local_ingest_staging`, prod uses `…/local_ingest_prod`.
> staging + prod share the physical cluster, so this (on top of disjoint node labels below) guarantees
> that even if a staging and a prod pod ever landed on the same node, their local disks never collide.
>
> This replaced an earlier `local`-PV-per-node design. A `local` PV requires the path to pre-exist on
> the node — the kubelet fails the mount otherwise, and that prep can't run in a CI deploy. hostPath
> uses the node root disk and isn't size-enforced — keep trial data bounded (no janitor on this tier yet).

## Scheduling: one node-label list, api + replicator in lockstep

Ingest nodes are declared **once** in `ingest-node-labels-staging.yaml` (partial `Node` objects that
carry `s3-staging-local-ingest=true`). Deploying applies that label; **both** the `api-local`
Deployment and the `cache-replicator` DaemonSet select on it (`nodeSelector`), so they always target
the identical node set — no duplicated hostname list to drift. The DaemonSet (one pod per labeled
node) therefore covers every node an api pod can land on. Current staging set: **node2, node3** (node1 is
excluded — it runs at its pod cap, so the agent can't schedule there and would block the DaemonSet roll)
(node4/5 are near pod-cap and left for prod). `api-local` also uses preferred pod anti-affinity to
spread its 2 replicas across the labeled nodes.

**Shared cluster, disjoint sets:** prod will label *different* nodes with `s3-local-ingest=true`. Two
distinct labels on distinct nodes + the per-env path denominator = staging and prod ingest never mix.

**Retiring an ingest node:** delete its stanza in `ingest-node-labels-staging.yaml` **and** clear the
label (`apply` does not prune): `kubectl label node <name> s3-staging-local-ingest-`.

**Provisioning note:** the deploy identity must be able to `patch nodes` (to apply the label objects)
— the staging deploy SA already has it.

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
# optionally clear the node dirs: rm -rf /var/lib/hippius/local_ingest_staging/* on each node via a privileged pod
# and clear the labels: kubectl label node k8s-v3-node1 k8s-v3-node2 k8s-v3-node3 s3-staging-local-ingest-
```

## Notes / risks

- Staging shares the `hippius` cluster with **prod** — keep ingest off node6-cache / psql (handled by
  the nodeAffinity allow-list + psql taints).
- The cache env vars are set **per-deployment**, never in the shared configmap — flipping the
  configmap would break any ceph pod (none run during the trial; relevant on teardown).
