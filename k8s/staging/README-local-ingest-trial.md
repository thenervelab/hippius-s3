# Staging trial: node-local-SSD ingest tier (s3-2.0 prototype)

Prototype the [s3-2.0](../../s3-2.0.html) upload flow on staging: run the **entire staging `api`
fleet on node-local SSD** (2 pods, one per ingest node), served through the **normal `api`
Service / gateway front door**, while all workers stay on CephFS exactly as today.
**The `hippius-drain` stack (per-node `drain-agent` DaemonSet + `drain-allocator`) is now installed
and draining local→ceph** — uploads complete e2e (see the drain-gating caveat under "Uploads" below).

## What this adds / changes (all under `k8s/staging/`)

| File | What |
|---|---|
| `api-local-deployments-staging.yaml` | `api-local` Deployment (**2 replicas**, one per ingest node) writing local (`HIPPIUS_OBJECT_CACHE_DIR=local_object_cache`) with ceph read-fallback. **This is the whole api fleet.** |
| `resource-limits.yaml` (modified) | Base (ceph) `api` deployment scaled to **0** — no ceph api pods. |
| `kustomization.yaml` (inline patch) | `api` Service selector switched `app: api` → `app: api-local`, so the gateway routes to the local pods. |
| `ingest-node-labels-staging.yaml` | **The ONE list** of staging ingest nodes — applying it labels them `s3-staging-local-ingest=true`. |
| `drain-agent-daemonset.yaml` | The per-node `hippius-drain` agent — drains each node's local SSD → CephFS pool. Selects the ingest label + a hostname allow-list. |
| `drain-allocator-deployment.yaml` | The singleton (leader-elected) drain budget allocator. |

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

## Scheduling: one node-label list + a hostname allow-list, api + drain-agent in lockstep

Ingest nodes are declared **once** in `ingest-node-labels-staging.yaml` (partial `Node` objects that
carry `s3-staging-local-ingest=true`). Deploying applies that label; **both** the `api-local`
Deployment and the `drain-agent` DaemonSet select on it (`nodeSelector`), so they always target
the identical node set — no duplicated hostname list to drift. The DaemonSet (one pod per labeled
node) therefore covers every node an api pod can land on. Current staging set: **node2, node3** (node1 is
excluded — it runs at its pod cap, so the agent can't schedule there and would block the DaemonSet roll)
(node4/5 are near pod-cap and left for prod). `api-local` also uses preferred pod anti-affinity to
spread its 2 replicas across the labeled nodes.

**Belt-and-suspenders:** both the `api-local` Deployment and the `drain-agent` DaemonSet ALSO carry a
**required `nodeAffinity` hostname allow-list** (`kubernetes.io/hostname In [node2, node3]`) on top of
the label. So a node that gets the ingest label by mistake still won't host ingest unless its hostname
is on the allow-list — a single misconfiguration can't leak ingest onto a psql/cache node. The label
and the two allow-lists must be kept in sync (all three live under `k8s/staging/`).

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

## Uploads: the drain copies local→ceph (now live)

Every api pod writes chunks to its **node-local** disk; the workers read **ceph**. The `drain-agent`
copies each complete part local→ceph (meta-last) and the read path serves from the ceph cache, so a
cross-node GET returns the object once its parts are drained. Replication lag is bounded by the drain
throughput (measured ~2.5 min for a 1GB / 128-part object on staging, fsync-bound by ceph-backed PG).

> **Known caveat (tracked in `s3-2.1-todo.md`, PR-7):** the api still enqueues the Arion/OVH backend
> upload at PUT/MPU-complete, *before* the drain has copied to ceph. For large objects the uploader's
> 30s meta-wait can expire before the drain finishes → the backend upload DLQs (`object_version=failed`)
> even though the object is on ceph and downloadable. The drain-gated upload (PR-7) fixes this; until it
> ships, keep large-object trial traffic bounded.

## Deploy & verify

```bash
kubectl kustomize k8s/staging | less          # review first
kubectl apply -k k8s/staging                  # or via the staging-deploy pipeline
kubectl -n hippius-s3-staging rollout status deploy/api-local --timeout=5m   # 2/2 Ready
kubectl -n hippius-s3-staging get pods -l app=api-local -o wide              # 2 pods, one per ingest node
kubectl -n hippius-s3-staging get deploy api                                 # base api 0/0
kubectl -n hippius-s3-staging get endpoints api                              # 2 endpoint IPs (local pods)
kubectl -n hippius-s3-staging get ds drain-agent                            # 2/2 ready (node2, node3)

# watch a local pod's ingest dir fill on PUT; the object appears on ceph once the drain-agent copies it
kubectl -n hippius-s3-staging exec deploy/api-local -c api -- sh -c 'ls -R /var/lib/hippius/local_object_cache | head'
# watch replication progress (pending→replicated) in the cephor table:
kubectl -n hippius-s3-staging exec postgres-1 -c postgres -- psql -U postgres -d hippius -tAc \
  "SELECT status, node_id, count(*) FROM cephor_replication_status GROUP BY 1,2 ORDER BY 1;"
```

The `drain-agent` copies local→ceph (meta-last) + rings `notify:`; a cross-node GET returns the object
once its parts are `replicated`. (Backend upload to Arion/OVH is still PUT-time enqueued — see the PR-7
caveat above.)

## Teardown

```bash
# in kustomization.yaml: remove the api-local-deployments resource + the inline api Service-selector
# patch (restores selector to app: api); in resource-limits.yaml: set base api replicas back to 2.
kubectl apply -k k8s/staging
# optionally clear the node dirs: rm -rf /var/lib/hippius/local_ingest_staging/* on each node via a privileged pod
# and clear the labels: kubectl label node k8s-v3-node2 k8s-v3-node3 s3-staging-local-ingest-
```

## Notes / risks

- Staging shares the `hippius` cluster with **prod** — keep ingest off node6-cache / psql (handled by
  the nodeAffinity allow-list + psql taints).
- The cache env vars are set **per-deployment**, never in the shared configmap — flipping the
  configmap would break any ceph pod (none run during the trial; relevant on teardown).
