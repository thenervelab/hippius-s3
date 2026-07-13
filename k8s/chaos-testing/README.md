# k8s/chaos-testing — staging chaos tooling (NEVER prod)

Installs the two pieces the drain chaos matrix needs, **scoped to `hippius-s3-staging` only**:

1. **Chaos Mesh** (Helm chart `2.7.3`, namespaced mode) — the controller + CRDs
   (`PodChaos`, `NetworkChaos`, `IOChaos`, `TimeChaos`) that the `k8s/chaos/f*.yaml` cells apply.
2. **Toxiproxy** (`toxiproxy.yaml`) — a transport-level fault proxy for the F3 redis-queues
   pathology cell (latency / hang / reset) that uses a toxic instead of a CRD. Only the in-cluster
   `redis_queues` proxy is pre-defined; no external backend address is baked into the manifest.

Together these unblock `stress-test/faults/` (F1–F8) against staging. Before this, both were absent —
`kubectl api-resources | grep chaos` was empty and there was no toxiproxy in the namespace, so the
matrix could not run.

## Why this cannot touch prod

Prod and staging share one cluster, so containment is the whole point:

- Chaos Mesh is installed with `clusterScoped: false` + `controllerManager.targetNamespace:
  hippius-s3-staging` (see `chaos-mesh.values.yaml`). Its fault-injection permission is a
  **RoleBinding in `hippius-s3-staging`**, not a ClusterRole — a `PodChaos`/`NetworkChaos` aimed at
  `hippius-s3-prod` is rejected by the controller. **This is the guarantee.**
- Nothing here is referenced by any prod overlay (`k8s/production`) or by
  `.github/workflows/production-deploy.yaml`. Only the **staging** workflow installs it. The one
  cluster-scoped object (`remotecluster-rbac.yaml`) is read-only on a chaos-mesh CRD type — no access
  over prod app workloads.

## How it deploys (chart 2.7.3 needs post-install patches)

chaos-mesh 2.7.3's namespaced mode is buggy — the chart **ignores several values** and the install
does not work out of the box on this cluster. `.github/workflows/staging-deploy.yaml` (staging only)
therefore does, after the helm install:

```bash
helm upgrade --install chaos-mesh chaos-mesh/chaos-mesh \
  --namespace hippius-s3-staging --version 2.7.3 \
  -f k8s/chaos-testing/chaos-mesh.values.yaml --timeout 5m     # NO --wait (chaos-daemon is a DaemonSet)
kubectl apply -f k8s/chaos-testing/webhook-networkpolicy.yaml  # apiserver -> webhook (allow-internal drops it)
kubectl apply -f k8s/chaos-testing/remotecluster-rbac.yaml     # the one cluster-scoped read the controller needs
kubectl -n hippius-s3-staging set env deploy/chaos-controller-manager \
  WEBHOOK_PORT=9443 ENABLE_FILTER_NAMESPACE=false              # 10250 is firewalled; filter needs cluster ns-list
kubectl -n hippius-s3-staging patch deploy chaos-controller-manager --type=json \
  -p '[{"op":"replace","path":"/spec/template/spec/containers/0/ports/0/containerPort","value":9443}]'
kubectl apply -f k8s/chaos-testing/toxiproxy.yaml
```

Everything is idempotent. **Why each patch** (all discovered live — each failure was a silent
`context deadline exceeded` / `Selected=False` / manager cache-sync abort):
- **webhook-networkpolicy** — the `allow-internal` NetworkPolicy only allows the apiserver on :8080, so
  webhook calls to the chaos controller were dropped and every chaos CRD create failed fail-closed.
- **remotecluster-rbac** — namespaced mode doesn't grant the cluster-scoped `remoteclusters` read the
  controller still requires; without it the manager aborts at startup and no reconcilers run.
- **WEBHOOK_PORT=9443** — the chart serves the webhook on 10250 (the kubelet port), firewalled
  apiserver→pod here; the Service targetPort is a named port so it follows the containerPort patch.
- **ENABLE_FILTER_NAMESPACE=false** — the namespace filter needs cluster-scoped namespace list/watch;
  `targetNamespace` already confines injection to staging, so the filter is redundant.

## Known limitation on this cluster

**Only controller-driven `PodChaos` (pod-kill / pod-failure) injects here.** Daemon-driven chaos —
`NetworkChaos`, `TimeChaos`, `IOChaos`, `StressChaos` — is created but **never reaches
`AllInjected=True`**: the chaos-daemon can't drive the node container-runtime to enter the target's
namespaces (a CRI-socket / privilege gap on these nodes). So of the matrix, the pod-kill cells (F1
agent-kill, F2 allocator-kill → the single-leader/epoch-fence headline) work; the network/time/io
cells (F2 partition, F3, F4, F5, F8-IOChaos) need the chaos-daemon runtime wired up first. The
hardened `inject.py` surfaces this correctly (it fails the cell on a non-injecting fault rather than
holding through and reporting green).

## Running the chaos matrix after it deploys

```bash
# CRD cells (F1,F2,F4,F5,F6,F7,F8) — no port-forward needed (F8 uses the IOChaos CRD):
HIPPIUS_DRAIN_LIVE=1 bash stress-test/faults/run_chaos.sh F1 F2

# toxic cell (F3 redis pathology) — expose the control API first:
kubectl -n hippius-s3-staging port-forward svc/toxiproxy 8474:8474 &
HIPPIUS_DRAIN_LIVE=1 TOXI=http://localhost:8474 bash stress-test/faults/run_chaos.sh F3
```

`HIPPIUS_DRAIN_LIVE=1` is the harness's safety latch (it refuses to inject without it). Each cell runs
under `inv-guard` — confirm the invariants are green first (no pre-existing non-terminal orphan rows),
or inv-guard's `--abort` will trip on the standing condition instead of on the injected fault.

## Uninstall

```bash
kubectl delete -f k8s/chaos-testing/toxiproxy.yaml
kubectl delete -f k8s/chaos-testing/webhook-networkpolicy.yaml
kubectl delete -f k8s/chaos-testing/remotecluster-rbac.yaml
helm uninstall chaos-mesh -n hippius-s3-staging
```
