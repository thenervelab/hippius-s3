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

## How it deploys

chaos-mesh 2.7.3's namespaced mode needs two **standalone** companion objects to work on this cluster
(the helm-managed Deployment itself is left untouched — patching it with kubectl would create a
field-manager that conflicts with the next `helm upgrade`, since helm 4 uses server-side apply).
`.github/workflows/staging-deploy.yaml` (staging only) runs, after the app apply:

```bash
helm upgrade --install chaos-mesh chaos-mesh/chaos-mesh \
  --namespace hippius-s3-staging --version 2.7.3 \
  -f k8s/chaos-testing/chaos-mesh.values.yaml --timeout 5m     # NO --wait (chaos-daemon is a DaemonSet)
kubectl apply -f k8s/chaos-testing/webhook-networkpolicy.yaml  # apiserver -> webhook (allow-internal drops it)
kubectl apply -f k8s/chaos-testing/remotecluster-rbac.yaml     # the one cluster-scoped read the controller needs
kubectl apply -f k8s/chaos-testing/toxiproxy.yaml
```

Everything is idempotent (no `--wait`, no Deployment patches). **Why each object** (all discovered live
— each failure was a silent `context deadline exceeded` / `Selected=False` / manager cache-sync abort):
- **webhook-networkpolicy** — the `allow-internal` NetworkPolicy only allows the apiserver on :8080, so
  webhook calls to the chaos controller (chart-default port 10250) were dropped and every chaos CRD
  create failed fail-closed. This was the ONLY thing blocking the webhook — the port itself is fine.
- **remotecluster-rbac** — namespaced mode doesn't grant the cluster-scoped `remoteclusters` read the
  controller still requires; without it the manager aborts at startup and no reconcilers run.
- **`enableFilterNamespace: false`** (in the values, not a patch) — the namespace filter needs
  cluster-scoped namespace list/watch that namespaced mode doesn't grant (→ `Selected=False`);
  `targetNamespace` already confines injection to staging, so the filter is redundant.
- **`chaosDaemon.runtime: containerd` + the k3s socket** (in the values) — the nodes run k3s
  (containerd), not docker; the chart's docker default made daemon-driven chaos (NetworkChaos /
  TimeChaos / IOChaos) create-but-never-inject. Pointing the daemon at
  `/run/k3s/containerd/containerd.sock` lets it nsenter the target and run `tc` / clock / io faults.

The full matrix (PodChaos + Network/Time/IO) injects on this cluster; verified live (F2 =
PodChaos + NetworkChaos both reach `AllInjected=True`). The hardened `inject.py` fails a cell on a
non-injecting fault rather than holding through and reporting green, so any future runtime regression
surfaces immediately.

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
