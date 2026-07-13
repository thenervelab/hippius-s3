# k8s/chaos-testing — staging chaos tooling (NEVER prod)

Installs the two pieces the drain chaos matrix needs, **scoped to `hippius-s3-staging` only**:

1. **Chaos Mesh** (Helm chart `2.7.3`, namespaced mode) — the controller + CRDs
   (`PodChaos`, `NetworkChaos`, `IOChaos`, `TimeChaos`) that the `k8s/chaos/f*.yaml` cells apply.
2. **Toxiproxy** (`toxiproxy.yaml`) — a transport-level fault proxy for the F3 (redis-queues
   pathology) and F8 (arion body-truncation) cells that use a toxic instead of a CRD.

Together these unblock `stress-test/faults/` (F1–F8) against staging. Before this, both were absent —
`kubectl api-resources | grep chaos` was empty and there was no toxiproxy in the namespace, so the
matrix could not run.

## Why this cannot touch prod

Prod and staging share one cluster, so containment is the whole point:

- Chaos Mesh is installed with `clusterScoped: false` + `controllerManager.targetNamespace:
  hippius-s3-staging` (see `chaos-mesh.values.yaml`). Its fault-injection permission is a
  **RoleBinding in `hippius-s3-staging`**, not a ClusterRole — a `PodChaos`/`NetworkChaos` aimed at
  `hippius-s3-prod` is rejected by the controller.
- `enableFilterNamespace: true` adds a second gate: only a namespace labelled
  `chaos-mesh.org/inject=enabled` can be injected, and the workflow labels **only**
  `hippius-s3-staging`.
- Nothing here is referenced by any prod overlay (`k8s/production`) or by
  `.github/workflows/production-deploy.yaml`. Only the **staging** workflow installs it.

## How it deploys

`.github/workflows/staging-deploy.yaml` (staging only) runs, after the app apply:

```bash
helm repo add chaos-mesh https://charts.chaos-mesh.org
helm upgrade --install chaos-mesh chaos-mesh/chaos-mesh \
  --namespace hippius-s3-staging --version 2.7.3 \
  -f k8s/chaos-testing/chaos-mesh.values.yaml --wait --timeout 5m
kubectl label namespace hippius-s3-staging chaos-mesh.org/inject=enabled --overwrite
kubectl apply -f k8s/chaos-testing/toxiproxy.yaml
```

`helm upgrade --install` is idempotent, so re-running the deploy is a no-op when nothing changed.

## Running the chaos matrix after it deploys

```bash
# CRD cells (F1,F2,F4,F5,F6,F7 and F8's IOChaos variant) — no port-forward needed:
HIPPIUS_DRAIN_LIVE=1 bash stress-test/faults/run_chaos.sh F1 F2

# toxic cells (F3 redis, F8 arion limit_data) — expose the control API first:
kubectl -n hippius-s3-staging port-forward svc/toxiproxy 8474:8474 &
HIPPIUS_DRAIN_LIVE=1 TOXI=http://localhost:8474 bash stress-test/faults/run_chaos.sh F8
```

`HIPPIUS_DRAIN_LIVE=1` is the harness's safety latch (it refuses to inject without it). Each cell runs
under `inv-guard` — clear the 2 standing orphan `pending` `cephor_replication_status` rows first, or
inv-guard's `--abort` will trip on them instead of on the injected fault.

## Uninstall

```bash
kubectl delete -f k8s/chaos-testing/toxiproxy.yaml
helm uninstall chaos-mesh -n hippius-s3-staging
kubectl label namespace hippius-s3-staging chaos-mesh.org/inject-
```
