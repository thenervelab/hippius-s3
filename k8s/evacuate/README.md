# Evacuate-to-ceph patches

Strategic-merge patches applied to the **live** prod deployments by `scripts/evacuate_cache.sh`
when the NVMe cache node (`k8s-v3-node6-cache`) is failing. Each patch is the exact inverse of
`k8s/production/local-cache-patch.yaml`:

- remove the `kubernetes.io/hostname` node pin (`nodeSelector: null`),
- remove the `local-cache` volume + mount (mandatory: the local PV's nodeAffinity would otherwise
  still force scheduling onto the broken node),
- mount the ceph `object-cache` volume read-write (it is read-only in nvme mode).

`configmap-ceph.yaml` flips the cache env (`HIPPIUS_OBJECT_CACHE_DIR` → ceph path, fallback
cleared). ConfigMap changes do not restart pods — the script issues `rollout restart` after
patching.

**Failback is the normal production deploy**: CI re-renders the manifests with the local-cache
patches, `kubectl apply` restores the pin/volumes/env (all fields here exist in the rendered
config, so three-way apply reverts them cleanly). Do not hand-revert these patches.

See `disk-failure-playbook.md` at the repo root.
