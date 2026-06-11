# Disk Failure Playbook — FS cache (NVMe ⇄ Ceph)

What to do when the cache node (`k8s-v3-node6-cache`) or its disks act up. Written after the
2026-06-11 incident (stale CephFS mounts → api 0/5 for ~3h).

---

## 1. How the cache is set up

Two cache volumes, one mode switch:

| | NVMe (normal mode) | Ceph (evacuated mode) |
|---|---|---|
| Volume | `local-cache-pvc` → `local-cache-pv` (40Ti **local** disk, RWO) | `object-cache-pvc` (CephFS, RWX, ~9.7Ti) |
| Mount path | `/var/lib/hippius/local_object_cache` | `/var/lib/hippius/object_cache` |
| Reachable from | **only** `k8s-v3-node6-cache` | every node |
| Primary cache env | `HIPPIUS_OBJECT_CACHE_DIR=/var/lib/hippius/local_object_cache` (+ ceph as `FALLBACK_DIR`, read-only) | `HIPPIUS_OBJECT_CACHE_DIR=/var/lib/hippius/object_cache` (no fallback) |

**Six workloads use the cache and live or die together** (they must all see the same chunks +
`meta.json`): `api`, `arion-uploader`, `arion-downloader`, `janitor` (hippius-s3) and `backup`,
`hydrator` (s3-backup). In NVMe mode all six are **hard-pinned to node6** — that's the SPOF.
Everything else (gateway, unpinner, cleanup, redis, postgres) runs elsewhere and is unaffected.

How the modes are constructed:
- **NVMe mode** = base manifests + `k8s/production/local-cache-patch.yaml` (pin + nvme volume +
  ceph demoted to RO) + `k8s/production/local-cache-configmap-patch.yaml` (env). s3-backup mirrors
  this with its own `k8s/production/local-cache-patch.yaml` + kustomization env literals.
- **Ceph mode** = exactly what the **base manifests already are** (ceph RWX mounted RW, no pin,
  default env). The evacuation just strips the production patches from the live objects.

**Detection**: the `disk-canary` deployment (pinned to node6) probes both mounts every 30s and
exports `disk_canary_mount_healthy{mount=…}`. Grafana pages on unhealthy (2m) **and on metric
absence** (5m — node too broken to run the canary, e.g. pods stuck in `Init` on bad mounts).

---

## 2. Decision tree when paged

```
canary page / pods stuck Init on node6
│
├─ Stale CephFS staging mount?  (pod events show: MountVolume.SetUp failed ... lstat ...
│  globalmount: permission denied)
│     → try the 10-minute fix first: §3 (stale-mount remediation)
│
├─ NVMe device errors / disk dead / node unresponsive, not fixable quickly?
│     → evacuate to ceph: §4
│
└─ Transient blip (canary recovered on its own)?
      → watch; no action.
```

Rule of thumb: if S3 is hard-down and the node fix isn't obviously < 30 minutes, **evacuate** —
it's reversible and rehearsed; a long outage is not.

---

## 3. Stale-mount remediation (the 2026-06-11 fix, ~10 min)

Symptoms: pods stuck `Init:0/1` on node6, events show `lstat ... globalmount: permission denied`.
The kernel CephFS session died; the staging mount is a zombie. CSI-pod restarts do NOT help.

```bash
# 1. Find the broken globalmount paths from the pod events
kubectl -n hippius-s3-prod describe pod <stuck-pod> | grep globalmount

# 2. Force-unmount them on the node via a privileged nsenter pod
kubectl debug node/k8s-v3-node6-cache -it --image=busybox -- chroot /host sh
  umount -f /var/lib/kubelet/plugins/kubernetes.io/csi/rook-ceph.cephfs.csi.ceph.com/<hash1>/globalmount
  umount -f /var/lib/kubelet/plugins/kubernetes.io/csi/rook-ceph.cephfs.csi.ceph.com/<hash2>/globalmount

# 3. Kubelet still thinks the volume is staged ("staging path is not a mountpoint"):
#    scale the consumers to 0 so kubelet unstages, then back up for a fresh NodeStageVolume.
kubectl -n hippius-s3-prod scale deploy/api --replicas=0
kubectl -n hippius-s3-prod scale deploy/api --replicas=5
# (repeat for any other deployment using the broken PVC)

# 4. Check dmesg for the root cause while you're there
kubectl debug node/k8s-v3-node6-cache -it --image=busybox -- chroot /host dmesg | grep -i ceph | tail
```

---

## 4. Redeploy everything on CEPH ONLY (evacuate)

One command. It preflights (proves the ceph PVC mounts from a non-cache node), patches the cache
env ConfigMaps, strips the pin + NVMe volume from all six deployments, mounts ceph read-write, and
rolls everything together:

```bash
cd ~/projects/hippius-s3

# see where things stand (read-only)
./scripts/evacuate_status.sh

# validate everything server-side without changing anything
./scripts/evacuate_cache.sh --dry-run

# DO IT
./scripts/evacuate_cache.sh --yes
```

What it changes (all reversible by a normal deploy):
- ConfigMap `hippius-s3-defaults`: `HIPPIUS_OBJECT_CACHE_DIR=/var/lib/hippius/object_cache`,
  `FALLBACK_DIR=""` — and the live hash-suffixed `s3-backup-config-*`: `OBJECT_CACHE_DIR` likewise.
- Deployments `api`, `arion-uploader`, `arion-downloader`, `janitor`, `backup`, `hydrator`:
  `nodeSelector` removed, `local-cache` volume removed (mandatory — the local PV's nodeAffinity
  would otherwise still force node6), `object-cache` mounted RW.
  (Patch files: `k8s/evacuate/*.yaml`.)

Afterwards:
```bash
# 1. verify spread + mode
./scripts/evacuate_status.sh

# 2. verify a PUT/GET round-trip via s3.hippius.com (any client)

# 3. if the NVMe data is gone for good: mark never-uploaded objects failed so GETs 404
#    instead of hanging (dry-run first, then --apply)
python -m hippius_s3.scripts.reconcile_lost_uploads
python -m hippius_s3.scripts.reconcile_lost_uploads --apply
```

Expectations while evacuated: ceph-speed cache (slower than NVMe), cold cache (GET misses refill
from Arion — expect elevated Arion download traffic for a while), and **no single node can take
S3 down**.

## 5. Redeploy everything on NVME ONLY (failback)

Failback is deliberately **just the normal production deploy** — CI re-renders the manifests with
the local-cache patches, which restores the pin, the NVMe volume, and the env:

```bash
# after node6/disk is repaired and verified healthy (canary green):
# hippius-s3 — push/merge the current release to the prod deploy branch:
git push origin <release>:k8s-production        # or merge via PR as usual
# s3-backup — same:
git push origin <release>:k8s-production
```

Notes:
- Do NOT hand-revert the evacuate patches — the deploy pipeline owns failback.
- Chunks written to ceph **during** the evacuation stay readable after failback: normal NVMe-mode
  config keeps ceph as `HIPPIUS_OBJECT_CACHE_FALLBACK_DIR` (read fallback) by design.
- Verify with `./scripts/evacuate_status.sh` → `MODE: NVME (normal)` and pods back on node6.

---

## 6. Quick reference

| Action | Command |
|---|---|
| Which mode are we in? | `./scripts/evacuate_status.sh` |
| Validate the flip w/o changes | `./scripts/evacuate_cache.sh --dry-run` |
| **Evacuate to ceph** | `./scripts/evacuate_cache.sh --yes` |
| Reconcile lost uploads | `python -m hippius_s3.scripts.reconcile_lost_uploads [--apply]` |
| **Failback to nvme** | normal production deploys (both repos) |
| Stale-mount quick fix | §3 above (`umount -f` + scale-cycle) |
| Canary health | Grafana: `disk_canary_mount_healthy`; logs: `DISK_CANARY_FAIL` |

Related docs: `proposal-1.md` (this design), `proposal-2.md` (standby alternative, rejected),
`proposal-3.md` (tiered-cache redesign, the long-term fix), `k8s/evacuate/README.md`.
