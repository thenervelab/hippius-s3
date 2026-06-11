#!/usr/bin/env bash
# evacuate_cache.sh — flip ALL cache-dependent prod workloads from the node-local NVMe cache to
# the shared ceph cache and unpin them from k8s-v3-node6-cache. Run when the cache node (NVMe
# device, mounts, or the node itself) is failing and not quickly fixable.
#
# What it does, in order:
#   1. Preflight: prove the ceph cache PVC is mountable from a NON-cache node (probe pod).
#   2. Patch the cache env ConfigMaps (hippius-s3-defaults + the live hash-suffixed
#      s3-backup-config) so the primary cache dir becomes /var/lib/hippius/object_cache (ceph).
#   3. Patch 6 deployments (api, arion-uploader, arion-downloader, janitor, backup, hydrator)
#      from k8s/evacuate/*.yaml: drop the node pin, drop the local-cache NVMe volume, mount the
#      ceph cache read-write.
#   4. Rollout-restart all 6 together and wait; print final pod placement.
#
# FAILBACK = run the normal production deploy (hippius-s3 push to k8s-production + s3-backup push
# to k8s-production). CI re-applies the local-cache patches and the nvme env values. Do NOT
# hand-revert. See disk-failure-playbook.md.
#
# Usage:
#   ./scripts/evacuate_cache.sh --dry-run          # validate everything, change nothing
#   ./scripts/evacuate_cache.sh --yes              # execute
#   ./scripts/evacuate_cache.sh --yes -n <ns>      # non-default namespace

set -euo pipefail

NS="hippius-s3-prod"
DRY_RUN=false
CONFIRM=false
CACHE_NODE="k8s-v3-node6-cache"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
EVAC_DIR="$SCRIPT_DIR/../k8s/evacuate"

HIPPIUS_DEPLOYS=(api arion-uploader arion-downloader janitor)
S3BACKUP_DEPLOYS=(backup hydrator)
ALL_DEPLOYS=("${HIPPIUS_DEPLOYS[@]}" "${S3BACKUP_DEPLOYS[@]}")

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run) DRY_RUN=true; shift ;;
    --yes) CONFIRM=true; shift ;;
    -n|--namespace) NS="$2"; shift 2 ;;
    *) echo "unknown arg: $1"; exit 2 ;;
  esac
done

log() { printf '\n==> %s\n' "$*"; }

log "EVACUATE CACHE -> CEPH (namespace=$NS, dry_run=$DRY_RUN)"
log "Current state"
"$SCRIPT_DIR/evacuate_status.sh" -n "$NS" || true

# ---------------------------------------------------------------- 1. preflight
log "Preflight: probing ceph PVC mount from a non-cache node"
if $DRY_RUN; then
  echo "(dry-run: skipping probe pod creation)"
else
  kubectl -n "$NS" delete pod evacuate-preflight --ignore-not-found --wait=false >/dev/null 2>&1 || true
  kubectl -n "$NS" apply -f - <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: evacuate-preflight
spec:
  restartPolicy: Never
  affinity:
    nodeAffinity:
      requiredDuringSchedulingIgnoredDuringExecution:
        nodeSelectorTerms:
          - matchExpressions:
              - key: kubernetes.io/hostname
                operator: NotIn
                values: ["$CACHE_NODE"]
  containers:
    - name: probe
      image: busybox:1.35
      command: ["sh", "-c", "touch /cache/.evacuate-preflight && rm /cache/.evacuate-preflight && echo CEPH_MOUNT_OK"]
      volumeMounts:
        - name: object-cache
          mountPath: /cache
  volumes:
    - name: object-cache
      persistentVolumeClaim:
        claimName: object-cache-pvc
EOF
  for i in $(seq 1 24); do
    phase=$(kubectl -n "$NS" get pod evacuate-preflight -o jsonpath='{.status.phase}' 2>/dev/null || echo Pending)
    [[ "$phase" == "Succeeded" ]] && break
    [[ "$phase" == "Failed" ]] && { echo "PREFLIGHT FAILED — ceph cache not mountable off-node. Aborting."; kubectl -n "$NS" logs evacuate-preflight | tail -5; exit 1; }
    sleep 5
  done
  [[ "$phase" == "Succeeded" ]] || { echo "PREFLIGHT TIMED OUT after 120s. Aborting."; exit 1; }
  kubectl -n "$NS" delete pod evacuate-preflight --wait=false >/dev/null
  echo "preflight OK — ceph cache mountable from non-cache nodes"
fi

if ! $DRY_RUN && ! $CONFIRM; then
  echo
  read -r -p "Proceed with evacuation of ${ALL_DEPLOYS[*]} in $NS? [type 'evacuate'] " ans
  [[ "$ans" == "evacuate" ]] || { echo "aborted"; exit 1; }
fi

PATCH_FLAGS=()
$DRY_RUN && PATCH_FLAGS+=(--dry-run=server)

# ------------------------------------------------------- 2. configmap env flip
log "Patching hippius-s3-defaults ConfigMap (cache env -> ceph)"
kubectl -n "$NS" patch configmap hippius-s3-defaults "${PATCH_FLAGS[@]}" \
  --patch-file "$EVAC_DIR/configmap-ceph.yaml"

log "Patching live s3-backup-config ConfigMap (OBJECT_CACHE_DIR -> ceph)"
S3B_CM=$(kubectl -n "$NS" get deploy backup \
  -o jsonpath='{.spec.template.spec.containers[0].envFrom[0].configMapRef.name}' 2>/dev/null || true)
if [[ -n "$S3B_CM" ]]; then
  kubectl -n "$NS" patch configmap "$S3B_CM" "${PATCH_FLAGS[@]}" --type merge -p \
    '{"data":{"OBJECT_CACHE_DIR":"/var/lib/hippius/object_cache","OBJECT_CACHE_FALLBACK_DIR":""}}'
else
  echo "WARNING: could not resolve s3-backup-config from deploy/backup — skipping (check manually)"
fi

# ------------------------------------------------------ 3. deployment patches
for d in "${ALL_DEPLOYS[@]}"; do
  log "Patching deployment/$d (unpin + drop local-cache + ceph RW)"
  kubectl -n "$NS" patch deployment "$d" "${PATCH_FLAGS[@]}" --patch-file "$EVAC_DIR/$d.yaml"
done

$DRY_RUN && { log "DRY RUN complete — all patches validated server-side, nothing changed."; exit 0; }

# ------------------------------------------------------------- 4. roll + wait
log "Rolling all deployments (configmap changes need restarts)"
for d in "${ALL_DEPLOYS[@]}"; do
  kubectl -n "$NS" rollout restart "deployment/$d"
done

log "Waiting for rollouts"
for d in "${ALL_DEPLOYS[@]}"; do
  kubectl -n "$NS" rollout status "deployment/$d" --timeout=10m || echo "WARNING: $d rollout not complete"
done

log "Final placement (expect spread across nodes, none on $CACHE_NODE)"
kubectl -n "$NS" get pods -o wide | grep -E "$(IFS='|'; echo "${ALL_DEPLOYS[*]}")" | awk '{print $1, $3, $7}' | sort -k3

log "DONE. Next steps:"
echo "  1. Verify a PUT/GET round-trip against the public endpoint."
echo "  2. If the NVMe data is lost, run:  python -m hippius_s3.scripts.reconcile_lost_uploads --apply"
echo "  3. Failback after node repair: normal production deploys (both repos). See disk-failure-playbook.md."
