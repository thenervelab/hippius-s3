#!/usr/bin/env bash
# evacuate_status.sh — read-only: report whether the cache workloads are in NVME mode (pinned to
# the cache node, local cache primary) or CEPH mode (evacuated), and where the pods are running.
set -euo pipefail

NS="hippius-s3-prod"
CACHE_NODE="k8s-v3-node6-cache"
while [[ $# -gt 0 ]]; do
  case "$1" in
    -n|--namespace) NS="$2"; shift 2 ;;
    *) echo "unknown arg: $1"; exit 2 ;;
  esac
done

DEPLOYS=(api arion-uploader arion-downloader janitor backup hydrator)

dir=$(kubectl -n "$NS" get configmap hippius-s3-defaults \
  -o jsonpath='{.data.HIPPIUS_OBJECT_CACHE_DIR}' 2>/dev/null || echo "?")
echo "HIPPIUS_OBJECT_CACHE_DIR = $dir"

nvme=0; ceph=0
for d in "${DEPLOYS[@]}"; do
  sel=$(kubectl -n "$NS" get deploy "$d" \
    -o jsonpath='{.spec.template.spec.nodeSelector.kubernetes\.io/hostname}' 2>/dev/null || echo "")
  vols=$(kubectl -n "$NS" get deploy "$d" \
    -o jsonpath='{.spec.template.spec.volumes[*].name}' 2>/dev/null || echo "")
  if [[ "$sel" == "$CACHE_NODE" && "$vols" == *local-cache* ]]; then
    mode="NVME (pinned)"; nvme=$((nvme+1))
  elif [[ -z "$sel" && "$vols" != *local-cache* ]]; then
    mode="CEPH (evacuated)"; ceph=$((ceph+1))
  else
    mode="MIXED/UNKNOWN (selector='$sel' volumes='$vols')"
  fi
  printf '  %-18s %s\n' "$d" "$mode"
done

echo
if [[ $nvme -eq ${#DEPLOYS[@]} ]]; then echo "MODE: NVME (normal)"
elif [[ $ceph -eq ${#DEPLOYS[@]} ]]; then echo "MODE: CEPH (evacuated)"
else echo "MODE: MIXED — investigate (a flip may be half-applied)"; fi

echo
echo "Pod placement:"
kubectl -n "$NS" get pods -o wide 2>/dev/null \
  | grep -E "^($(IFS='|'; echo "${DEPLOYS[*]}"))-" \
  | awk '{print $7}' | sort | uniq -c | sort -rn | sed 's/^/  /'
