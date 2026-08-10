#!/usr/bin/env bash
# Read the ingest SSD on every drain node, and say how much a fill would have to write.
#
# Run this BEFORE `run.py --fill-gb N`. The number you pass to --fill-gb is a judgement about the
# cluster on the day, not something the suite may decide for itself: on staging this filesystem is
# shared with other tenants, so a self-sizing fill would consume somebody else's headroom.
#
# The two numbers that matter, and why both:
#   df free   — what the evictor and the api's fs_cache_pressure gate actually measure.
#   du cache  — how much of that this system owns, i.e. the most eviction could ever reclaim.
# A deficit larger than `du cache` is unmeetable by construction: the evictor will free everything
# it has, still miss the target, and report `starved`. That is correct behaviour, not a bug — but
# it means the run proves nothing about eviction keeping up.
#
# Pure read. Nothing here writes to the cluster.

set -euo pipefail

NS="${NS:-hippius-s3-staging}"
SSD="${CEPHOR_SSD_ROOT:-/var/lib/hippius/local_object_cache}"

reserve=$(kubectl -n "$NS" get ds drain-agent -o jsonpath='{.spec.template.spec.containers[0].env[?(@.name=="CEPHOR_EVICT_RESERVE_PERMILLE")].value}' 2>/dev/null || echo "")
headroom=$(kubectl -n "$NS" get ds drain-agent -o jsonpath='{.spec.template.spec.containers[0].env[?(@.name=="CEPHOR_EVICT_HEADROOM_PERMILLE")].value}' 2>/dev/null || echo "")
reserve="${reserve:-150}"; headroom="${headroom:-50}"

echo "namespace: $NS   path: $SSD"
echo "evictor: arms below ${reserve}‰ free, frees back to $((reserve + headroom))‰"
echo

printf "%-16s %10s %10s %8s %10s %14s %14s\n" NODE TOTAL FREE FREE% CACHE "TO-ARM" "EVICTABLE"
for entry in $(kubectl -n "$NS" get pods -l app=drain-agent --field-selector=status.phase=Running \
                 -o jsonpath='{range .items[*]}{.metadata.name}:{.spec.nodeName}{"\n"}{end}'); do
  pod="${entry%%:*}"; node="${entry##*:}"
  df_line=$(kubectl -n "$NS" exec "$pod" -c agent -- df -PB1 "$SSD" 2>/dev/null | tail -1 || true)
  [ -z "$df_line" ] && { printf "%-16s %10s\n" "$node" "unreachable"; continue; }
  total=$(echo "$df_line" | awk '{print $2}')
  free=$(echo "$df_line" | awk '{print $4}')
  cache=$(kubectl -n "$NS" exec "$pod" -c agent -- du -sb "$SSD" 2>/dev/null | awk '{print $1}' || echo 0)

  python3 - "$node" "$total" "$free" "$cache" "$reserve" <<'PY'
import sys
node, total, free, cache, reserve = sys.argv[1], int(sys.argv[2]), int(sys.argv[3]), int(sys.argv[4]), int(sys.argv[5])
G = 1e9
arm_at = total * reserve / 1000          # free bytes at which eviction arms
to_arm = max(0, free - arm_at)           # bytes that must be WRITTEN to reach it
print(f"{node:<16} {total/G:9.1f}G {free/G:9.1f}G {free/total:7.1%} {cache/G:9.2f}G "
      f"{to_arm/G:13.1f}G {cache/G:13.2f}G")
PY
done

cat <<'EOF'

TO-ARM     bytes you must write on THAT node before the evictor arms.
EVICTABLE  the most eviction could reclaim there (= cache). If TO-ARM's resulting
           deficit exceeds this, the pass will starve by construction.

The api Service round-robins across ingest nodes, so a fill spreads roughly evenly:
budget ~= TO-ARM x (number of nodes) to arm them all.

  ./ssd_disk_report.sh
  python run.py --fill-gb <N>          # N from the table above, your call
EOF
