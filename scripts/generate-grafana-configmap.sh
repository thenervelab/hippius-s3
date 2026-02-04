#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"
DASHBOARD_DIR="$REPO_ROOT/monitoring/grafana/dashboards"
OUTPUT_FILE="$REPO_ROOT/k8s/base/grafana-dashboards.yaml"

cat <<'HEADER' > "$OUTPUT_FILE"
apiVersion: v1
kind: ConfigMap
metadata:
  name: hippius-s3-dashboards
  namespace: monitoring
data:
HEADER

for file in "$DASHBOARD_DIR"/*.json; do
  basename=$(basename "$file")
  echo "  ${basename}: |" >> "$OUTPUT_FILE"
  cat "$file" | \
    sed 's/"uid": "PBFA97CFB590B2093"/"uid": "prometheus"/g' | \
    sed 's/"uid": "-- Grafana --"/"uid": "grafana"/g' | \
    sed 's/^/    /' >> "$OUTPUT_FILE"
done

echo "Generated: $OUTPUT_FILE"
echo "Dashboards included:"
grep "\.json:" "$OUTPUT_FILE"
