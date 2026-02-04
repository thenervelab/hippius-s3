#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$(dirname "$SCRIPT_DIR")")"
APP_NAMESPACE="${1:-hippius-s3-staging}"
NAMESPACE="monitoring"

echo "=== Installing LGTM stack in '$NAMESPACE' namespace ==="
echo "    App namespace: $APP_NAMESPACE"
echo ""

helm repo add grafana https://grafana.github.io/helm-charts
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo update

echo "--- Creating namespace ---"
kubectl apply -f "$SCRIPT_DIR/namespace.yaml"

echo "--- Creating dashboard ConfigMap ---"
DASHBOARD_DIR="$REPO_ROOT/monitoring/grafana/dashboards"

TMPFILES=()
DASHBOARD_FILES=""
for file in "$DASHBOARD_DIR"/*.json; do
  if [ -f "$file" ]; then
    basename=$(basename "$file")
    tmpfile=$(mktemp)
    TMPFILES+=("$tmpfile")
    sed \
      -e 's/"uid": "PBFA97CFB590B2093"/"uid": "prometheus"/g' \
      -e 's/"uid": "-- Grafana --"/"uid": "grafana"/g' \
      "$file" > "$tmpfile"
    DASHBOARD_FILES="$DASHBOARD_FILES --from-file=$basename=$tmpfile"
  fi
done

eval kubectl create configmap hippius-s3-dashboards \
  -n "$NAMESPACE" \
  $DASHBOARD_FILES \
  --dry-run=client -o yaml | kubectl apply -f -

rm -f "${TMPFILES[@]}" 2>/dev/null || true

echo "--- Installing Prometheus ---"
PROM_VALUES=$(mktemp)
sed "s/__APP_NAMESPACE__/$APP_NAMESPACE/g" "$SCRIPT_DIR/values/prometheus.yaml" > "$PROM_VALUES"
helm upgrade --install prometheus prometheus-community/prometheus \
  -n "$NAMESPACE" \
  -f "$PROM_VALUES"
rm -f "$PROM_VALUES"

echo "--- Installing Loki ---"
helm upgrade --install loki grafana/loki \
  -n "$NAMESPACE" \
  -f "$SCRIPT_DIR/values/loki.yaml"

echo "--- Installing Tempo ---"
helm upgrade --install tempo grafana/tempo \
  -n "$NAMESPACE" \
  -f "$SCRIPT_DIR/values/tempo.yaml"

echo "--- Installing Alloy ---"
helm upgrade --install alloy grafana/alloy \
  -n "$NAMESPACE" \
  -f "$SCRIPT_DIR/values/alloy.yaml"

echo "--- Installing Grafana ---"
if [ -z "${GRAFANA_ADMIN_PASSWORD:-}" ]; then
  echo "GRAFANA_ADMIN_PASSWORD not set, using default 'admin'"
  GRAFANA_ADMIN_PASSWORD="admin"
fi
helm upgrade --install grafana grafana/grafana \
  -n "$NAMESPACE" \
  -f "$SCRIPT_DIR/values/grafana.yaml" \
  --set adminPassword="$GRAFANA_ADMIN_PASSWORD"

echo ""
echo "=== Installation complete ==="
echo ""
echo "Port-forward commands:"
echo "  kubectl -n $NAMESPACE port-forward svc/prometheus-server 9090:80"
echo "  kubectl -n $NAMESPACE port-forward svc/loki 3100:3100"
echo "  kubectl -n $NAMESPACE port-forward svc/tempo 3200:3200"
echo "  kubectl -n $NAMESPACE port-forward svc/grafana 3000:80"
echo ""
echo "Don't forget to re-apply kustomize for OTel Collector + NetworkPolicy updates:"
echo "  kubectl apply -k k8s/staging/"
