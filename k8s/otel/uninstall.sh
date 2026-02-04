#!/bin/bash
set -uo pipefail

NAMESPACE="monitoring"

echo "=== Uninstalling LGTM stack from '$NAMESPACE' namespace ==="

echo "--- Uninstalling Grafana ---"
helm uninstall grafana -n "$NAMESPACE" || true

echo "--- Uninstalling Alloy ---"
helm uninstall alloy -n "$NAMESPACE" || true

echo "--- Uninstalling Tempo ---"
helm uninstall tempo -n "$NAMESPACE" || true

echo "--- Uninstalling Loki ---"
helm uninstall loki -n "$NAMESPACE" || true

echo "--- Uninstalling Prometheus ---"
helm uninstall prometheus -n "$NAMESPACE" || true

echo "--- Deleting dashboard ConfigMap ---"
kubectl delete configmap hippius-s3-dashboards -n "$NAMESPACE" || true

echo "--- Deleting namespace ---"
kubectl delete namespace "$NAMESPACE" || true

echo ""
echo "=== Uninstall complete ==="
