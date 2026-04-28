#!/usr/bin/env bash
# Print the kubectl recipes for fetching the FRONTEND_HMAC_SECRET that the
# sub-token smoke tests need. The secret lives in the `hippius-s3-secrets`
# secret of the `hippius-s3-staging` / `hippius-s3-prod` namespaces and is
# the same across regions per environment.
set -euo pipefail

cat <<'EOF'
# Staging:
export FRONTEND_HMAC_SECRET=$(kubectl get secret -n hippius-s3-staging hippius-s3-secrets -o jsonpath='{.data.FRONTEND_HMAC_SECRET}' | base64 -d)

# Production:
export FRONTEND_HMAC_SECRET=$(kubectl get secret -n hippius-s3-prod hippius-s3-secrets -o jsonpath='{.data.FRONTEND_HMAC_SECRET}' | base64 -d)
EOF
