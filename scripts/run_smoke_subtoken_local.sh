#!/usr/bin/env bash
# Run the sub-token scope smoke tests locally against the staging gateway.
#
# Expects .aws.cli.env (gitignored) to define:
#   AWS_ACCESS_KEY            — master access key (hip_*)
#   AWS_SECRET_KEY            — master secret
#   HIPPIUS_USER_TOKEN        — DRF token for the test account
#   HIPPIUS_MASTER_ACCOUNT_ID — SS58 of the test account
#   HIPPIUS_ENDPOINT          — e.g. https://s3-staging.hippius.com
#
# FRONTEND_HMAC_SECRET is fetched from kubectl on every run so it stays fresh.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

if [[ ! -f .aws.cli.env ]]; then
  echo "error: .aws.cli.env not found in repo root" >&2
  exit 1
fi
# shellcheck disable=SC1091
source .aws.cli.env

NAMESPACE="${SMOKE_NAMESPACE:-hippius-s3-staging}"
echo "Fetching FRONTEND_HMAC_SECRET from $NAMESPACE..."
FRONTEND_HMAC_SECRET=$(kubectl get secret -n "$NAMESPACE" hippius-s3-secrets -o jsonpath='{.data.FRONTEND_HMAC_SECRET}' | base64 -d)
export FRONTEND_HMAC_SECRET

: "${HIPPIUS_USER_TOKEN:?HIPPIUS_USER_TOKEN must be set in .aws.cli.env}"
: "${HIPPIUS_MASTER_ACCOUNT_ID:?HIPPIUS_MASTER_ACCOUNT_ID must be set in .aws.cli.env}"
: "${HIPPIUS_ENDPOINT:?HIPPIUS_ENDPOINT must be set in .aws.cli.env}"

echo "Running sub-token smoke tests against $HIPPIUS_ENDPOINT..."
exec pytest tests/smoke/test_smoke_subtoken_scope.py -v -m smoke_subtoken "$@"
