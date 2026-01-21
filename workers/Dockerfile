ARG BASE_IMAGE=ghcr.io/thenervelab/hippius-s3/base:latest
FROM ${BASE_IMAGE}

COPY . .

COPY start-worker.sh /start-worker.sh
RUN chmod +x /start-worker.sh

RUN cat > /entrypoint.sh <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

# Configure hippius key storage
echo "Configuring hippius key storage..." >&2
hippius config set key_storage enabled True >/dev/null 2>&1
if [ -n "${HIPPIUS_KEYSTORE_DATABASE_URL:-}" ]; then
  hippius config set key_storage database_url "${HIPPIUS_KEYSTORE_DATABASE_URL}" >/dev/null 2>&1
else
  hippius config set key_storage database_url "${DATABASE_URL}" >/dev/null 2>&1
fi

echo "Configuring hippius IPFS URLs..." >&2
if [ -n "${HIPPIUS_IPFS_API_URLS:-}" ]; then
  first_api_url="${HIPPIUS_IPFS_API_URLS%%,*}"
  echo "Setting IPFS API URL (first of HIPPIUS_IPFS_API_URLS) to: ${first_api_url}" >&2
  hippius config set ipfs api_url "${first_api_url}" >/dev/null 2>&1
fi

exec "$@"
EOF

RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
CMD ["/start-worker.sh"]
