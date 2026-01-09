FROM hippius-s3-base:latest

COPY . .

COPY start-worker.sh /start-worker.sh
RUN chmod +x /start-worker.sh

RUN echo '#!/bin/bash\nset -e\n\n# Configure hippius key storage\necho "Configuring hippius key storage..." >&2\nhippius config set key_storage enabled True >/dev/null 2>&1\nif [ -n "$HIPPIUS_KEYSTORE_DATABASE_URL" ]; then\n  hippius config set key_storage database_url "$HIPPIUS_KEYSTORE_DATABASE_URL" >/dev/null 2>&1\nelse\n  hippius config set key_storage database_url "$DATABASE_URL" >/dev/null 2>&1\nfi\n\necho "Configuring hippius IPFS URLs..." >&2\nif [ -n "$HIPPIUS_IPFS_API_URLS" ]; then\n  first_api_url=\"${HIPPIUS_IPFS_API_URLS%%,*}\"\n  echo \"Setting IPFS API URL (first of HIPPIUS_IPFS_API_URLS) to: $first_api_url\" >&2\n  hippius config set ipfs api_url \"$first_api_url\" >/dev/null 2>&1\nfi\n\nexec \"$@\"' > /entrypoint.sh && \
    chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
CMD ["/start-worker.sh"]
