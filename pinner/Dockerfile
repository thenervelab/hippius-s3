FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install build dependencies and clean up in same layer to reduce image size
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    make \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Copy the entire project including pyproject.toml
COPY . .

# Install Python dependencies from pyproject.toml
RUN pip install --no-cache-dir -e . \
    && apt-get remove -y gcc g++ make \
    && apt-get autoremove -y

# Set Python path
ENV PYTHONPATH=/app

# Create a startup script that configures hippius
RUN echo '#!/bin/bash\nset -e\n\n# Configure hippius key storage\necho "Configuring hippius key storage..." >&2\nhippius config set key_storage enabled True >/dev/null 2>&1\nhippius config set key_storage database_url "${DATABASE_URL/hippius/hippius_keys}" >/dev/null 2>&1\n\n# Configure hippius to use IPFS URLs from environment\necho "Configuring hippius IPFS URLs..." >&2\nif [ -n "$HIPPIUS_IPFS_STORE_URL" ]; then\n  echo "Setting IPFS API URL to: $HIPPIUS_IPFS_STORE_URL" >&2\n  hippius config set ipfs api_url "$HIPPIUS_IPFS_STORE_URL" >/dev/null 2>&1\nfi\nif [ -n "$HIPPIUS_IPFS_GET_URL" ]; then\n  echo "Setting IPFS gateway URL to: $HIPPIUS_IPFS_GET_URL" >&2\n  hippius config set ipfs gateway "$HIPPIUS_IPFS_GET_URL" >/dev/null 2>&1\nfi\n\n# Execute the provided command\nexec "$@"' > /entrypoint.sh && \
    chmod +x /entrypoint.sh

# Set the entrypoint
ENTRYPOINT ["/entrypoint.sh"]
