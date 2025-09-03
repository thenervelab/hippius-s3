FROM python:3.11-slim

WORKDIR /app

# Install dependencies including dbmate, gcc, and build tools
RUN apt-get update && apt-get install -y \
    curl \
    ca-certificates \
    gcc \
    g++ \
    make \
    && curl -fsSL -o /usr/local/bin/dbmate https://github.com/amacneil/dbmate/releases/latest/download/dbmate-linux-amd64 \
    && chmod +x /usr/local/bin/dbmate \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Copy the entire project
COPY . .

# Install Python dependencies
RUN pip install --no-cache-dir -e .

# Set environment variables
ENV PYTHONUNBUFFERED=1

# Create a startup script that runs dbmate, configures hippius, and then uvicorn
RUN echo '#!/bin/bash\nset -e\n\n# Apply database migrations\necho "Applying database migrations..." >&2\ndbmate wait >/dev/null 2>&1\ndbmate up >/dev/null 2>&1\n\n# Configure hippius key storage\necho "Configuring hippius key storage..." >&2\nhippius config set key_storage enabled True >/dev/null 2>&1\nhippius config set key_storage database_url "${DATABASE_URL/hippius/hippius_keys}" >/dev/null 2>&1\n\n# Configure hippius to use IPFS URLs from environment\necho "Configuring hippius IPFS URLs..." >&2\nif [ -n "$HIPPIUS_IPFS_STORE_URL" ]; then\n  echo "Setting IPFS API URL to: $HIPPIUS_IPFS_STORE_URL" >&2\n  hippius config set ipfs api_url "$HIPPIUS_IPFS_STORE_URL" >/dev/null 2>&1\nfi\nif [ -n "$HIPPIUS_IPFS_GET_URL" ]; then\n  echo "Setting IPFS gateway URL to: $HIPPIUS_IPFS_GET_URL" >&2\n  hippius config set ipfs gateway "$HIPPIUS_IPFS_GET_URL" >/dev/null 2>&1\nfi\n\n# Run the application\nexec uvicorn hippius_s3.main:app --host 0.0.0.0 --port 8000 --reload --reload-dir /app' > /start.sh && \
    chmod +x /start.sh

# Run the startup script
CMD ["/start.sh"]
