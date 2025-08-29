FROM python:3.11-slim

WORKDIR /app

# Copy the entire project
COPY . .

# Install Python dependencies
RUN pip install --no-cache-dir -e .

# Set environment variables
ENV PYTHONUNBUFFERED=1

# Create a startup script that configures hippius and runs pinner
RUN echo '#!/bin/bash\nset -e\n\n# Configure hippius key storage\necho "Configuring hippius key storage..."\nhippius config set key_storage enabled True\nhippius config set key_storage database_url postgresql://postgres:postgres@db:5432/hippius_keys\n\n# Configure hippius to use IPFS URLs from environment\necho "Configuring hippius IPFS URLs..."\nif [ -n "$HIPPIUS_IPFS_STORE_URL" ]; then\n  echo "Setting IPFS API URL to: $HIPPIUS_IPFS_STORE_URL"\n  hippius config set ipfs api_url "$HIPPIUS_IPFS_STORE_URL"\nfi\nif [ -n "$HIPPIUS_IPFS_GET_URL" ]; then\n  echo "Setting IPFS gateway URL to: $HIPPIUS_IPFS_GET_URL"\n  hippius config set ipfs gateway "$HIPPIUS_IPFS_GET_URL"\nfi\n\n# Run the pinner\nexec python pinner/run_pinner_in_loop.py' > /start-pinner.sh && \
    chmod +x /start-pinner.sh

# Run the startup script
CMD ["/start-pinner.sh"]
