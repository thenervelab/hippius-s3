FROM hippius-s3-base:latest

# Install dbmate for migrations
RUN apt-get update && apt-get install -y \
    curl \
    ca-certificates \
    && curl -fsSL -o /usr/local/bin/dbmate https://github.com/amacneil/dbmate/releases/latest/download/dbmate-linux-amd64 \
    && chmod +x /usr/local/bin/dbmate \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Copy the entire project
COPY . .

# Run migrations and then start the application
CMD ["sh", "-c", "python -m hippius_s3.scripts.migrate && uvicorn hippius_s3.main:app --host 0.0.0.0 --port 8000 --reload --log-level debug --access-log"]
