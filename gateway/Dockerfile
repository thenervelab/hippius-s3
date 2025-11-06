FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir fastapi uvicorn[standard] httpx asyncpg redis python-dotenv

COPY gateway/ ./gateway/
COPY hippius_s3/models/ ./hippius_s3/models/
COPY hippius_s3/repositories/__init__.py ./hippius_s3/repositories/
COPY hippius_s3/repositories/acl_repository.py ./hippius_s3/repositories/
COPY hippius_s3/__init__.py ./hippius_s3/

ENV PYTHONUNBUFFERED=1

CMD ["python", "-m", "gateway.main"]
