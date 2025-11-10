FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir fastapi uvicorn[standard] httpx asyncpg redis python-dotenv mnemonic pydantic substrateinterface

COPY gateway/ ./gateway/
COPY hippius_s3/ ./hippius_s3/

ENV PYTHONUNBUFFERED=1

CMD ["python", "-m", "gateway.main"]
