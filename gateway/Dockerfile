FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    fastapi \
    uvicorn[standard] \
    httpx \
    asyncpg \
    redis \
    python-dotenv \
    mnemonic \
    pydantic \
    substrate-interface==1.7.11 \
    opentelemetry-api>=1.37.0 \
    opentelemetry-sdk>=1.37.0 \
    opentelemetry-distro>=0.58b0 \
    opentelemetry-instrumentation>=0.58b0 \
    opentelemetry-instrumentation-fastapi>=0.58b0 \
    opentelemetry-instrumentation-redis>=0.58b0 \
    opentelemetry-instrumentation-asyncpg>=0.58b0 \
    opentelemetry-instrumentation-httpx>=0.58b0 \
    opentelemetry-exporter-prometheus>=0.58b0 \
    opentelemetry-exporter-otlp-proto-grpc>=1.37.0 \
    prometheus-client>=0.23.1 \
    loki-logger-handler>=1.1.2

COPY gateway/ ./gateway/
COPY hippius_s3/ ./hippius_s3/
COPY gateway/start-gateway.sh ./gateway/

RUN chmod +x gateway/start-gateway.sh

ENV PYTHONUNBUFFERED=1

CMD ["./gateway/start-gateway.sh"]
