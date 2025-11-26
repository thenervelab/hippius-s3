#!/bin/bash
set -euo pipefail

UVICORN_HOST=${UVICORN_HOST:-0.0.0.0}
UVICORN_PORT=${UVICORN_PORT:-8000}
UVICORN_WORKERS=${UVICORN_WORKERS:-1}
UVICORN_LOG_LEVEL=${UVICORN_LOG_LEVEL:-info}

export OTEL_EXPORTER_OTLP_PROTOCOL="grpc"
export OTEL_EXPORTER_OTLP_ENDPOINT=${OTEL_EXPORTER_OTLP_ENDPOINT:-http://otel-collector:4317}
export OTEL_SERVICE_NAME=${OTEL_SERVICE_NAME:-hippius-s3-api}
export OTEL_PYTHON_LOGGING_AUTO_INSTRUMENTATION_ENABLED=false
export OTEL_PYTHON_LOG_CORRELATION=false
export OTEL_LOGS_EXPORTER=none

echo "Running database migrations..."
python -m hippius_s3.scripts.migrate

RELOAD_FLAG=""
if [ "${DEBUG:-false}" = "true" ]; then
    RELOAD_FLAG="--reload"
    echo "DEBUG mode enabled - auto-reload is ON"
fi

if [ "${ENABLE_MONITORING:-false}" = "true" ]; then
    echo "Starting hippius-s3-api via uvicorn with OpenTelemetry instrumentation"
    opentelemetry-instrument \
        --logs_exporter none \
        --traces_exporter otlp \
        --metrics_exporter otlp \
        --service_name hippius-s3-api \
            uvicorn \
            --host=$UVICORN_HOST \
            --port=$UVICORN_PORT \
            --workers=$UVICORN_WORKERS \
            --loop=uvloop \
            --log-level=$UVICORN_LOG_LEVEL \
            --access-log \
            --factory \
            $RELOAD_FLAG \
            hippius_s3.main:factory
else
    echo "Starting hippius-s3-api via uvicorn (monitoring disabled)"
    uvicorn \
        --host=$UVICORN_HOST \
        --port=$UVICORN_PORT \
        --workers=$UVICORN_WORKERS \
        --loop=uvloop \
        --log-level=$UVICORN_LOG_LEVEL \
        --access-log \
        --factory \
        $RELOAD_FLAG \
        hippius_s3.main:factory
fi
