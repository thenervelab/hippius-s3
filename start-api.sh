#!/bin/bash
set -euo pipefail

ASGI_SERVER=${ASGI_SERVER:-uvicorn}
ASGI_HOST=${ASGI_HOST:-0.0.0.0}
ASGI_PORT=${ASGI_PORT:-8000}
ASGI_WORKERS=${ASGI_WORKERS:-1}
ASGI_WORKER_CLASS=${ASGI_WORKER_CLASS:-uvloop}

export OTEL_EXPORTER_OTLP_PROTOCOL="grpc"
export OTEL_EXPORTER_OTLP_ENDPOINT=${OTEL_EXPORTER_OTLP_ENDPOINT:-http://tempo:4317}
export OTEL_SERVICE_NAME=${OTEL_SERVICE_NAME:-hippius-s3-api}
export OTEL_PYTHON_LOGGING_AUTO_INSTRUMENTATION_ENABLED=true
export OTEL_PYTHON_LOG_CORRELATION=true

echo "Running database migrations..."
python -m hippius_s3.scripts.migrate

if [[ $ASGI_SERVER == "uvicorn" ]]; then
    echo "Starting hippius-s3-api via uvicorn with OpenTelemetry instrumentation"

    opentelemetry-instrument \
        --logs_exporter otlp \
        --traces_exporter otlp \
        --metrics_exporter otlp \
        --service_name hippius-s3-api \
            uvicorn \
            --host=$ASGI_HOST \
            --port=$ASGI_PORT \
            --workers=$ASGI_WORKERS \
            --loop=$ASGI_WORKER_CLASS \
            --log-level=debug \
            --access-log \
            --factory \
            hippius_s3.main:factory

    exit 0
fi

if [[ $ASGI_SERVER == "gunicorn" ]]; then
    echo "Starting hippius-s3-api via gunicorn with OpenTelemetry instrumentation"

    opentelemetry-instrument \
        --logs_exporter otlp \
        --traces_exporter otlp \
        --metrics_exporter otlp \
        --service_name hippius-s3-api \
            gunicorn \
            --access-logfile=- \
            --workers=$ASGI_WORKERS \
            --bind=$ASGI_HOST:$ASGI_PORT \
            --worker-class=uvicorn.workers.UvicornWorker \
            "hippius_s3.main:factory()"

    exit 0
fi

echo "$ASGI_SERVER isn't available, please choose either uvicorn or gunicorn"
exit 1
