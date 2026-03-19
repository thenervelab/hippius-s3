#!/bin/bash
set -euo pipefail

UVICORN_HOST=${UVICORN_HOST:-0.0.0.0}
UVICORN_PORT=${UVICORN_PORT:-8000}
UVICORN_WORKERS=${UVICORN_WORKERS:-1}
UVICORN_LOG_LEVEL=${UVICORN_LOG_LEVEL:-info}
UVICORN_MAX_REQUESTS=${UVICORN_MAX_REQUESTS:-10000}
UVICORN_MAX_REQUESTS_JITTER=${UVICORN_MAX_REQUESTS_JITTER:-1000}

echo "Running database migrations..."
python -m hippius_s3.scripts.migrate

RELOAD_FLAG=""
if [ "${DEBUG:-false}" = "true" ]; then
    RELOAD_FLAG="--reload"
    echo "DEBUG mode enabled - auto-reload is ON"
fi

echo "Starting hippius-s3-api via uvicorn (workers=$UVICORN_WORKERS, max_requests=$UVICORN_MAX_REQUESTS)"
uvicorn \
    --host=$UVICORN_HOST \
    --port=$UVICORN_PORT \
    --workers=$UVICORN_WORKERS \
    --loop=uvloop \
    --log-level=$UVICORN_LOG_LEVEL \
    --access-log \
    --limit-max-requests=$UVICORN_MAX_REQUESTS \
    --limit-max-requests-jitter=$UVICORN_MAX_REQUESTS_JITTER \
    --factory \
    $RELOAD_FLAG \
    hippius_s3.main:factory
