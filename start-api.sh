#!/bin/bash
set -euo pipefail

UVICORN_HOST=${UVICORN_HOST:-0.0.0.0}
UVICORN_PORT=${UVICORN_PORT:-8000}
UVICORN_WORKERS=${UVICORN_WORKERS:-1}
UVICORN_LOG_LEVEL=${UVICORN_LOG_LEVEL:-info}
UVICORN_MAX_REQUESTS=${UVICORN_MAX_REQUESTS:-0}
UVICORN_MAX_REQUESTS_JITTER=${UVICORN_MAX_REQUESTS_JITTER:-1000}

echo "Running database migrations..."
python -m hippius_s3.scripts.migrate

RELOAD_FLAG=""
if [ "${DEBUG:-false}" = "true" ]; then
    RELOAD_FLAG="--reload"
    echo "DEBUG mode enabled - auto-reload is ON"
fi

# In-process worker recycling (--limit-max-requests) is OFF by default. It churns a
# worker while the pod stays Ready, so the edge/k8s Service routes a connection into
# the ~1-6s restart window and gets a reset -> 502 "Next Hop Connection Failed". Prod
# memory sits at ~10-15% of the pod limit with zero OOMs, so recycling bought nothing.
# Set UVICORN_MAX_REQUESTS>0 to re-enable if a real leak ever appears.
MAX_REQUESTS_ARGS=()
if [ "${UVICORN_MAX_REQUESTS}" -gt 0 ] 2>/dev/null; then
    MAX_REQUESTS_ARGS=(--limit-max-requests="$UVICORN_MAX_REQUESTS" --limit-max-requests-jitter="$UVICORN_MAX_REQUESTS_JITTER")
fi

echo "Starting hippius-s3-api via uvicorn (workers=$UVICORN_WORKERS, max_requests=$UVICORN_MAX_REQUESTS)"
uvicorn \
    --host=$UVICORN_HOST \
    --port=$UVICORN_PORT \
    --workers=$UVICORN_WORKERS \
    --loop=uvloop \
    --log-level=$UVICORN_LOG_LEVEL \
    --access-log \
    "${MAX_REQUESTS_ARGS[@]+"${MAX_REQUESTS_ARGS[@]}"}" \
    --factory \
    $RELOAD_FLAG \
    hippius_s3.main:factory
