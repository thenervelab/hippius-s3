#!/bin/bash
set -euo pipefail

UVICORN_HOST=${UVICORN_HOST:-0.0.0.0}
UVICORN_PORT=${UVICORN_PORT:-8000}
UVICORN_WORKERS=${UVICORN_WORKERS:-1}
UVICORN_LOG_LEVEL=${UVICORN_LOG_LEVEL:-info}
UVICORN_MAX_REQUESTS=${UVICORN_MAX_REQUESTS:-0}
UVICORN_MAX_REQUESTS_JITTER=${UVICORN_MAX_REQUESTS_JITTER:-1000}
# Must be < terminationGracePeriodSeconds minus the preStop sleep, or the kubelet
# SIGKILLs us mid-drain and we are back to severed connections.
UVICORN_GRACEFUL_TIMEOUT=${UVICORN_GRACEFUL_TIMEOUT:-25}
# Must stay ABOVE the keepalive_expiry of every pool that holds a connection to us — today
# the ATS/HAProxy edge tier and peer chunk fetchers (originally the gateway's ForwardService
# httpx pool at 30s, before the merge removed that hop). uvicorn's default is 5s, so a
# pooling side holding sockets longer than we keep them dispatches requests onto connections
# we just closed, which die after the header is written. Whoever pools the connection must be
# the one to retire it; the server closing first is what turns an idle socket into a failed
# request.
UVICORN_KEEP_ALIVE=${UVICORN_KEEP_ALIVE:-75}

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
# `exec` is load-bearing, do not drop it. Without it this script stays PID 1 and bash
# defers SIGTERM until its foreground child exits — which uvicorn never does on its own.
# The kubelet then SIGKILLs the pod at the end of terminationGracePeriodSeconds and every
# in-flight request dies as `RemoteProtocolError: Server disconnected` at the gateway.
# With exec, uvicorn is PID 1, gets the SIGTERM, and drains. Guarded by
# tests/unit/test_start_scripts_exec.py.
exec uvicorn \
    --host=$UVICORN_HOST \
    --port=$UVICORN_PORT \
    --workers=$UVICORN_WORKERS \
    --loop=uvloop \
    --log-level=$UVICORN_LOG_LEVEL \
    --access-log \
    --timeout-graceful-shutdown="$UVICORN_GRACEFUL_TIMEOUT" \
    --timeout-keep-alive="$UVICORN_KEEP_ALIVE" \
    "${MAX_REQUESTS_ARGS[@]+"${MAX_REQUESTS_ARGS[@]}"}" \
    --factory \
    $RELOAD_FLAG \
    hippius_s3.main:factory
