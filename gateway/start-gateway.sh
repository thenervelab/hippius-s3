#!/bin/bash
set -euo pipefail

UVICORN_HOST=${UVICORN_HOST:-0.0.0.0}
UVICORN_PORT=${UVICORN_PORT:-8080}
UVICORN_WORKERS=${UVICORN_WORKERS:-1}
UVICORN_LOG_LEVEL=${UVICORN_LOG_LEVEL:-info}
UVICORN_MAX_REQUESTS=${UVICORN_MAX_REQUESTS:-0}
UVICORN_MAX_REQUESTS_JITTER=${UVICORN_MAX_REQUESTS_JITTER:-1000}
# Must be < terminationGracePeriodSeconds minus the preStop sleep, or the kubelet
# SIGKILLs us mid-drain and we are back to severed connections.
UVICORN_GRACEFUL_TIMEOUT=${UVICORN_GRACEFUL_TIMEOUT:-25}
# Must stay ABOVE the ATS edge's proxy.config.http.keep_alive_no_activity_timeout_out (60s
# on every host in hippius-ats), so ATS always retires a pooled origin connection before we
# do. uvicorn's default is 5s, which had it closing 12x sooner than the proxy that pools it:
# for the other 55s ATS believes those sockets are live, and a request dispatched onto one we
# just closed dies after the header is written. ATS retries that for a GET; for a PUT it is
# non-idempotent, so ATS marks the origin down instead and the client gets a hard 502
# "Next Hop Connection Failed" (apache/trafficserver#7290). Raise ATS's value and this must
# move with it.
UVICORN_KEEP_ALIVE=${UVICORN_KEEP_ALIVE:-75}

RELOAD_FLAG=""
if [ "${DEBUG:-false}" = "true" ]; then
    RELOAD_FLAG="--reload"
    echo "DEBUG mode enabled - auto-reload is ON"
fi

# In-process worker recycling (--limit-max-requests) is OFF by default. It churns a
# worker while the pod stays Ready, so the ATS edge / k8s Service routes a connection
# into the ~1-6s restart window and gets a reset -> 502 "Next Hop Connection Failed".
# Prod memory sits at ~10-15% of the pod limit with zero OOMs, so recycling bought
# nothing. Set UVICORN_MAX_REQUESTS>0 to re-enable if a real leak ever appears.
MAX_REQUESTS_ARGS=()
if [ "${UVICORN_MAX_REQUESTS}" -gt 0 ] 2>/dev/null; then
    MAX_REQUESTS_ARGS=(--limit-max-requests="$UVICORN_MAX_REQUESTS" --limit-max-requests-jitter="$UVICORN_MAX_REQUESTS_JITTER")
fi

echo "Starting hippius-s3-gateway via uvicorn (workers=$UVICORN_WORKERS, max_requests=$UVICORN_MAX_REQUESTS)"
# `exec` is load-bearing, do not drop it. Without it this script stays PID 1 and bash
# defers SIGTERM until its foreground child exits — which uvicorn never does on its own.
# The kubelet then SIGKILLs the pod at the end of terminationGracePeriodSeconds, cutting
# every in-flight client request. With exec, uvicorn is PID 1, gets the SIGTERM, and
# drains. Guarded by tests/unit/test_start_scripts_exec.py.
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
    gateway.main:factory
