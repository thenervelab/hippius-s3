#!/bin/bash
set -euo pipefail

if [ -z "${WORKER_SCRIPT:-}" ]; then
    echo "ERROR: WORKER_SCRIPT environment variable must be set"
    echo "Example: WORKER_SCRIPT=workers/run_uploader_in_loop.py"
    exit 1
fi

export OTEL_EXPORTER_OTLP_PROTOCOL="grpc"
export OTEL_EXPORTER_OTLP_ENDPOINT=${OTEL_EXPORTER_OTLP_ENDPOINT:-http://otel-collector:4317}
export OTEL_SERVICE_NAME=${OTEL_SERVICE_NAME:-hippius-worker}
export OTEL_PYTHON_LOGGING_AUTO_INSTRUMENTATION_ENABLED=${OTEL_PYTHON_LOGGING_AUTO_INSTRUMENTATION_ENABLED:-true}
export OTEL_PYTHON_LOG_CORRELATION=${OTEL_PYTHON_LOG_CORRELATION:-true}
export OTEL_LOGS_EXPORTER=${OTEL_LOGS_EXPORTER:-none}

echo "Starting worker: $WORKER_SCRIPT (service: $OTEL_SERVICE_NAME)"

if [[ "${OTEL_SDK_DISABLED:-false}" == "true" ]]; then
    echo "OpenTelemetry disabled; starting worker without instrumentation"
    if [[ "${ENABLE_WATCHFILES:-false}" == "true" ]]; then
        exec watchfiles --filter python python "$WORKER_SCRIPT"
    else
        exec python "$WORKER_SCRIPT"
    fi
fi

if ! command -v opentelemetry-instrument >/dev/null 2>&1; then
    echo "OpenTelemetry CLI not found; starting worker without instrumentation"
    if [[ "${ENABLE_WATCHFILES:-false}" == "true" ]]; then
        exec watchfiles --filter python python "$WORKER_SCRIPT"
    else
        exec python "$WORKER_SCRIPT"
    fi
fi

if [[ "${ENABLE_WATCHFILES:-false}" == "true" ]]; then
    exec opentelemetry-instrument \
        --logs_exporter otlp \
        --traces_exporter otlp \
        --metrics_exporter otlp \
        --service_name "$OTEL_SERVICE_NAME" \
            watchfiles \
            --filter python \
            "python $WORKER_SCRIPT"
else
    exec opentelemetry-instrument \
        --logs_exporter otlp \
        --traces_exporter otlp \
        --metrics_exporter otlp \
        --service_name "$OTEL_SERVICE_NAME" \
            python "$WORKER_SCRIPT"
fi
