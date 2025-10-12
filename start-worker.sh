#!/bin/bash
set -euo pipefail

if [ -z "${WORKER_SCRIPT:-}" ]; then
    echo "ERROR: WORKER_SCRIPT environment variable must be set"
    echo "Example: WORKER_SCRIPT=workers/run_uploader_in_loop.py"
    exit 1
fi

export OTEL_EXPORTER_OTLP_PROTOCOL="grpc"
export OTEL_EXPORTER_OTLP_ENDPOINT=${OTEL_EXPORTER_OTLP_ENDPOINT:-http://tempo:4317}
export OTEL_SERVICE_NAME=${OTEL_SERVICE_NAME:-hippius-worker}
export OTEL_PYTHON_LOGGING_AUTO_INSTRUMENTATION_ENABLED=true
export OTEL_PYTHON_LOG_CORRELATION=true

echo "Starting worker: $WORKER_SCRIPT (service: $OTEL_SERVICE_NAME) with OpenTelemetry instrumentation"

if [[ "${ENABLE_WATCHFILES:-false}" == "true" ]]; then
    opentelemetry-instrument \
        --logs_exporter otlp \
        --traces_exporter otlp \
        --metrics_exporter otlp \
        --service_name "$OTEL_SERVICE_NAME" \
            watchfiles \
            --filter python \
            "python $WORKER_SCRIPT"
else
    opentelemetry-instrument \
        --logs_exporter otlp \
        --traces_exporter otlp \
        --metrics_exporter otlp \
        --service_name "$OTEL_SERVICE_NAME" \
            python "$WORKER_SCRIPT"
fi
