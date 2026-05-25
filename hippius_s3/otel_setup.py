import logging
import os
import socket

from opentelemetry import metrics
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.asyncpg import AsyncPGInstrumentor
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
from opentelemetry.instrumentation.redis import RedisInstrumentor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor


logger = logging.getLogger(__name__)

_otel_configured_pid: int | None = None


def configure_otel(service_name: str) -> None:
    """Programmatic OTel initialization, safe to call per-worker after fork.

    This replaces the opentelemetry-instrument CLI wrapper so that each
    forked uvicorn worker gets its own TracerProvider, MeterProvider, and
    auto-instrumentors. The CLI wrapper breaks with --workers N because
    the SDK initializes in the parent process and the BatchSpanProcessor
    thread doesn't survive fork.
    """
    global _otel_configured_pid
    if _otel_configured_pid == os.getpid():
        return
    _otel_configured_pid = os.getpid()

    if os.environ.get("ENABLE_MONITORING", "false").lower() not in ("true", "1", "yes"):
        logger.info("OTel programmatic init skipped (monitoring disabled)")
        return

    endpoint = os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT", "http://otel-collector:4317")

    # Resource.create() automatically merges OTEL_RESOURCE_ATTRIBUTES from env
    resource = Resource.create(
        {
            "service.name": service_name,
            "service.instance.id": f"{socket.gethostname()}-{os.getpid()}",
        }
    )

    # Traces
    tracer_provider = TracerProvider(resource=resource)
    tracer_provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=endpoint, insecure=True)))
    trace.set_tracer_provider(tracer_provider)

    # Metrics
    metric_reader = PeriodicExportingMetricReader(
        OTLPMetricExporter(endpoint=endpoint, insecure=True),
        export_interval_millis=10000,
    )
    meter_provider = MeterProvider(resource=resource, metric_readers=[metric_reader])
    metrics.set_meter_provider(meter_provider)

    # Auto-instrument libraries (replaces what opentelemetry-instrument CLI was doing)
    FastAPIInstrumentor().instrument()
    RedisInstrumentor().instrument()
    AsyncPGInstrumentor().instrument()
    HTTPXClientInstrumentor().instrument()

    logger.info(f"OTel programmatic init complete for {service_name} (pid={os.getpid()}, endpoint={endpoint})")
