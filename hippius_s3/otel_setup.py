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


def build_resource(service_name: str) -> Resource:
    """Identify this PROCESS — not this pod — to the collector.

    configure_otel runs once per forked uvicorn worker (see the pid guard below), so
    with UVICORN_WORKERS=4 a pod holds four independent MeterProviders. If they share
    one service.instance.id, their four cumulative counters collide in a single
    accumulator slot in the collector's prometheus exporter: the exported value
    flip-flops between workers and every downward step reads as a counter reset.
    Measured on prod 2026-07-17 before this fix, 19 resets per series per 10m, and
    rate(http_requests_total) reporting 14.5k/s against a true 138/s from the gateway
    audit log — ~105x.

    There is no way to fix this from the collector side. Its accumulator enforces the
    single-writer principle: the delta-to-cumulative path only accumulates points whose
    timestamps chain from one stream, and the histogram path drops misaligned points
    outright (accumulator.go:204 and :256-272, contrib v0.96.0). So delta temporality
    does NOT help — the identity has to be unique per writer.

    An earlier comment here forbade os.getpid() to protect cardinality. That reasoning
    was wrong: this attribute is the POD NAME, which already churns completely on every
    deploy, so a pid adds no new churn axis — only a bounded 4x, and only for as long as
    a worker lives (UVICORN_MAX_REQUESTS=0 disables recycling, so workers respawn only
    on crash). The same change removed the account labels, which were 87% of this
    namespace's series, so cardinality drops sharply on net.

    Resource.create() merges OTEL_RESOURCE_ATTRIBUTES from env first and the dict passed
    here wins over it — which matters, because the deployments set
    service.instance.id=$(POD_NAME) there and that would otherwise undo this silently.
    Everything else in that env var (service.namespace, deployment.environment) still
    merges through.
    """
    return Resource.create(
        {
            "service.name": service_name,
            "service.instance.id": f"{socket.gethostname()}:{os.getpid()}",
        }
    )


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

    resource = build_resource(service_name)

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
    # AsyncPGInstrumentor creates a span PER query. On the hot PUT path (~13 DB ops/request) this
    # adds measurable per-request overhead for little benefit — the writer/endpoint already emit
    # manual spans for the key phases. Disabled by default; set ENABLE_DB_QUERY_TRACING=true to
    # re-enable per-query DB spans for debugging.
    AsyncPGInstrumentor().instrument()
    HTTPXClientInstrumentor().instrument()

    logger.info(f"OTel programmatic init complete for {service_name} (pid={os.getpid()}, endpoint={endpoint})")
