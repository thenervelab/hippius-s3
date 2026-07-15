import logging
import os
import sys
from typing import Protocol

from loki_logger_handler.loki_logger_handler import LokiLoggerHandler

from hippius_s3.services.ray_id_service import ray_id_context


class LoggingConfig(Protocol):
    log_level: str
    loki_enabled: bool
    loki_url: str
    environment: str


class RayIDFilter(logging.Filter):
    """Logging filter that ensures ray_id is always present in log records.

    Reads ray_id from contextvar if not already in the record.
    If no ray_id in contextvar, defaults to 'no-ray-id'.
    This ensures the log format string never fails even when ray_id is missing.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        if not hasattr(record, "ray_id"):
            record.ray_id = ray_id_context.get()
        return True


class HealthCheckFilter(logging.Filter):
    """Suppress uvicorn access log entries for /health endpoints."""

    def filter(self, record: logging.LogRecord) -> bool:
        message = record.getMessage()
        return "/health" not in message


class OtelExporterBackpressureFilter(logging.Filter):
    """Downgrade OTLP exporter backpressure ERRORs to WARNING (keep visible, off ERROR dashboards)."""

    _LOGGER_PREFIX = "opentelemetry.exporter.otlp.proto.grpc"

    def filter(self, record: logging.LogRecord) -> bool:
        # The OTLP gRPC exporter logs "Failed to export ... StatusCode.UNAVAILABLE" at ERROR when the
        # collector's memory_limiter sheds load — transient backpressure, not a service error. Scoped
        # by record name because this filter is attached to the shared handlers (below), not to the
        # exporter logger: a logger-level filter would NOT see records propagated from the exporter's
        # child loggers (...grpc.trace_exporter / .metric_exporter / .exporter).
        if record.name.startswith(self._LOGGER_PREFIX) and record.levelno >= logging.ERROR:
            record.levelno = logging.WARNING
            record.levelname = "WARNING"
        return True


def setup_loki_logging(config: LoggingConfig, service_name: str, include_ray_id: bool = True) -> logging.Logger:
    """
    Configure logging with optional Loki handler and ray ID support.

    Args:
        config: Application configuration
        service_name: Name of the service (e.g., "api", "uploader", "substrate")
        include_ray_id: Whether to include ray_id in log format (default: True)

    Returns:
        Configured logger instance
    """
    log_level = getattr(logging, config.log_level.upper(), logging.INFO)

    handlers: list[logging.Handler] = [logging.StreamHandler(sys.stdout)]

    if config.loki_enabled and config.loki_url:
        loki_handler = LokiLoggerHandler(
            url=config.loki_url,
            labels={
                "service": service_name,
                "environment": config.environment,
                "host": os.getenv("HOSTNAME", "unknown"),
            },
            timeout=10,
            compressed=True,
        )
        handlers.append(loki_handler)

    if include_ray_id:
        ray_id_filter = RayIDFilter()
        for handler in handlers:
            handler.addFilter(ray_id_filter)
        log_format = "%(asctime)s - [%(ray_id)s] - %(name)s - %(levelname)s - %(message)s"
    else:
        log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

    # Attached to the handlers (like RayIDFilter) so it also catches records propagated up from the
    # OTLP exporter's child loggers — a logger-level filter would miss those.
    otel_backpressure_filter = OtelExporterBackpressureFilter()
    for handler in handlers:
        handler.addFilter(otel_backpressure_filter)

    logging.basicConfig(
        level=log_level,
        format=log_format,
        handlers=handlers,
    )

    # Suppress noisy /health access log lines from uvicorn
    logging.getLogger("uvicorn.access").addFilter(HealthCheckFilter())

    return logging.getLogger(service_name)
