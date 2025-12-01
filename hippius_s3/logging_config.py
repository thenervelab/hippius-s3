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

    handlers = [logging.StreamHandler(sys.stdout)]

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

    logging.basicConfig(
        level=log_level,
        format=log_format,
        handlers=handlers,
    )

    return logging.getLogger(service_name)
