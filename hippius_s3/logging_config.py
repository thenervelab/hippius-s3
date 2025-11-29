import logging
import os
import sys
from typing import Protocol

from loki_logger_handler.loki_logger_handler import LokiLoggerHandler


class LoggingConfig(Protocol):
    log_level: str
    loki_enabled: bool
    loki_url: str
    environment: str


class RayIDFilter(logging.Filter):
    """Logging filter that ensures ray_id is always present in log records.

    If ray_id is not in the record, defaults to 'no-ray-id'.
    This ensures the log format string never fails even when ray_id is missing.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        if not hasattr(record, "ray_id"):
            record.ray_id = "no-ray-id"
        return True


def setup_loki_logging(config: LoggingConfig, service_name: str) -> logging.Logger:
    """
    Configure logging with optional Loki handler and ray ID support.

    Args:
        config: Application configuration
        service_name: Name of the service (e.g., "api", "uploader", "substrate")

    Returns:
        Configured logger instance
    """
    log_level = getattr(logging, config.log_level.upper(), logging.INFO)

    ray_id_filter = RayIDFilter()

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

    for handler in handlers:
        handler.addFilter(ray_id_filter)

    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - [%(ray_id)s] - %(name)s - %(levelname)s - %(message)s",
        handlers=handlers,
    )

    return logging.getLogger(service_name)
