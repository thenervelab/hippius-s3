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


def _ray_id_log_record_factory(
    name: str,
    level: int,
    fn: str,
    lno: int,
    msg: str,
    args: tuple,
    exc_info: tuple | None,
    func: str | None = None,
    sinfo: str | None = None,
    **kwargs: object,
) -> logging.LogRecord:
    """Custom LogRecord factory that ensures ray_id is always present.

    If ray_id is not in the record's extra data, defaults to 'no-ray-id'.
    This ensures the log format string never fails even when ray_id is missing.
    """
    record = logging.LogRecord(name, level, fn, lno, msg, args, exc_info, func, sinfo)

    if kwargs:
        record.__dict__.update(kwargs)

    if not hasattr(record, "ray_id"):
        record.ray_id = "no-ray-id"

    return record


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

    logging.setLogRecordFactory(_ray_id_log_record_factory)

    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - [%(ray_id)s] - %(name)s - %(levelname)s - %(message)s",
        handlers=handlers,
    )

    return logging.getLogger(service_name)
