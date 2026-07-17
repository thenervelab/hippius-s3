import logging
from unittest.mock import Mock

import pytest

from hippius_s3.logging_config import OtelExporterBackpressureFilter
from hippius_s3.logging_config import RayIDFilter
from hippius_s3.logging_config import setup_loki_logging


OTEL_LOGGER_NAME = "opentelemetry.exporter.otlp.proto.grpc"


def _make_record(name: str, level: int) -> logging.LogRecord:
    return logging.LogRecord(
        name=name,
        level=level,
        pathname="exporter.py",
        lineno=42,
        msg="Failed to export to collector, error code: StatusCode.UNAVAILABLE",
        args=(),
        exc_info=None,
    )


@pytest.fixture
def mock_config():
    config = Mock()
    config.log_level = "INFO"
    config.loki_enabled = False
    config.loki_url = ""
    config.environment = "test"
    return config


def test_ray_id_filter_adds_ray_id_when_missing():
    ray_filter = RayIDFilter()
    record = logging.LogRecord(
        name="test",
        level=logging.INFO,
        pathname="test.py",
        lineno=10,
        msg="Test message",
        args=(),
        exc_info=None,
    )

    result = ray_filter.filter(record)

    assert result is True
    assert hasattr(record, "ray_id")
    assert record.ray_id == "no-ray-id"


def test_ray_id_filter_preserves_existing_ray_id():
    ray_filter = RayIDFilter()
    record = logging.LogRecord(
        name="test",
        level=logging.INFO,
        pathname="test.py",
        lineno=10,
        msg="Test message",
        args=(),
        exc_info=None,
    )
    record.ray_id = "a1b2c3d4e5f67890"

    result = ray_filter.filter(record)

    assert result is True
    assert record.ray_id == "a1b2c3d4e5f67890"


def test_ray_id_filter_returns_true():
    ray_filter = RayIDFilter()
    record = logging.LogRecord(
        name="test",
        level=logging.INFO,
        pathname="test.py",
        lineno=10,
        msg="Test message",
        args=(),
        exc_info=None,
    )

    result = ray_filter.filter(record)

    assert result is True


def test_ray_id_filter_works_with_logger():
    logger = logging.getLogger("test_filter_logger")
    logger.setLevel(logging.INFO)

    handler = logging.StreamHandler()
    handler.addFilter(RayIDFilter())
    logger.addHandler(handler)

    log_records = []

    class RecordCapture(logging.Handler):
        def emit(self, record):
            log_records.append(record)

    capture_handler = RecordCapture()
    capture_handler.addFilter(RayIDFilter())
    logger.addHandler(capture_handler)

    logger.info("Test message")

    assert len(log_records) == 1
    assert hasattr(log_records[0], "ray_id")
    assert log_records[0].ray_id == "no-ray-id"

    logger.handlers.clear()


def test_ray_id_filter_works_with_extra():
    logger = logging.getLogger("test_filter_extra_logger")
    logger.setLevel(logging.INFO)

    log_records = []

    class RecordCapture(logging.Handler):
        def emit(self, record):
            log_records.append(record)

    capture_handler = RecordCapture()
    capture_handler.addFilter(RayIDFilter())
    logger.addHandler(capture_handler)

    logger.info("Test message", extra={"ray_id": "a1b2c3d4e5f67890"})

    assert len(log_records) == 1
    assert hasattr(log_records[0], "ray_id")
    assert log_records[0].ray_id == "a1b2c3d4e5f67890"

    logger.handlers.clear()


def test_setup_loki_logging_returns_logger(mock_config):
    logger = setup_loki_logging(mock_config, "test_service")

    assert isinstance(logger, logging.Logger)
    assert logger.name == "test_service"


def test_setup_loki_logging_parses_log_level(mock_config):
    mock_config.log_level = "DEBUG"
    logger = setup_loki_logging(mock_config, "test_service")

    assert isinstance(logger, logging.Logger)
    log_level_value = getattr(logging, mock_config.log_level.upper())
    assert log_level_value == logging.DEBUG


def test_setup_loki_logging_without_loki(mock_config):
    mock_config.loki_enabled = False
    logger = setup_loki_logging(mock_config, "test_service")

    assert isinstance(logger, logging.Logger)
    root_logger = logging.getLogger()
    assert len(root_logger.handlers) >= 1


OTEL_CHILD_LOGGER_NAME = "opentelemetry.exporter.otlp.proto.grpc.trace_exporter"


def test_otel_filter_downgrades_error_to_warning_for_child_logger():
    otel_filter = OtelExporterBackpressureFilter()
    record = _make_record(OTEL_CHILD_LOGGER_NAME, logging.ERROR)

    result = otel_filter.filter(record)

    assert result is True
    assert record.levelno == logging.WARNING
    assert record.levelname == "WARNING"


def test_otel_filter_downgrades_critical_to_warning():
    otel_filter = OtelExporterBackpressureFilter()
    record = _make_record(OTEL_CHILD_LOGGER_NAME, logging.CRITICAL)

    result = otel_filter.filter(record)

    assert result is True
    assert record.levelno == logging.WARNING
    assert record.levelname == "WARNING"


def test_otel_filter_leaves_warning_unchanged():
    otel_filter = OtelExporterBackpressureFilter()
    record = _make_record(OTEL_LOGGER_NAME, logging.WARNING)

    result = otel_filter.filter(record)

    assert result is True
    assert record.levelno == logging.WARNING
    assert record.levelname == "WARNING"


def test_otel_filter_leaves_info_unchanged():
    otel_filter = OtelExporterBackpressureFilter()
    record = _make_record(OTEL_LOGGER_NAME, logging.INFO)

    result = otel_filter.filter(record)

    assert result is True
    assert record.levelno == logging.INFO
    assert record.levelname == "INFO"


def test_otel_filter_leaves_non_otel_error_unchanged():
    # Name-scoped: an ERROR from an app logger must NOT be downgraded (the filter runs on the shared
    # handlers, so it sees every record and must only touch the otel exporter's own lines).
    otel_filter = OtelExporterBackpressureFilter()
    record = _make_record("hippius_s3.api", logging.ERROR)

    result = otel_filter.filter(record)

    assert result is True
    assert record.levelno == logging.ERROR
    assert record.levelname == "ERROR"


def test_otel_filter_never_drops_records():
    otel_filter = OtelExporterBackpressureFilter()
    for name in (OTEL_CHILD_LOGGER_NAME, "hippius_s3.api"):
        for level in (logging.DEBUG, logging.INFO, logging.WARNING, logging.ERROR, logging.CRITICAL):
            record = _make_record(name, level)
            assert otel_filter.filter(record) is True


def test_handler_attached_filter_downgrades_child_logger_via_propagation():
    # The regression this whole fix turns on: the noisy lines are emitted by CHILD loggers
    # (...grpc.trace_exporter). A filter on the parent logger would NOT see them (propagation skips
    # ancestor logger filters). Attached to a HANDLER, it does — and only touches otel records.
    captured: list[tuple[str, str]] = []

    class RecordCapture(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            captured.append((record.name, record.levelname))

    handler = RecordCapture()
    handler.addFilter(OtelExporterBackpressureFilter())
    root = logging.getLogger()
    prev_level = root.level
    root.addHandler(handler)
    root.setLevel(logging.DEBUG)
    try:
        logging.getLogger(OTEL_CHILD_LOGGER_NAME).error("Failed to export traces, StatusCode.UNAVAILABLE")
        logging.getLogger("hippius_s3.api.put").error("genuine service error")
    finally:
        root.removeHandler(handler)
        root.setLevel(prev_level)

    assert (OTEL_CHILD_LOGGER_NAME, "WARNING") in captured
    assert ("hippius_s3.api.put", "ERROR") in captured


def test_setup_loki_logging_attaches_otel_filter_to_handlers_not_logger(mock_config):
    root = logging.getLogger()
    saved_handlers = root.handlers[:]
    otel_logger = logging.getLogger(OTEL_LOGGER_NAME)
    otel_logger.filters = [f for f in otel_logger.filters if not isinstance(f, OtelExporterBackpressureFilter)]
    # Simulate a fresh process: basicConfig (which setup_loki_logging uses, like the RayIDFilter path)
    # only installs the filter-bearing handlers when the root logger has none yet.
    root.handlers = []
    try:
        setup_loki_logging(mock_config, "test_service")
        assert any(isinstance(f, OtelExporterBackpressureFilter) for h in root.handlers for f in h.filters)
        # Must NOT be a logger-level filter on the exporter logger — that would miss child records.
        assert not any(isinstance(f, OtelExporterBackpressureFilter) for f in otel_logger.filters)
    finally:
        root.handlers = saved_handlers
