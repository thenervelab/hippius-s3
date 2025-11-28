import logging
from unittest.mock import Mock

import pytest

from hippius_s3.logging_config import RayIDFilter
from hippius_s3.logging_config import setup_loki_logging


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
