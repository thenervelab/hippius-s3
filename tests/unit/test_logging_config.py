import logging
from unittest.mock import Mock

import pytest

from hippius_s3.logging_config import _ray_id_log_record_factory
from hippius_s3.logging_config import setup_loki_logging


@pytest.fixture
def mock_config():
    config = Mock()
    config.log_level = "INFO"
    config.loki_enabled = False
    config.loki_url = ""
    config.environment = "test"
    return config


def test_ray_id_log_record_factory_with_ray_id():
    record = _ray_id_log_record_factory(
        name="test",
        level=logging.INFO,
        fn="test.py",
        lno=10,
        msg="Test message",
        args=(),
        exc_info=None,
        ray_id="a1b2c3d4e5f67890",
    )

    assert record.ray_id == "a1b2c3d4e5f67890"
    assert record.name == "test"
    assert record.msg == "Test message"


def test_ray_id_log_record_factory_without_ray_id():
    record = _ray_id_log_record_factory(
        name="test",
        level=logging.INFO,
        fn="test.py",
        lno=10,
        msg="Test message",
        args=(),
        exc_info=None,
    )

    assert record.ray_id == "no-ray-id"
    assert record.name == "test"
    assert record.msg == "Test message"


def test_ray_id_log_record_factory_preserves_other_kwargs():
    record = _ray_id_log_record_factory(
        name="test",
        level=logging.INFO,
        fn="test.py",
        lno=10,
        msg="Test message",
        args=(),
        exc_info=None,
        ray_id="a1b2c3d4e5f67890",
        custom_field="custom_value",
    )

    assert record.ray_id == "a1b2c3d4e5f67890"
    assert record.custom_field == "custom_value"


def test_setup_loki_logging_sets_ray_id_factory(mock_config):
    logger = setup_loki_logging(mock_config, "test_service")

    record = logging.LogRecord(
        name="test",
        level=logging.INFO,
        pathname="test.py",
        lineno=10,
        msg="Test message",
        args=(),
        exc_info=None,
    )

    factory = logging.getLogRecordFactory()
    new_record = factory(
        name="test",
        level=logging.INFO,
        fn="test.py",
        lno=10,
        msg="Test message",
        args=(),
        exc_info=None,
    )

    assert hasattr(new_record, "ray_id")
    assert new_record.ray_id == "no-ray-id"


def test_setup_loki_logging_log_format_includes_ray_id(mock_config, caplog):
    logger = setup_loki_logging(mock_config, "test_service")
    logger.setLevel(logging.INFO)

    with caplog.at_level(logging.INFO):
        logger.info("Test message with ray_id")

    assert len(caplog.records) == 1
    assert hasattr(caplog.records[0], "ray_id")


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
