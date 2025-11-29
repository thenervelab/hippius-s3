import logging
import re
from unittest.mock import Mock

import pytest

from hippius_s3.services.ray_id_service import RayIDLoggerAdapter
from hippius_s3.services.ray_id_service import generate_ray_id
from hippius_s3.services.ray_id_service import get_logger_with_ray_id


def test_generate_ray_id_format():
    ray_id = generate_ray_id()
    assert isinstance(ray_id, str)
    assert len(ray_id) == 16
    assert re.match(r"^[0-9a-f]{16}$", ray_id), f"Ray ID should be 16 hex chars, got: {ray_id}"


def test_generate_ray_id_uniqueness():
    ray_ids = {generate_ray_id() for _ in range(1000)}
    assert len(ray_ids) == 1000, "Ray IDs should be unique"


def test_generate_ray_id_lowercase():
    ray_id = generate_ray_id()
    assert ray_id == ray_id.lower(), "Ray ID should be lowercase"


def test_ray_id_logger_adapter_with_ray_id():
    logger = logging.getLogger("test")
    adapter = RayIDLoggerAdapter(logger, "a1b2c3d4e5f67890")

    assert adapter.extra["ray_id"] == "a1b2c3d4e5f67890"


def test_ray_id_logger_adapter_without_ray_id():
    logger = logging.getLogger("test")
    adapter = RayIDLoggerAdapter(logger, None)

    assert adapter.extra["ray_id"] == "no-ray-id"


def test_ray_id_logger_adapter_process():
    logger = logging.getLogger("test")
    adapter = RayIDLoggerAdapter(logger, "a1b2c3d4e5f67890")

    msg, kwargs = adapter.process("Test message", {})

    assert msg == "Test message"
    assert "extra" in kwargs
    assert kwargs["extra"]["ray_id"] == "a1b2c3d4e5f67890"


def test_ray_id_logger_adapter_process_preserves_existing_extra():
    logger = logging.getLogger("test")
    adapter = RayIDLoggerAdapter(logger, "a1b2c3d4e5f67890")

    msg, kwargs = adapter.process("Test message", {"extra": {"custom_field": "value"}})

    assert msg == "Test message"
    assert "extra" in kwargs
    assert kwargs["extra"]["ray_id"] == "a1b2c3d4e5f67890"
    assert kwargs["extra"]["custom_field"] == "value"


def test_get_logger_with_ray_id():
    adapter = get_logger_with_ray_id("test_logger", "a1b2c3d4e5f67890")

    assert isinstance(adapter, RayIDLoggerAdapter)
    assert adapter.extra["ray_id"] == "a1b2c3d4e5f67890"
    assert adapter.logger.name == "test_logger"


def test_get_logger_with_ray_id_no_id():
    adapter = get_logger_with_ray_id("test_logger", None)

    assert isinstance(adapter, RayIDLoggerAdapter)
    assert adapter.extra["ray_id"] == "no-ray-id"


def test_ray_id_logger_adapter_info_log(caplog):
    logger = logging.getLogger("test_info")
    logger.setLevel(logging.INFO)
    adapter = RayIDLoggerAdapter(logger, "a1b2c3d4e5f67890")

    with caplog.at_level(logging.INFO):
        adapter.info("Test info message")

    assert len(caplog.records) == 1
    assert caplog.records[0].ray_id == "a1b2c3d4e5f67890"
    assert caplog.records[0].message == "Test info message"


def test_ray_id_logger_adapter_error_log(caplog):
    logger = logging.getLogger("test_error")
    logger.setLevel(logging.ERROR)
    adapter = RayIDLoggerAdapter(logger, "deadbeefcafebabe")

    with caplog.at_level(logging.ERROR):
        adapter.error("Test error message")

    assert len(caplog.records) == 1
    assert caplog.records[0].ray_id == "deadbeefcafebabe"
    assert caplog.records[0].message == "Test error message"
