import json
import logging
from unittest.mock import Mock
from unittest.mock import patch

import pytest

from hippius_s3.services.audit_service import AuditLogger


@pytest.fixture
def audit_logger():
    return AuditLogger()


@pytest.fixture
def sample_audit_data():
    return {
        "client_ip": "192.168.1.100",
        "user_agent": "aws-cli/2.0.0",
        "account_id": "test-account",
        "method": "GET",
        "path": "/test-bucket/test-key",
        "query_params": {"prefix": "test"},
        "status_code": 200,
        "processing_time_ms": 123.45,
        "content_length": 1024,
        "timestamp": 1234567890.12,
    }


def test_should_skip_metrics_endpoint(audit_logger):
    assert audit_logger.should_skip("/metrics", "192.168.1.100") is True


def test_should_skip_health_endpoint_internal_ip(audit_logger):
    assert audit_logger.should_skip("/health", "127.0.0.1") is True
    assert audit_logger.should_skip("/health", "localhost") is True
    assert audit_logger.should_skip("/health", "172.20.0.5") is True


def test_should_not_skip_health_endpoint_external_ip(audit_logger):
    assert audit_logger.should_skip("/health", "192.168.1.100") is False


def test_should_skip_root_endpoint_internal_ip(audit_logger):
    assert audit_logger.should_skip("/", "127.0.0.1") is True
    assert audit_logger.should_skip("/", "172.20.0.5") is True


def test_should_not_skip_root_endpoint_external_ip(audit_logger):
    assert audit_logger.should_skip("/", "192.168.1.100") is False


def test_should_not_skip_regular_endpoint(audit_logger):
    assert audit_logger.should_skip("/test-bucket/test-key", "192.168.1.100") is False
    assert audit_logger.should_skip("/test-bucket/test-key", "127.0.0.1") is False


@patch("hippius_s3.services.audit_service.logging.getLogger")
def test_log_request_success(mock_get_logger, audit_logger, sample_audit_data):
    mock_logger = Mock()
    mock_get_logger.return_value = mock_logger
    audit_logger.logger = mock_logger

    audit_logger.log_request(**sample_audit_data)

    mock_logger.info.assert_called_once()
    call_args = mock_logger.info.call_args[0][0]
    assert call_args.startswith("S3_OPERATION_SUCCESS: ")

    log_data = json.loads(call_args.replace("S3_OPERATION_SUCCESS: ", ""))
    assert log_data["client_ip"] == "192.168.1.100"
    assert log_data["account_id"] == "test-account"
    assert log_data["status_code"] == 200
    assert log_data["processing_time_ms"] == 123.45


@patch("hippius_s3.services.audit_service.logging.getLogger")
def test_log_request_client_error(mock_get_logger, audit_logger, sample_audit_data):
    mock_logger = Mock()
    mock_get_logger.return_value = mock_logger
    audit_logger.logger = mock_logger

    sample_audit_data["status_code"] = 404
    audit_logger.log_request(**sample_audit_data)

    mock_logger.warning.assert_called_once()
    call_args = mock_logger.warning.call_args[0][0]
    assert call_args.startswith("S3_OPERATION_CLIENT_ERROR: ")


@patch("hippius_s3.services.audit_service.logging.getLogger")
def test_log_request_server_error(mock_get_logger, audit_logger, sample_audit_data):
    mock_logger = Mock()
    mock_get_logger.return_value = mock_logger
    audit_logger.logger = mock_logger

    sample_audit_data["status_code"] = 500
    audit_logger.log_request(**sample_audit_data)

    mock_logger.error.assert_called_once()
    call_args = mock_logger.error.call_args[0][0]
    assert call_args.startswith("S3_OPERATION_ERROR: ")


@patch("hippius_s3.services.audit_service.logging.getLogger")
def test_log_request_skips_metrics(mock_get_logger, audit_logger, sample_audit_data):
    mock_logger = Mock()
    mock_get_logger.return_value = mock_logger
    audit_logger.logger = mock_logger

    sample_audit_data["path"] = "/metrics"
    audit_logger.log_request(**sample_audit_data)

    mock_logger.info.assert_not_called()
    mock_logger.warning.assert_not_called()
    mock_logger.error.assert_not_called()


@patch("hippius_s3.services.audit_service.logging.getLogger")
def test_log_request_skips_health_internal_ip(mock_get_logger, audit_logger, sample_audit_data):
    mock_logger = Mock()
    mock_get_logger.return_value = mock_logger
    audit_logger.logger = mock_logger

    sample_audit_data["path"] = "/health"
    sample_audit_data["client_ip"] = "127.0.0.1"
    audit_logger.log_request(**sample_audit_data)

    mock_logger.info.assert_not_called()
    mock_logger.warning.assert_not_called()
    mock_logger.error.assert_not_called()


@patch("hippius_s3.services.audit_service.logging.getLogger")
def test_log_request_processing_time_rounding(mock_get_logger, audit_logger, sample_audit_data):
    mock_logger = Mock()
    mock_get_logger.return_value = mock_logger
    audit_logger.logger = mock_logger

    sample_audit_data["processing_time_ms"] = 123.456789
    audit_logger.log_request(**sample_audit_data)

    call_args = mock_logger.info.call_args[0][0]
    log_data = json.loads(call_args.replace("S3_OPERATION_SUCCESS: ", ""))
    assert log_data["processing_time_ms"] == 123.46
