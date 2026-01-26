"""Unit tests for OVH KMS client."""

import base64
from pathlib import Path
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import httpx
import pytest

from hippius_s3.services.ovh_kms_client import OVHKMSAuthenticationError
from hippius_s3.services.ovh_kms_client import OVHKMSClient
from hippius_s3.services.ovh_kms_client import OVHKMSError
from hippius_s3.services.ovh_kms_client import OVHKMSUnavailableError
from hippius_s3.services.ovh_kms_client import _compute_backoff_ms


@pytest.fixture
def mock_config():
    """Create a mock config for KMS client tests."""
    config = MagicMock()
    config.ovh_kms_endpoint = "https://eu-west-rbx.okms.ovh.net"
    config.ovh_kms_okms_id = "test-okms-id"
    config.ovh_kms_default_key_id = "test-key-id"
    config.ovh_kms_cert_path = "/path/to/client.crt"
    config.ovh_kms_key_path = "/path/to/client.key"
    config.ovh_kms_ca_path = ""
    config.ovh_kms_timeout_seconds = 30.0
    config.ovh_kms_max_retries = 3
    config.ovh_kms_retry_base_ms = 100
    config.ovh_kms_retry_max_ms = 1000
    return config


@pytest.fixture
def mock_cert_paths():
    """Patch Path.exists() to return True for cert paths (avoids 30s wait)."""
    original_exists = Path.exists

    def patched_exists(self):
        if str(self) in ["/path/to/client.crt", "/path/to/client.key", "/path/to/ca.crt"]:
            return True
        return original_exists(self)

    with patch.object(Path, "exists", patched_exists):
        yield


class TestComputeBackoff:
    """Tests for backoff calculation."""

    def test_first_attempt_uses_base(self):
        """First attempt should use base + positive jitter (0-10%)."""
        backoff = _compute_backoff_ms(1, base_ms=500, max_ms=5000)
        # jitter is uniform(0, 50), so result is 500..550
        assert 500 <= backoff <= 550

    def test_second_attempt_doubles(self):
        """Second attempt should use 2x base + positive jitter."""
        backoff = _compute_backoff_ms(2, base_ms=500, max_ms=5000)
        # jitter is uniform(0, 100), so result is 1000..1100
        assert 1000 <= backoff <= 1100

    def test_respects_max(self):
        """Backoff should never exceed max."""
        backoff = _compute_backoff_ms(10, base_ms=500, max_ms=5000)
        assert backoff <= 5000


@pytest.mark.asyncio
class TestOVHKMSClient:
    """Tests for OVHKMSClient."""

    @pytest.fixture(autouse=True)
    def auto_mock_cert_paths(self, mock_cert_paths):
        """Auto-use the mock_cert_paths fixture for all tests in this class."""
        pass

    async def test_generate_data_key_success(self, mock_config):
        """Generate returns plaintext and wrapped JWE."""
        expected_plaintext = b"plaintext-kek-32-bytes-12345678"
        expected_wrapped_jwe = "mock-jwe.wrapped-data"

        mock_response = MagicMock()
        mock_response.status_code = 201
        mock_response.json.return_value = {
            "plaintext": base64.b64encode(expected_plaintext).decode(),
            "key": expected_wrapped_jwe,
        }

        with patch("httpx.AsyncClient") as MockClient:
            mock_client_instance = AsyncMock()
            mock_client_instance.request = AsyncMock(return_value=mock_response)
            MockClient.return_value = mock_client_instance

            client = OVHKMSClient(config=mock_config)
            plaintext, wrapped = await client.generate_data_key(key_id="test-key-id")

            assert plaintext == expected_plaintext
            assert wrapped == expected_wrapped_jwe
            mock_client_instance.request.assert_called_once()
            call_args = mock_client_instance.request.call_args
            assert call_args[0][0] == "POST"
            # Verify full path format: /api/{okms_id}/v1/servicekey/{key_id}/datakey
            assert call_args[0][1] == "/api/test-okms-id/v1/servicekey/test-key-id/datakey"

    async def test_decrypt_data_key_success(self, mock_config):
        """Decrypt returns base64-decoded plaintext."""
        wrapped_jwe = "mock-jwe.wrapped-data"
        expected_plaintext = b"plaintext-kek-32-bytes-12345678"

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "plaintext": base64.b64encode(expected_plaintext).decode()
        }

        with patch("httpx.AsyncClient") as MockClient:
            mock_client_instance = AsyncMock()
            mock_client_instance.request = AsyncMock(return_value=mock_response)
            MockClient.return_value = mock_client_instance

            client = OVHKMSClient(config=mock_config)
            result = await client.decrypt_data_key(wrapped_jwe, key_id="test-key-id")

            assert result == expected_plaintext
            mock_client_instance.request.assert_called_once()
            call_args = mock_client_instance.request.call_args
            assert call_args[0][0] == "POST"
            # Verify full path format: /api/{okms_id}/v1/servicekey/{key_id}/datakey/decrypt
            assert call_args[0][1] == "/api/test-okms-id/v1/servicekey/test-key-id/datakey/decrypt"

    async def test_generate_data_key_uses_provided_key_id(self, mock_config):
        """Generate uses the provided key_id in the URL (supports rotation)."""
        expected_plaintext = b"plaintext-kek"
        expected_wrapped = "mock-jwe.wrapped"

        mock_response = MagicMock()
        mock_response.status_code = 201
        mock_response.json.return_value = {
            "plaintext": base64.b64encode(expected_plaintext).decode(),
            "key": expected_wrapped,
        }

        with patch("httpx.AsyncClient") as MockClient:
            mock_client_instance = AsyncMock()
            mock_client_instance.request = AsyncMock(return_value=mock_response)
            MockClient.return_value = mock_client_instance

            client = OVHKMSClient(config=mock_config)
            await client.generate_data_key(key_id="custom-key-123")

            call_args = mock_client_instance.request.call_args
            assert "custom-key-123" in call_args[0][1]

    async def test_decrypt_data_key_uses_stored_key_id(self, mock_config):
        """Decrypt uses the provided key_id (supports key rotation)."""
        wrapped_jwe = "mock-jwe.wrapped"
        expected_plaintext = b"plaintext"

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "plaintext": base64.b64encode(expected_plaintext).decode()
        }

        with patch("httpx.AsyncClient") as MockClient:
            mock_client_instance = AsyncMock()
            mock_client_instance.request = AsyncMock(return_value=mock_response)
            MockClient.return_value = mock_client_instance

            client = OVHKMSClient(config=mock_config)
            # Use a different key ID than the default - simulates rotation
            await client.decrypt_data_key(wrapped_jwe, key_id="old-rotated-key-456")

            call_args = mock_client_instance.request.call_args
            assert "old-rotated-key-456" in call_args[0][1]

    async def test_generate_data_key_retry_on_503(self, mock_config):
        """Retries on 5xx, eventually succeeds."""
        expected_plaintext = b"plaintext-kek"
        expected_wrapped = "mock-jwe.wrapped"

        # First two calls fail with 503, third succeeds
        mock_response_503 = MagicMock()
        mock_response_503.status_code = 503
        mock_response_503.text = "Service Unavailable"

        mock_response_201 = MagicMock()
        mock_response_201.status_code = 201
        mock_response_201.json.return_value = {
            "plaintext": base64.b64encode(expected_plaintext).decode(),
            "key": expected_wrapped,
        }

        with patch("httpx.AsyncClient") as MockClient:
            mock_client_instance = AsyncMock()
            mock_client_instance.request = AsyncMock(
                side_effect=[mock_response_503, mock_response_503, mock_response_201]
            )
            MockClient.return_value = mock_client_instance

            # Use shorter retry delays for faster tests
            mock_config.ovh_kms_retry_base_ms = 1
            mock_config.ovh_kms_retry_max_ms = 10

            client = OVHKMSClient(config=mock_config)
            plaintext, wrapped = await client.generate_data_key(key_id="test-key-id")

            assert plaintext == expected_plaintext
            assert wrapped == expected_wrapped
            assert mock_client_instance.request.call_count == 3

    async def test_generate_data_key_retry_on_429(self, mock_config):
        """Retries on 429 rate limit, eventually succeeds."""
        expected_plaintext = b"plaintext-kek"
        expected_wrapped = "mock-jwe.wrapped"

        mock_response_429 = MagicMock()
        mock_response_429.status_code = 429
        mock_response_429.text = "Too Many Requests"

        mock_response_201 = MagicMock()
        mock_response_201.status_code = 201
        mock_response_201.json.return_value = {
            "plaintext": base64.b64encode(expected_plaintext).decode(),
            "key": expected_wrapped,
        }

        with patch("httpx.AsyncClient") as MockClient:
            mock_client_instance = AsyncMock()
            mock_client_instance.request = AsyncMock(
                side_effect=[mock_response_429, mock_response_201]
            )
            MockClient.return_value = mock_client_instance

            mock_config.ovh_kms_retry_base_ms = 1
            mock_config.ovh_kms_retry_max_ms = 10

            client = OVHKMSClient(config=mock_config)
            plaintext, wrapped = await client.generate_data_key(key_id="test-key-id")

            assert plaintext == expected_plaintext
            assert wrapped == expected_wrapped
            assert mock_client_instance.request.call_count == 2

    async def test_generate_data_key_no_retry_on_401(self, mock_config):
        """No retry on auth errors - raises immediately."""
        mock_response = MagicMock()
        mock_response.status_code = 401

        with patch("httpx.AsyncClient") as MockClient:
            mock_client_instance = AsyncMock()
            mock_client_instance.request = AsyncMock(return_value=mock_response)
            MockClient.return_value = mock_client_instance

            client = OVHKMSClient(config=mock_config)

            with pytest.raises(OVHKMSAuthenticationError):
                await client.generate_data_key(key_id="test-key-id")

            # Should not retry on auth error
            assert mock_client_instance.request.call_count == 1

    async def test_generate_data_key_no_retry_on_403(self, mock_config):
        """No retry on 403 forbidden - raises immediately."""
        mock_response = MagicMock()
        mock_response.status_code = 403

        with patch("httpx.AsyncClient") as MockClient:
            mock_client_instance = AsyncMock()
            mock_client_instance.request = AsyncMock(return_value=mock_response)
            MockClient.return_value = mock_client_instance

            client = OVHKMSClient(config=mock_config)

            with pytest.raises(OVHKMSAuthenticationError):
                await client.generate_data_key(key_id="test-key-id")

            assert mock_client_instance.request.call_count == 1

    async def test_decrypt_data_key_unavailable_after_retries(self, mock_config):
        """OVHKMSUnavailableError after max retries exhausted."""
        wrapped_jwe = "mock-jwe.wrapped"

        mock_response = MagicMock()
        mock_response.status_code = 503
        mock_response.text = "Service Unavailable"

        with patch("httpx.AsyncClient") as MockClient:
            mock_client_instance = AsyncMock()
            mock_client_instance.request = AsyncMock(return_value=mock_response)
            MockClient.return_value = mock_client_instance

            # Use shorter retry delays
            mock_config.ovh_kms_retry_base_ms = 1
            mock_config.ovh_kms_retry_max_ms = 10

            client = OVHKMSClient(config=mock_config)

            with pytest.raises(OVHKMSUnavailableError):
                await client.decrypt_data_key(wrapped_jwe, key_id="test-key-id")

            # Should have tried max_retries times
            assert mock_client_instance.request.call_count == mock_config.ovh_kms_max_retries

    async def test_generate_data_key_timeout_retry(self, mock_config):
        """Retries on timeout, eventually succeeds."""
        expected_plaintext = b"plaintext-kek"
        expected_wrapped = "mock-jwe.wrapped"

        mock_response_201 = MagicMock()
        mock_response_201.status_code = 201
        mock_response_201.json.return_value = {
            "plaintext": base64.b64encode(expected_plaintext).decode(),
            "key": expected_wrapped,
        }

        with patch("httpx.AsyncClient") as MockClient:
            mock_client_instance = AsyncMock()
            mock_client_instance.request = AsyncMock(
                side_effect=[
                    httpx.TimeoutException("timeout"),
                    mock_response_201,
                ]
            )
            MockClient.return_value = mock_client_instance

            mock_config.ovh_kms_retry_base_ms = 1
            mock_config.ovh_kms_retry_max_ms = 10

            client = OVHKMSClient(config=mock_config)
            plaintext, wrapped = await client.generate_data_key(key_id="test-key-id")

            assert plaintext == expected_plaintext
            assert mock_client_instance.request.call_count == 2

    async def test_generate_data_key_connection_error_retry(self, mock_config):
        """Retries on connection error, eventually succeeds."""
        expected_plaintext = b"plaintext-kek"
        expected_wrapped = "mock-jwe.wrapped"

        mock_response_201 = MagicMock()
        mock_response_201.status_code = 201
        mock_response_201.json.return_value = {
            "plaintext": base64.b64encode(expected_plaintext).decode(),
            "key": expected_wrapped,
        }

        with patch("httpx.AsyncClient") as MockClient:
            mock_client_instance = AsyncMock()
            mock_client_instance.request = AsyncMock(
                side_effect=[
                    httpx.ConnectError("connection failed"),
                    mock_response_201,
                ]
            )
            MockClient.return_value = mock_client_instance

            mock_config.ovh_kms_retry_base_ms = 1
            mock_config.ovh_kms_retry_max_ms = 10

            client = OVHKMSClient(config=mock_config)
            plaintext, wrapped = await client.generate_data_key(key_id="test-key-id")

            assert plaintext == expected_plaintext
            assert mock_client_instance.request.call_count == 2

    async def test_generate_data_key_400_no_retry(self, mock_config):
        """4xx client errors (except 401/403/429) raise immediately without retry."""
        mock_response = MagicMock()
        mock_response.status_code = 400
        mock_response.text = "Bad Request"

        with patch("httpx.AsyncClient") as MockClient:
            mock_client_instance = AsyncMock()
            mock_client_instance.request = AsyncMock(return_value=mock_response)
            MockClient.return_value = mock_client_instance

            client = OVHKMSClient(config=mock_config)

            with pytest.raises(OVHKMSError):
                await client.generate_data_key(key_id="test-key-id")

            # Should not retry on 4xx
            assert mock_client_instance.request.call_count == 1

    async def test_invalid_response_raises_error(self, mock_config):
        """Invalid response format raises OVHKMSError."""
        mock_response = MagicMock()
        mock_response.status_code = 201
        mock_response.json.return_value = {"wrong_key": "value"}

        with patch("httpx.AsyncClient") as MockClient:
            mock_client_instance = AsyncMock()
            mock_client_instance.request = AsyncMock(return_value=mock_response)
            MockClient.return_value = mock_client_instance

            client = OVHKMSClient(config=mock_config)

            with pytest.raises(OVHKMSError, match="Invalid response"):
                await client.generate_data_key(key_id="test-key-id")

    async def test_context_manager(self, mock_config):
        """Client works as async context manager."""
        with patch("httpx.AsyncClient") as MockClient:
            mock_client_instance = AsyncMock()
            mock_client_instance.aclose = AsyncMock()
            MockClient.return_value = mock_client_instance

            async with OVHKMSClient(config=mock_config) as client:
                assert client is not None

            mock_client_instance.aclose.assert_called_once()

    async def test_close_calls_aclose(self, mock_config):
        """close() properly closes the httpx client."""
        with patch("httpx.AsyncClient") as MockClient:
            mock_client_instance = AsyncMock()
            mock_client_instance.aclose = AsyncMock()
            MockClient.return_value = mock_client_instance

            client = OVHKMSClient(config=mock_config)
            await client.close()

            mock_client_instance.aclose.assert_called_once()

    async def test_uses_mtls_cert(self, mock_config):
        """Client is initialized with mTLS certificate configuration."""
        with patch("httpx.AsyncClient") as MockClient:
            MockClient.return_value = AsyncMock()

            OVHKMSClient(config=mock_config)

            MockClient.assert_called_once()
            call_kwargs = MockClient.call_args[1]
            assert call_kwargs["cert"] == (
                mock_config.ovh_kms_cert_path,
                mock_config.ovh_kms_key_path,
            )
            assert call_kwargs["base_url"] == mock_config.ovh_kms_endpoint

    async def test_uses_custom_ca_when_provided(self, mock_config):
        """Client uses custom CA path when provided."""
        mock_config.ovh_kms_ca_path = "/path/to/ca.crt"

        with patch("httpx.AsyncClient") as MockClient:
            MockClient.return_value = AsyncMock()

            OVHKMSClient(config=mock_config)

            call_kwargs = MockClient.call_args[1]
            assert call_kwargs["verify"] == "/path/to/ca.crt"

    async def test_uses_default_verification_without_ca(self, mock_config):
        """Client uses default SSL verification when no CA path provided."""
        mock_config.ovh_kms_ca_path = ""

        with patch("httpx.AsyncClient") as MockClient:
            MockClient.return_value = AsyncMock()

            OVHKMSClient(config=mock_config)

            call_kwargs = MockClient.call_args[1]
            assert call_kwargs["verify"] is True
