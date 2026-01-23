"""Integration tests for KEK service with KMS wrapping."""

import base64
import os
import uuid
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest


# Mock KMS master key for XOR-based "encryption" in tests
MOCK_KMS_KEY = b"test-master-key-0123456789abcdef"


def xor_with_mock_key(data: bytes) -> bytes:
    """XOR data with the mock KMS key (repeating if needed).

    This is a test helper that mimics the mock KMS server behavior.
    NOT secure - for testing only.
    """
    key_repeated = (MOCK_KMS_KEY * ((len(data) // len(MOCK_KMS_KEY)) + 1))[: len(data)]
    return bytes(a ^ b for a, b in zip(data, key_repeated, strict=True))


def make_mock_jwe(plaintext: bytes) -> str:
    """Create a mock JWE-format wrapped key from plaintext."""
    wrapped = xor_with_mock_key(plaintext)
    return f"mock-jwe.{base64.b64encode(wrapped).decode()}"


def unwrap_mock_jwe(jwe: str) -> bytes:
    """Unwrap a mock JWE-format key to get plaintext."""
    wrapped_b64 = jwe[len("mock-jwe."):]
    wrapped = base64.b64decode(wrapped_b64)
    return xor_with_mock_key(wrapped)


@pytest.fixture
def mock_kms_client():
    """Create a mock KMS client that generates/decrypts data keys using XOR."""
    client = AsyncMock()

    async def mock_generate(*, key_id: str, name: str = "kek") -> tuple[bytes, str]:
        """Generate a random key and return (plaintext, wrapped_jwe)."""
        plaintext = os.urandom(32)
        wrapped_jwe = make_mock_jwe(plaintext)
        return plaintext, wrapped_jwe

    async def mock_decrypt(wrapped_jwe: str, *, key_id: str) -> bytes:
        """Decrypt a wrapped JWE key to get plaintext."""
        return unwrap_mock_jwe(wrapped_jwe)

    client.generate_data_key = AsyncMock(side_effect=mock_generate)
    client.decrypt_data_key = AsyncMock(side_effect=mock_decrypt)
    client.close = AsyncMock()

    return client


@pytest.fixture
def required_mode_config():
    """Create a mock config with KMS_MODE=required."""
    config = MagicMock()
    config.kms_mode = "required"
    config.ovh_kms_endpoint = "https://mock-kms:8443"
    config.ovh_kms_okms_id = "mock-okms-id"
    config.ovh_kms_default_key_id = "test-key-id"
    config.ovh_kms_cert_path = "/path/to/cert"
    config.ovh_kms_key_path = "/path/to/key"
    config.ovh_kms_ca_path = ""
    config.ovh_kms_timeout_seconds = 30.0
    config.ovh_kms_max_retries = 3
    config.ovh_kms_retry_base_ms = 100
    config.ovh_kms_retry_max_ms = 1000
    config.kek_cache_ttl_seconds = 300
    config.kek_db_pool_min_size = 1
    config.kek_db_pool_max_size = 5
    config.encryption_database_url = "postgresql://test:test@localhost/test"
    return config


@pytest.fixture
def disabled_mode_config():
    """Create a mock config with KMS_MODE=disabled (local wrapping)."""
    config = MagicMock()
    config.kms_mode = "disabled"
    # KMS vars not needed in disabled mode
    config.ovh_kms_endpoint = ""
    config.ovh_kms_default_key_id = ""
    config.ovh_kms_cert_path = ""
    config.ovh_kms_key_path = ""
    config.ovh_kms_ca_path = ""
    # Other required config
    config.kek_cache_ttl_seconds = 300
    config.kek_db_pool_min_size = 1
    config.kek_db_pool_max_size = 5
    config.encryption_database_url = "postgresql://test:test@localhost/test"
    config.hippius_secret_decryption_material = "test-local-wrapping-secret-key"
    return config


@pytest.mark.asyncio
class TestKEKServiceRequiredMode:
    """Tests for KEK service with KMS in required mode."""

    async def test_create_kek_wraps_via_kms(self, mock_kms_client, required_mode_config):
        """New KEK is wrapped via KMS before storage."""
        import hippius_s3.services.kek_service as kek_service

        # Reset module-level state and set KMS client
        kek_service._POOL = None
        kek_service._POOL_DSN = None
        kek_service._KMS_CLIENT = mock_kms_client
        kek_service._KEK_CACHE.clear()

        bucket_id = str(uuid.uuid4())

        # Mock database
        mock_conn = AsyncMock()
        mock_conn.fetchrow = AsyncMock(return_value=None)  # No existing KEK
        mock_conn.execute = AsyncMock()

        mock_pool = AsyncMock()
        mock_pool.acquire = MagicMock(return_value=AsyncMock(__aenter__=AsyncMock(return_value=mock_conn)))

        with (
            patch.object(kek_service, "_get_pool", return_value=mock_pool),
            patch("hippius_s3.services.kek_service.get_config", return_value=required_mode_config),
        ):
            kek_id, kek_bytes = await kek_service.get_or_create_active_bucket_kek(bucket_id=bucket_id)

            # Verify KMS generate_data_key was called
            mock_kms_client.generate_data_key.assert_called_once()

            # Verify plaintext KEK returned (32 bytes)
            assert len(kek_bytes) == 32

            # Verify INSERT was called with wrapped_kek_bytes column
            insert_call = mock_conn.execute.call_args
            assert insert_call is not None
            sql = insert_call[0][0]
            assert "wrapped_kek_bytes" in sql
            assert "kms_key_id" in sql

    async def test_fetch_kek_unwraps_on_cache_miss(self, mock_kms_client, required_mode_config):
        """Cache miss triggers KMS unwrap."""
        import hippius_s3.services.kek_service as kek_service

        kek_service._POOL = None
        kek_service._POOL_DSN = None
        kek_service._KMS_CLIENT = mock_kms_client
        kek_service._KEK_CACHE.clear()

        bucket_id = str(uuid.uuid4())
        kek_id = uuid.uuid4()
        plaintext_kek = os.urandom(32)

        # Simulate wrapped KEK using JWE format (UTF-8 bytes in DB)
        wrapped_jwe = make_mock_jwe(plaintext_kek)
        wrapped_kek_bytes = wrapped_jwe.encode("utf-8")

        # Mock database returning wrapped KEK
        mock_record = {"wrapped_kek_bytes": wrapped_kek_bytes, "kms_key_id": "test-key-id"}
        mock_conn = AsyncMock()
        mock_conn.fetchrow = AsyncMock(return_value=mock_record)

        mock_pool = AsyncMock()
        mock_pool.acquire = MagicMock(return_value=AsyncMock(__aenter__=AsyncMock(return_value=mock_conn)))

        with (
            patch.object(kek_service, "_get_pool", return_value=mock_pool),
            patch("hippius_s3.services.kek_service.get_config", return_value=required_mode_config),
        ):
            result = await kek_service.get_bucket_kek_bytes(bucket_id=bucket_id, kek_id=kek_id)

            # Verify KMS decrypt was called with the stored key_id
            mock_kms_client.decrypt_data_key.assert_called_once()
            call_kwargs = mock_kms_client.decrypt_data_key.call_args
            assert call_kwargs[1]["key_id"] == "test-key-id"

            # Verify plaintext KEK returned
            assert result == plaintext_kek

    async def test_cache_hit_skips_kms(self, mock_kms_client, required_mode_config):
        """Cached KEK doesn't call KMS."""
        import hippius_s3.services.kek_service as kek_service

        kek_service._POOL = None
        kek_service._POOL_DSN = None
        kek_service._KMS_CLIENT = mock_kms_client
        kek_service._KEK_CACHE.clear()

        bucket_id = str(uuid.uuid4())
        kek_id = uuid.uuid4()
        cached_kek = os.urandom(32)

        # Pre-populate cache
        with patch("hippius_s3.services.kek_service.get_config", return_value=required_mode_config):
            await kek_service._set_cached_kek(bucket_id, kek_id, cached_kek)

        mock_pool = AsyncMock()

        with (
            patch.object(kek_service, "_get_pool", return_value=mock_pool),
            patch("hippius_s3.services.kek_service.get_config", return_value=required_mode_config),
        ):
            result = await kek_service.get_bucket_kek_bytes(bucket_id=bucket_id, kek_id=kek_id)

            # Verify KMS was NOT called (cache hit)
            mock_kms_client.decrypt_data_key.assert_not_called()

            # Verify cached KEK returned
            assert result == cached_kek

    async def test_cache_sliding_window_refreshes_ttl(self, mock_kms_client, required_mode_config):
        """Cache TTL is refreshed on access (sliding window)."""
        import hippius_s3.services.kek_service as kek_service

        kek_service._POOL = None
        kek_service._POOL_DSN = None
        kek_service._KMS_CLIENT = mock_kms_client
        kek_service._KEK_CACHE.clear()

        bucket_id = str(uuid.uuid4())
        kek_id = uuid.uuid4()
        cached_kek = os.urandom(32)

        ttl = required_mode_config.kek_cache_ttl_seconds  # 300s
        t0 = 1000.0

        with patch("hippius_s3.services.kek_service.get_config", return_value=required_mode_config):
            # T=0: Cache the KEK
            with patch("hippius_s3.services.kek_service.time") as mock_time:
                mock_time.monotonic.return_value = t0
                await kek_service._set_cached_kek(bucket_id, kek_id, cached_kek)

            # T=280s: Access within TTL → hit, TTL refreshed
            with patch("hippius_s3.services.kek_service.time") as mock_time:
                mock_time.monotonic.return_value = t0 + 280
                result = await kek_service._get_cached_kek(bucket_id, kek_id)
                assert result == cached_kek

            # T=560s: 560s from original cache, but only 280s from last access
            # Without sliding window this would be a miss (560 > 300)
            # With sliding window this is a hit (280 < 300)
            with patch("hippius_s3.services.kek_service.time") as mock_time:
                mock_time.monotonic.return_value = t0 + 560
                result = await kek_service._get_cached_kek(bucket_id, kek_id)
                assert result == cached_kek

            # T=870s: 310s since last access → expired
            with patch("hippius_s3.services.kek_service.time") as mock_time:
                mock_time.monotonic.return_value = t0 + 870
                result = await kek_service._get_cached_kek(bucket_id, kek_id)
                assert result is None

    async def test_kms_unavailable_fails_closed(self, required_mode_config):
        """KMS outage blocks operations (fail-closed)."""
        import hippius_s3.services.kek_service as kek_service
        from hippius_s3.services.ovh_kms_client import OVHKMSUnavailableError

        # Mock KMS that raises unavailable error
        mock_kms_client = AsyncMock()
        mock_kms_client.decrypt_data_key = AsyncMock(side_effect=OVHKMSUnavailableError("KMS timeout"))

        kek_service._POOL = None
        kek_service._POOL_DSN = None
        kek_service._KMS_CLIENT = mock_kms_client
        kek_service._KEK_CACHE.clear()

        bucket_id = str(uuid.uuid4())
        kek_id = uuid.uuid4()

        # Mock database returning wrapped KEK (JWE format as UTF-8 bytes)
        wrapped_jwe = make_mock_jwe(os.urandom(32))
        wrapped_kek_bytes = wrapped_jwe.encode("utf-8")
        mock_record = {"wrapped_kek_bytes": wrapped_kek_bytes, "kms_key_id": "test-key-id"}
        mock_conn = AsyncMock()
        mock_conn.fetchrow = AsyncMock(return_value=mock_record)

        mock_pool = AsyncMock()
        mock_pool.acquire = MagicMock(return_value=AsyncMock(__aenter__=AsyncMock(return_value=mock_conn)))

        with (
            patch.object(kek_service, "_get_pool", return_value=mock_pool),
            patch("hippius_s3.services.kek_service.get_config", return_value=required_mode_config),
        ):
            with pytest.raises(RuntimeError, match="kms_unavailable"):
                await kek_service.get_bucket_kek_bytes(bucket_id=bucket_id, kek_id=kek_id)

    async def test_close_kek_pool_closes_kms_client(self, mock_kms_client, required_mode_config):
        """close_kek_pool() also closes the KMS client."""
        import hippius_s3.services.kek_service as kek_service

        kek_service._POOL = None
        kek_service._POOL_DSN = None
        kek_service._KMS_CLIENT = mock_kms_client
        kek_service._KEK_CACHE.clear()

        await kek_service.close_kek_pool()

        mock_kms_client.close.assert_called_once()
        assert kek_service._KMS_CLIENT is None

    async def test_kms_key_rotation_old_key_decrypts(self, required_mode_config):
        """Old KEKs can be decrypted using their stored key_id (supports rotation)."""
        import hippius_s3.services.kek_service as kek_service

        bucket_id = str(uuid.uuid4())
        kek_id = uuid.uuid4()
        plaintext_kek = os.urandom(32)

        # Simulate wrapped KEK using JWE format (UTF-8 bytes in DB)
        wrapped_jwe = make_mock_jwe(plaintext_kek)
        wrapped_kek_bytes = wrapped_jwe.encode("utf-8")

        # The KEK was wrapped with old-key-123, not the current default
        old_key_id = "old-key-123"
        mock_record = {"wrapped_kek_bytes": wrapped_kek_bytes, "kms_key_id": old_key_id}
        mock_conn = AsyncMock()
        mock_conn.fetchrow = AsyncMock(return_value=mock_record)

        mock_pool = AsyncMock()
        mock_pool.acquire = MagicMock(return_value=AsyncMock(__aenter__=AsyncMock(return_value=mock_conn)))

        # Create a mock KMS client that tracks which key_id was used
        captured_key_ids = []

        async def mock_decrypt(wrapped_jwe: str, *, key_id: str) -> bytes:
            captured_key_ids.append(key_id)
            return unwrap_mock_jwe(wrapped_jwe)

        mock_kms_client = AsyncMock()
        mock_kms_client.decrypt_data_key = AsyncMock(side_effect=mock_decrypt)

        kek_service._POOL = None
        kek_service._POOL_DSN = None
        kek_service._KMS_CLIENT = mock_kms_client
        kek_service._KEK_CACHE.clear()

        with (
            patch.object(kek_service, "_get_pool", return_value=mock_pool),
            patch("hippius_s3.services.kek_service.get_config", return_value=required_mode_config),
        ):
            result = await kek_service.get_bucket_kek_bytes(bucket_id=bucket_id, kek_id=kek_id)

            # Verify the OLD key_id was used, not the current default
            assert captured_key_ids == [old_key_id]
            assert result == plaintext_kek

    async def test_get_kms_client_fails_if_not_initialized(self, required_mode_config):
        """_get_kms_client raises if called before init_kms_client()."""
        import hippius_s3.services.kek_service as kek_service

        kek_service._KMS_CLIENT = None

        with patch("hippius_s3.services.kek_service.get_config", return_value=required_mode_config):
            with pytest.raises(RuntimeError, match="KMS client not initialized"):
                kek_service._get_kms_client()


@pytest.mark.asyncio
class TestKMSClientStartup:
    """Tests for KMS client startup initialization."""

    async def test_init_kms_client_creates_client_in_required_mode(self, required_mode_config):
        """init_kms_client() creates and stores the KMS client."""
        import hippius_s3.services.kek_service as kek_service

        kek_service._KMS_CLIENT = None

        mock_client = AsyncMock()

        # Create a mock class with async wait_for_certs
        mock_kms_class = MagicMock()
        mock_kms_class.return_value = mock_client
        mock_kms_class.wait_for_certs = AsyncMock()

        with (
            patch("hippius_s3.services.kek_service.get_config", return_value=required_mode_config),
            patch("hippius_s3.services.ovh_kms_client.OVHKMSClient", mock_kms_class),
        ):
            await kek_service.init_kms_client()

            # Verify client was created and stored
            assert kek_service._KMS_CLIENT is mock_client
            mock_kms_class.wait_for_certs.assert_called_once()

    async def test_init_kms_client_noop_in_disabled_mode(self, disabled_mode_config):
        """init_kms_client() is a no-op in disabled mode."""
        import hippius_s3.services.kek_service as kek_service

        kek_service._KMS_CLIENT = None

        with patch("hippius_s3.services.kek_service.get_config", return_value=disabled_mode_config):
            await kek_service.init_kms_client()

            # Verify no client was created
            assert kek_service._KMS_CLIENT is None

    async def test_init_kms_client_fails_fast_on_cert_error(self, required_mode_config):
        """init_kms_client() raises RuntimeError if certs are missing."""
        import hippius_s3.services.kek_service as kek_service

        kek_service._KMS_CLIENT = None

        # Mock wait_for_certs to raise ValueError (certs not found)
        mock_kms_class = MagicMock()
        mock_kms_class.wait_for_certs = AsyncMock(
            side_effect=ValueError("KMS cert files not found: cert: /path/to/cert")
        )

        with (
            patch("hippius_s3.services.kek_service.get_config", return_value=required_mode_config),
            patch("hippius_s3.services.ovh_kms_client.OVHKMSClient", mock_kms_class),
        ):
            with pytest.raises(RuntimeError, match="KMS initialization failed"):
                await kek_service.init_kms_client()

    async def test_init_kms_client_idempotent(self, required_mode_config):
        """init_kms_client() is idempotent - second call is a no-op."""
        import hippius_s3.services.kek_service as kek_service

        mock_client = AsyncMock()
        kek_service._KMS_CLIENT = mock_client

        mock_kms_class = MagicMock()

        with (
            patch("hippius_s3.services.kek_service.get_config", return_value=required_mode_config),
            patch("hippius_s3.services.ovh_kms_client.OVHKMSClient", mock_kms_class),
        ):
            await kek_service.init_kms_client()

            # Verify no new client was created (constructor not called)
            mock_kms_class.assert_not_called()
            assert kek_service._KMS_CLIENT is mock_client


@pytest.mark.asyncio
class TestKEKServiceDisabledMode:
    """Tests for KEK service in disabled mode with local wrapping."""

    async def test_create_kek_wraps_locally(self, disabled_mode_config):
        """New KEK is wrapped locally in disabled mode."""
        import hippius_s3.services.kek_service as kek_service

        # Reset module-level state
        kek_service._POOL = None
        kek_service._POOL_DSN = None
        kek_service._KMS_CLIENT = None
        kek_service._KEK_CACHE.clear()

        bucket_id = str(uuid.uuid4())

        # Mock database
        mock_conn = AsyncMock()
        mock_conn.fetchrow = AsyncMock(return_value=None)  # No existing KEK
        mock_conn.execute = AsyncMock()

        mock_pool = AsyncMock()
        mock_pool.acquire = MagicMock(return_value=AsyncMock(__aenter__=AsyncMock(return_value=mock_conn)))

        with (
            patch.object(kek_service, "_get_pool", return_value=mock_pool),
            patch("hippius_s3.services.kek_service.get_config", return_value=disabled_mode_config),
        ):
            kek_id, kek_bytes = await kek_service.get_or_create_active_bucket_kek(bucket_id=bucket_id)

            # Verify plaintext KEK returned (32 bytes)
            assert len(kek_bytes) == 32

            # Verify INSERT was called with wrapped_kek_bytes and kms_key_id="local"
            insert_call = mock_conn.execute.call_args
            assert insert_call is not None
            sql = insert_call[0][0]
            assert "wrapped_kek_bytes" in sql
            assert "kms_key_id" in sql

            # Check that kms_key_id is "local"
            args = insert_call[0]
            assert "local" in args

    async def test_fetch_locally_wrapped_kek_on_cache_miss(self, disabled_mode_config):
        """Cache miss for locally wrapped KEK unwraps correctly."""
        import hippius_s3.services.kek_service as kek_service
        from hippius_s3.services.local_kek_wrapper import wrap_key_local

        kek_service._POOL = None
        kek_service._POOL_DSN = None
        kek_service._KMS_CLIENT = None
        kek_service._KEK_CACHE.clear()

        bucket_id = str(uuid.uuid4())
        kek_id = uuid.uuid4()
        plaintext_kek = os.urandom(32)

        # Wrap using local wrapper
        secret = disabled_mode_config.hippius_secret_decryption_material
        wrapped_kek = wrap_key_local(plaintext_kek, secret)

        # Mock database returning locally wrapped KEK
        mock_record = {"wrapped_kek_bytes": wrapped_kek, "kms_key_id": "local"}
        mock_conn = AsyncMock()
        mock_conn.fetchrow = AsyncMock(return_value=mock_record)

        mock_pool = AsyncMock()
        mock_pool.acquire = MagicMock(return_value=AsyncMock(__aenter__=AsyncMock(return_value=mock_conn)))

        with (
            patch.object(kek_service, "_get_pool", return_value=mock_pool),
            patch("hippius_s3.services.kek_service.get_config", return_value=disabled_mode_config),
        ):
            result = await kek_service.get_bucket_kek_bytes(bucket_id=bucket_id, kek_id=kek_id)

            # Verify plaintext KEK returned
            assert result == plaintext_kek

    async def test_get_kms_client_returns_none_in_disabled_mode(self, disabled_mode_config):
        """_get_kms_client returns None in disabled mode."""
        import hippius_s3.services.kek_service as kek_service

        kek_service._KMS_CLIENT = None

        with patch("hippius_s3.services.kek_service.get_config", return_value=disabled_mode_config):
            client = kek_service._get_kms_client()
            assert client is None

    async def test_local_unwrap_failed_raises_runtime_error(self, disabled_mode_config):
        """Failed local unwrap raises RuntimeError."""
        import hippius_s3.services.kek_service as kek_service

        kek_service._POOL = None
        kek_service._POOL_DSN = None
        kek_service._KMS_CLIENT = None
        kek_service._KEK_CACHE.clear()

        bucket_id = str(uuid.uuid4())
        kek_id = uuid.uuid4()

        # Invalid wrapped bytes (will fail authentication)
        invalid_wrapped = os.urandom(60)

        mock_record = {"wrapped_kek_bytes": invalid_wrapped, "kms_key_id": "local"}
        mock_conn = AsyncMock()
        mock_conn.fetchrow = AsyncMock(return_value=mock_record)

        mock_pool = AsyncMock()
        mock_pool.acquire = MagicMock(return_value=AsyncMock(__aenter__=AsyncMock(return_value=mock_conn)))

        with (
            patch.object(kek_service, "_get_pool", return_value=mock_pool),
            patch("hippius_s3.services.kek_service.get_config", return_value=disabled_mode_config),
        ):
            with pytest.raises(RuntimeError, match="local_unwrap_failed"):
                await kek_service.get_bucket_kek_bytes(bucket_id=bucket_id, kek_id=kek_id)

    async def test_kms_wrapped_kek_fails_in_disabled_mode(self, disabled_mode_config):
        """KEK wrapped by KMS cannot be unwrapped in disabled mode."""
        import hippius_s3.services.kek_service as kek_service

        kek_service._POOL = None
        kek_service._POOL_DSN = None
        kek_service._KMS_CLIENT = None
        kek_service._KEK_CACHE.clear()

        bucket_id = str(uuid.uuid4())
        kek_id = uuid.uuid4()

        # KEK was wrapped by KMS (not locally)
        kms_wrapped = os.urandom(32)
        mock_record = {"wrapped_kek_bytes": kms_wrapped, "kms_key_id": "kms-key-123"}
        mock_conn = AsyncMock()
        mock_conn.fetchrow = AsyncMock(return_value=mock_record)

        mock_pool = AsyncMock()
        mock_pool.acquire = MagicMock(return_value=AsyncMock(__aenter__=AsyncMock(return_value=mock_conn)))

        with (
            patch.object(kek_service, "_get_pool", return_value=mock_pool),
            patch("hippius_s3.services.kek_service.get_config", return_value=disabled_mode_config),
        ):
            with pytest.raises(RuntimeError, match="HIPPIUS_KMS_MODE=disabled"):
                await kek_service.get_bucket_kek_bytes(bucket_id=bucket_id, kek_id=kek_id)
