"""KEK (Key Encryption Key) service with wrapping support.

Key hierarchy:
- KMS Master Key (OVH HSM) → wraps → KEKs (stored encrypted in DB) → wrap → DEKs (per-object)
- Local wrap key (dev only) → wraps → KEKs → wrap → DEKs

Wrapping modes (controlled by HIPPIUS_KMS_MODE):
- "required": KMS is mandatory, fail at startup if unavailable (prod/staging/e2e)
- "disabled": KMS is off, use local software wrapping (dev only)

No silent fallback - misconfiguration crashes fast.

Schema:
- wrapped_kek_bytes: The encrypted KEK bytes (NOT NULL)
- kms_key_id: Identifier for the wrapping key (NOT NULL)
  - "local" for local software wrapping (disabled mode)
  - KMS key UUID for OVH KMS wrapping (required mode)

Key rotation support:
- Wrapping-key rotation (KMS keys): New KEKs use default key, old KEKs use stored key_id
- KEK rotation (bucket keys): Modeled via status='active'
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
import uuid
from typing import TYPE_CHECKING
from typing import Optional

import asyncpg  # type: ignore[import-untyped]

from hippius_s3.config import get_config


if TYPE_CHECKING:
    from hippius_s3.config import Config
    from hippius_s3.services.ovh_kms_client import OVHKMSClient

logger = logging.getLogger(__name__)

KEK_SIZE_BYTES = 32

_POOL: asyncpg.Pool | None = None  # type: ignore[name-defined]
_POOL_DSN: str | None = None
_POOL_LOCK = asyncio.Lock()

_KEK_CACHE: dict[tuple[str, str], tuple[bytes, float]] = {}
_KEK_CACHE_LOCK = asyncio.Lock()

# KMS client singleton. In "required" mode, set at startup via init_kms_client().
# In "disabled" mode, always None.
_KMS_CLIENT: "OVHKMSClient | None" = None  # type: ignore[name-defined]
_KMS_CLIENT_LOCK = asyncio.Lock()

# Table initialization tracking
_TABLES_ENSURED = False
_TABLES_LOCK = asyncio.Lock()


async def _get_pool(dsn: str) -> asyncpg.Pool:  # type: ignore[name-defined]
    global _POOL, _POOL_DSN
    if _POOL is not None and dsn == _POOL_DSN:
        return _POOL
    async with _POOL_LOCK:
        if _POOL is not None and dsn == _POOL_DSN:
            return _POOL
        cfg = get_config()
        _POOL = await asyncpg.create_pool(
            dsn,  # type: ignore[arg-type]
            min_size=int(cfg.kek_db_pool_min_size),
            max_size=int(cfg.kek_db_pool_max_size),
        )
        _POOL_DSN = dsn
        return _POOL


async def init_kms_client(config: "Config | None" = None) -> None:
    """Initialize KMS client at application startup.

    Call this once during app startup (e.g., in FastAPI lifespan).

    Args:
        config: Application config. If None, loads via get_config().

    In "required" mode:
    - Waits for cert files (handles docker-compose race conditions)
    - Creates the KMS client
    - Raises on failure (fail-fast)

    In "disabled" mode:
    - Logs and returns (local wrapping will be used)

    Raises:
        RuntimeError: If KMS initialization fails in required mode.
    """
    global _KMS_CLIENT

    if config is None:
        config = get_config()

    if config.kms_mode == "disabled":
        logger.info("KMS disabled, using local software wrapping")
        return

    # Required mode: initialize the KMS client
    from hippius_s3.services.ovh_kms_client import OVHKMSClient

    async with _KMS_CLIENT_LOCK:
        if _KMS_CLIENT is not None:
            return  # Already initialized

        logger.info("Initializing OVH KMS client...")

        # Wait for cert files (handles docker-compose race conditions)
        try:
            await OVHKMSClient.wait_for_certs(config)
        except ValueError as e:
            raise RuntimeError(f"KMS initialization failed: {e}") from e

        try:
            _KMS_CLIENT = OVHKMSClient(config)
            logger.info("OVH KMS client initialized successfully")
        except Exception as e:
            raise RuntimeError(f"KMS client creation failed: {e}") from e


def _get_kms_client() -> "OVHKMSClient | None":  # type: ignore[name-defined]
    """Get KMS client singleton.

    In "required" mode: Returns the client (must have been initialized via init_kms_client()).
    In "disabled" mode: Returns None (use local wrapping).

    Raises:
        RuntimeError: If called in required mode before init_kms_client().
    """
    cfg = get_config()

    if cfg.kms_mode == "disabled":
        return None

    # Required mode: client must be initialized
    if _KMS_CLIENT is None:
        raise RuntimeError("KMS client not initialized. Call init_kms_client() at startup.")
    return _KMS_CLIENT


async def _create_wrapped_kek(context: str) -> tuple[bytes, bytes, str]:
    """Generate a new KEK and wrap it using KMS or local wrapping.

    For KMS mode: KMS generates and wraps the key (centralizes key generation)
    For local mode: We generate the key and wrap it locally

    Args:
        context: Context for error messages

    Returns:
        Tuple of (kek_plaintext, wrapped_bytes, key_id)
        - kek_plaintext: The plaintext KEK for immediate use
        - wrapped_bytes: The wrapped key to store in DB
        - key_id: KMS key ID or "local"
    """
    kms_client = _get_kms_client()

    if kms_client is not None:
        # Use KMS - it generates AND wraps the key in one call
        from hippius_s3.services.ovh_kms_client import OVHKMSAuthenticationError
        from hippius_s3.services.ovh_kms_client import OVHKMSError
        from hippius_s3.services.ovh_kms_client import OVHKMSUnavailableError

        cfg = get_config()
        key_id = cfg.ovh_kms_default_key_id

        try:
            kek_bytes, wrapped_jwe = await kms_client.generate_data_key(key_id=key_id, name="kek")
            # Store JWE string as UTF-8 bytes in DB
            wrapped_bytes = wrapped_jwe.encode("utf-8")
            return kek_bytes, wrapped_bytes, key_id
        except OVHKMSAuthenticationError as e:
            logger.error(f"KMS authentication failed during key generation ({context}): {e}")
            raise RuntimeError("kms_auth_failed") from e
        except OVHKMSUnavailableError as e:
            logger.error(f"KMS unavailable during key generation ({context}): {e}")
            raise RuntimeError("kms_unavailable") from e
        except OVHKMSError as e:
            logger.error(f"KMS error during key generation ({context}): {e}")
            raise RuntimeError("kms_error") from e
    else:
        # Use local software wrapping (derives key from HIPPIUS_AUTH_ENCRYPTION_KEY)
        from hippius_s3.services.local_kek_wrapper import LOCAL_WRAP_KEY_ID
        from hippius_s3.services.local_kek_wrapper import wrap_key_local

        cfg = get_config()
        # Generate random KEK locally
        kek_bytes = os.urandom(KEK_SIZE_BYTES)
        # hippius_secret_decryption_material = HIPPIUS_AUTH_ENCRYPTION_KEY env var
        secret = cfg.hippius_secret_decryption_material
        wrapped_bytes = wrap_key_local(kek_bytes, secret)
        logger.debug(f"Created KEK using local software wrapping ({context})")
        return kek_bytes, wrapped_bytes, LOCAL_WRAP_KEY_ID


async def _unwrap_kek(
    wrapped: bytes,
    stored_key_id: str,
    kek_id: uuid.UUID,
) -> bytes:
    """Unwrap a KEK using KMS or local unwrapping based on stored key_id.

    Args:
        wrapped: Wrapped key bytes (JWE string for KMS, AES-GCM ciphertext for local)
        stored_key_id: The key ID stored with the wrapped key
                      ("local" for software wrap, KMS key ID otherwise)
        kek_id: The KEK ID (for logging)

    Returns:
        Plaintext KEK bytes
    """
    from hippius_s3.services.local_kek_wrapper import LOCAL_WRAP_KEY_ID

    if stored_key_id == LOCAL_WRAP_KEY_ID:
        # Unwrap using local software wrapping (key from HIPPIUS_AUTH_ENCRYPTION_KEY)
        from hippius_s3.services.local_kek_wrapper import unwrap_key_local

        cfg = get_config()
        # hippius_secret_decryption_material = HIPPIUS_AUTH_ENCRYPTION_KEY env var
        secret = cfg.hippius_secret_decryption_material
        try:
            return unwrap_key_local(wrapped, secret)
        except ValueError as e:
            logger.error(f"Local unwrap failed (kek_id={kek_id}): {e}")
            raise RuntimeError("local_unwrap_failed") from e
    else:
        # Unwrap using KMS
        kms_client = _get_kms_client()
        if kms_client is None:
            # This happens if a KMS-wrapped KEK exists but KMS mode is now disabled.
            # This is a configuration error - you can't disable KMS if you have
            # KEKs that were wrapped with KMS.
            raise RuntimeError(
                f"KEK {kek_id} was wrapped with KMS key {stored_key_id}, "
                "but HIPPIUS_KMS_MODE=disabled. Cannot decrypt KMS-wrapped keys "
                "without KMS. Set HIPPIUS_KMS_MODE=required."
            )

        from hippius_s3.services.ovh_kms_client import OVHKMSAuthenticationError
        from hippius_s3.services.ovh_kms_client import OVHKMSError
        from hippius_s3.services.ovh_kms_client import OVHKMSUnavailableError

        try:
            # Wrapped key is stored as UTF-8 bytes (JWE string)
            try:
                wrapped_jwe = wrapped.decode("utf-8")
            except UnicodeDecodeError as e:
                logger.error(
                    f"Invalid wrapped key format (kek_id={kek_id}, key_id={stored_key_id}): "
                    "not valid UTF-8 - possible DB corruption or format mismatch"
                )
                raise RuntimeError("kms_error") from e
            return await kms_client.decrypt_data_key(wrapped_jwe, key_id=stored_key_id)
        except OVHKMSAuthenticationError as e:
            logger.error(f"KMS authentication failed during decrypt (kek_id={kek_id}): {e}")
            raise RuntimeError("kms_auth_failed") from e
        except OVHKMSUnavailableError as e:
            logger.error(f"KMS unavailable during decrypt (kek_id={kek_id}): {e}")
            raise RuntimeError("kms_unavailable") from e
        except OVHKMSError as e:
            logger.error(f"KMS error during decrypt (kek_id={kek_id}): {e}")
            raise RuntimeError("kms_error") from e


async def _get_cached_kek(bucket_id: str, kek_id: uuid.UUID) -> bytes | None:
    cfg = get_config()
    ttl = int(cfg.kek_cache_ttl_seconds)
    if ttl <= 0:
        return None
    key = (str(bucket_id), str(kek_id))
    now = time.monotonic()
    async with _KEK_CACHE_LOCK:
        entry = _KEK_CACHE.get(key)
        if not entry:
            return None
        kek_bytes, expires_at = entry
        if expires_at <= now:
            _KEK_CACHE.pop(key, None)
            return None
        return kek_bytes


async def _set_cached_kek(bucket_id: str, kek_id: uuid.UUID, kek_bytes: bytes) -> None:
    cfg = get_config()
    ttl = int(cfg.kek_cache_ttl_seconds)
    if ttl <= 0:
        return
    key = (str(bucket_id), str(kek_id))
    expires_at = time.monotonic() + ttl
    async with _KEK_CACHE_LOCK:
        _KEK_CACHE[key] = (bytes(kek_bytes), expires_at)


async def _ensure_tables(conn: asyncpg.Connection) -> None:  # type: ignore[name-defined]
    """Create bucket_keks table.

    Schema:
    - wrapped_kek_bytes: Encrypted KEK bytes (NOT NULL)
    - kms_key_id: "local" or KMS key UUID (NOT NULL)
    """
    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS bucket_keks (
            bucket_id UUID NOT NULL,
            kek_id UUID PRIMARY KEY,
            wrapped_kek_bytes BYTEA NOT NULL,
            kms_key_id TEXT NOT NULL CHECK (kms_key_id <> ''),
            status TEXT NOT NULL DEFAULT 'active',
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        CREATE INDEX IF NOT EXISTS idx_bucket_keks_bucket_status_created
          ON bucket_keks(bucket_id, status, created_at DESC);
        CREATE UNIQUE INDEX IF NOT EXISTS uniq_bucket_active_kek
          ON bucket_keks(bucket_id)
         WHERE status = 'active';
        """
    )


async def _maybe_ensure_tables(conn: asyncpg.Connection) -> None:  # type: ignore[name-defined]
    """Ensure tables exist (called once per process lifetime)."""
    global _TABLES_ENSURED
    if _TABLES_ENSURED:
        return
    async with _TABLES_LOCK:
        if _TABLES_ENSURED:
            return
        await _ensure_tables(conn)
        _TABLES_ENSURED = True


async def get_or_create_active_bucket_kek(
    *,
    bucket_id: str,
) -> tuple[uuid.UUID, bytes]:
    """Return (kek_id, kek_bytes) for the active KEK of a bucket, creating one if missing.

    The returned kek_bytes is always the plaintext KEK (suitable for DEK operations).

    Hot path: If KEK is cached, returns immediately without unwrap.
    Cold path: Fetches from DB, unwraps, caches plaintext.
    """
    cfg = get_config()
    dsn: Optional[str] = getattr(cfg, "encryption_database_url", None)
    if not dsn:
        raise RuntimeError("kek_database_unavailable")

    pool = await _get_pool(dsn)
    async with pool.acquire() as conn:
        await _maybe_ensure_tables(conn)

        async def _fetch_active() -> asyncpg.Record | None:  # type: ignore[name-defined]
            return await conn.fetchrow(
                """
                SELECT kek_id, wrapped_kek_bytes, kms_key_id
                  FROM bucket_keks
                 WHERE bucket_id = $1
                   AND status = 'active'
                 ORDER BY created_at DESC
                 LIMIT 1
                """,
                uuid.UUID(str(bucket_id)),
            )

        row = await _fetch_active()

        if row is not None:
            kek_id = uuid.UUID(str(row["kek_id"]))

            # Check for cached KEK first
            cached = await _get_cached_kek(bucket_id, kek_id)
            if cached is not None:
                return kek_id, cached

            # Unwrap the KEK
            wrapped_kek_bytes = row["wrapped_kek_bytes"]
            kms_key_id = row["kms_key_id"]
            kek_bytes = await _unwrap_kek(bytes(wrapped_kek_bytes), kms_key_id, kek_id)

            await _set_cached_kek(bucket_id, kek_id, kek_bytes)
            return kek_id, kek_bytes

        # Create new KEK (KMS generates the key, or we generate locally)
        kek_id = uuid.uuid4()
        kek_bytes, wrapped_kek_bytes, kms_key_id = await _create_wrapped_kek(f"new KEK for bucket {bucket_id}")

        try:
            await conn.execute(
                """
                INSERT INTO bucket_keks (bucket_id, kek_id, wrapped_kek_bytes, kms_key_id, status)
                VALUES ($1, $2, $3, $4, 'active')
                """,
                uuid.UUID(str(bucket_id)),
                kek_id,
                wrapped_kek_bytes,
                kms_key_id,
            )
        except Exception as e:
            # Handle race condition: another process created the KEK
            if getattr(e, "sqlstate", "") == "23505":
                row = await _fetch_active()
                if row is not None:
                    kek_id = uuid.UUID(str(row["kek_id"]))

                    # Check cache first
                    cached = await _get_cached_kek(bucket_id, kek_id)
                    if cached is not None:
                        return kek_id, cached

                    # Unwrap the KEK
                    wrapped = row["wrapped_kek_bytes"]
                    key_id = row["kms_key_id"]
                    kek_bytes = await _unwrap_kek(bytes(wrapped), key_id, kek_id)

                    await _set_cached_kek(bucket_id, kek_id, kek_bytes)
                    return kek_id, kek_bytes
            raise

        await _set_cached_kek(bucket_id, kek_id, kek_bytes)
        return kek_id, kek_bytes


async def get_bucket_kek_bytes(*, bucket_id: str, kek_id: uuid.UUID) -> bytes:
    """Fetch a specific KEK by id for a bucket (used for decrypting old versions after rotation).

    Returns plaintext KEK bytes (unwrapped from DB storage).
    """
    cached = await _get_cached_kek(bucket_id, kek_id)
    if cached is not None:
        return cached

    cfg = get_config()
    dsn: Optional[str] = getattr(cfg, "encryption_database_url", None)
    if not dsn:
        raise RuntimeError("kek_database_unavailable")

    pool = await _get_pool(dsn)
    async with pool.acquire() as conn:
        await _maybe_ensure_tables(conn)

        row = await conn.fetchrow(
            """
            SELECT wrapped_kek_bytes, kms_key_id
              FROM bucket_keks
             WHERE bucket_id = $1
               AND kek_id = $2
             LIMIT 1
            """,
            uuid.UUID(str(bucket_id)),
            uuid.UUID(str(kek_id)),
        )
        if row is None:
            raise RuntimeError("kek_not_found")

        # Unwrap the KEK
        wrapped_kek_bytes = row["wrapped_kek_bytes"]
        kms_key_id = row["kms_key_id"]
        kek_bytes = await _unwrap_kek(bytes(wrapped_kek_bytes), kms_key_id, kek_id)

        await _set_cached_kek(bucket_id, kek_id, kek_bytes)
        return kek_bytes


async def close_kek_pool() -> None:
    """Close the KEK database pool and KMS client, clear cache."""
    global _POOL, _POOL_DSN, _KMS_CLIENT, _TABLES_ENSURED

    async with _POOL_LOCK:
        if _POOL is not None:
            await _POOL.close()
            _POOL = None
            _POOL_DSN = None

    async with _KMS_CLIENT_LOCK:
        if _KMS_CLIENT is not None:
            await _KMS_CLIENT.close()
            _KMS_CLIENT = None
            logger.info("OVH KMS client closed")

    async with _KEK_CACHE_LOCK:
        _KEK_CACHE.clear()

    async with _TABLES_LOCK:
        _TABLES_ENSURED = False
