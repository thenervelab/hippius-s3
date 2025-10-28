from __future__ import annotations

import base64
import hashlib
from typing import Optional

import asyncpg  # type: ignore[import-untyped]
import nacl.secret  # type: ignore[import-untyped]
import nacl.utils  # type: ignore[import-untyped]

from hippius_s3.config import get_config


async def get_or_create_encryption_key_bytes(
    *,
    main_account_id: str,
    bucket_name: str,
) -> bytes:
    """Resolve encryption key bytes for a given main_account_id + bucket using the keystore DB.

    This implementation uses the configured keystore database (Config.encryption_database_url),
    generating a new key if one doesn't exist. No SDK config or seed fallback is used.
    """
    combined_id = f"{main_account_id}:{bucket_name}"

    # Resolve DSN strictly from application config
    cfg = get_config()
    dsn: Optional[str] = getattr(cfg, "encryption_database_url", None)
    if not dsn:
        raise RuntimeError("encryption_key_unavailable")

    async def _ensure_tables(conn: asyncpg.Connection) -> None:  # type: ignore[name-defined]
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS encryption_keys (
                id SERIAL PRIMARY KEY,
                subaccount_id VARCHAR(255) NOT NULL,
                encryption_key_b64 TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE INDEX IF NOT EXISTS idx_encryption_keys_subaccount_created
              ON encryption_keys(subaccount_id, created_at DESC);
            """
        )

    conn = await asyncpg.connect(dsn)  # type: ignore[arg-type]
    try:
        sub_hash = hashlib.sha256(combined_id.encode("utf-8")).hexdigest()
        try:
            row = await conn.fetchrow(
                """
                SELECT encryption_key_b64
                  FROM encryption_keys
                 WHERE subaccount_id = $1
                 ORDER BY created_at DESC
                 LIMIT 1
                """,
                sub_hash,
            )
        except Exception as e:
            # Undefined table (42P01) → create schema and retry once
            if isinstance(e, Exception) and getattr(e, "sqlstate", "") == "42P01":
                await _ensure_tables(conn)
                row = await conn.fetchrow(
                    """
                    SELECT encryption_key_b64
                      FROM encryption_keys
                     WHERE subaccount_id = $1
                     ORDER BY created_at DESC
                     LIMIT 1
                    """,
                    sub_hash,
                )
            else:
                raise
        key_b64: Optional[str] = row and row["encryption_key_b64"]
        if not key_b64:
            # Generate new key
            key_bytes = nacl.utils.random(nacl.secret.SecretBox.KEY_SIZE)
            key_b64 = base64.b64encode(key_bytes).decode("utf-8")
            try:
                await conn.execute(
                    "INSERT INTO encryption_keys (subaccount_id, encryption_key_b64) VALUES ($1, $2)",
                    sub_hash,
                    key_b64,
                )
            except Exception as e:
                if isinstance(e, Exception) and getattr(e, "sqlstate", "") == "42P01":
                    await _ensure_tables(conn)
                    await conn.execute(
                        "INSERT INTO encryption_keys (subaccount_id, encryption_key_b64) VALUES ($1, $2)",
                        sub_hash,
                        key_b64,
                    )
                else:
                    raise
        return base64.b64decode(key_b64)
    finally:
        await conn.close()
