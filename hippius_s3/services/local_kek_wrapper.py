"""Local software KEK wrapping for development environments.

This module provides a local (non-HSM) key wrapping mechanism using AES-256-GCM.
It derives a wrapping key from HIPPIUS_AUTH_ENCRYPTION_KEY and uses it to wrap/unwrap KEKs.

SECURITY NOTE: This is NOT as secure as HSM-backed KMS wrapping. Use only for:
- Local development
- Testing without KMS infrastructure
- Environments where KMS is not available

For production, always use HIPPIUS_KMS_MODE=required with real KMS.
"""

from __future__ import annotations

import hashlib
import os

from cryptography.hazmat.primitives.ciphers.aead import AESGCM


# Marker used as kms_key_id to indicate local wrapping
LOCAL_WRAP_KEY_ID = "local"

# AES-GCM parameters
_NONCE_SIZE = 12  # 96 bits recommended for AES-GCM
_KEY_SIZE = 32  # 256 bits

# Domain separation prefix for key derivation (prevents accidental key reuse)
_KEY_DERIVATION_PREFIX = b"hippius-local-kek-wrap-v1:"


def _derive_wrapping_key(secret: str) -> bytes:
    """Derive a 256-bit wrapping key from the secret material.

    Uses SHA-256 with domain separation to derive a fixed-size key.
    Domain separation ensures this key won't collide with other uses of the same secret.
    """
    return hashlib.sha256(_KEY_DERIVATION_PREFIX + secret.encode()).digest()


def wrap_key_local(plaintext_kek: bytes, secret: str) -> bytes:
    """Wrap a KEK using local AES-256-GCM encryption.

    Args:
        plaintext_kek: The plaintext KEK bytes to wrap (typically 32 bytes)
        secret: The secret material to derive the wrapping key from
                (typically HIPPIUS_AUTH_ENCRYPTION_KEY)

    Returns:
        Wrapped key bytes: nonce (12 bytes) || ciphertext+tag
    """
    wrapping_key = _derive_wrapping_key(secret)
    nonce = os.urandom(_NONCE_SIZE)

    aesgcm = AESGCM(wrapping_key)
    ciphertext = aesgcm.encrypt(nonce, plaintext_kek, associated_data=None)

    # Return nonce || ciphertext (ciphertext includes 16-byte auth tag)
    return nonce + ciphertext


def unwrap_key_local(wrapped_kek: bytes, secret: str) -> bytes:
    """Unwrap a KEK using local AES-256-GCM decryption.

    Args:
        wrapped_kek: The wrapped key bytes (nonce || ciphertext+tag)
        secret: The secret material to derive the wrapping key from

    Returns:
        Plaintext KEK bytes

    Raises:
        ValueError: If decryption fails (wrong key or tampered data)
    """
    if len(wrapped_kek) < _NONCE_SIZE + 16:  # At least nonce + auth tag
        raise ValueError("wrapped_kek too short")

    wrapping_key = _derive_wrapping_key(secret)
    nonce = wrapped_kek[:_NONCE_SIZE]
    ciphertext = wrapped_kek[_NONCE_SIZE:]

    aesgcm = AESGCM(wrapping_key)
    try:
        return aesgcm.decrypt(nonce, ciphertext, associated_data=None)
    except Exception as e:
        raise ValueError("Failed to unwrap KEK: authentication failed") from e
