import base64
import logging

from nacl.secret import SecretBox


logger = logging.getLogger(__name__)


class DecryptionError(Exception):
    pass


def decrypt_secret(encrypted_b64: str, nonce_b64: str, key_hex: str) -> str:
    """
    Decrypt encrypted secret using NaCl SecretBox.

    The encrypted_b64 contains the full encrypted message from box.encrypt(),
    which includes the nonce. The nonce_b64 parameter is provided separately
    by the API but is already embedded in the encrypted message.

    Args:
        encrypted_b64: Base64-encoded encrypted secret (includes nonce)
        nonce_b64: Base64-encoded nonce (unused, kept for API compatibility)
        key_hex: Hex-encoded decryption key (HIPPIUS_AUTH_ENCRYPTION_KEY)

    Returns:
        Decrypted secret as string

    Raises:
        DecryptionError: If decryption fails
    """
    if not key_hex:
        raise DecryptionError("Decryption key is required (HIPPIUS_AUTH_ENCRYPTION_KEY not set)")

    try:
        key_bytes = bytes.fromhex(key_hex)
    except ValueError as e:
        raise DecryptionError(f"Invalid key format: must be hex-encoded, got error: {e}") from e

    if len(key_bytes) != 32:
        raise DecryptionError(f"Invalid key length: expected 32 bytes, got {len(key_bytes)}")

    box = SecretBox(key_bytes)

    try:
        encrypted_bytes = base64.b64decode(encrypted_b64)
    except Exception as e:
        raise DecryptionError(f"Invalid base64 encrypted data: {e}") from e

    try:
        plaintext_bytes = box.decrypt(encrypted_bytes)
        return plaintext_bytes.decode("utf-8")
    except Exception as e:
        raise DecryptionError(f"Decryption failed: {e}") from e
