"""Unit tests for auth_service.py"""

import base64

import pytest
from nacl.secret import SecretBox

from gateway.services.auth_service import decrypt_secret


def test_decrypt_secret_with_valid_data() -> None:
    """Test decryption with valid encrypted data"""
    test_secret = "my_super_secret_key_12345"
    test_key_hex = "a" * 64

    key_bytes = bytes.fromhex(test_key_hex)
    box = SecretBox(key_bytes)

    encrypted = box.encrypt(test_secret.encode("utf-8"))

    encrypted_b64 = base64.b64encode(encrypted).decode("utf-8")
    nonce_b64 = base64.b64encode(encrypted.nonce).decode("utf-8")

    decrypted = decrypt_secret(encrypted_b64, nonce_b64, test_key_hex)

    assert decrypted == test_secret


def test_decrypt_secret_with_short_key() -> None:
    """Test decryption with a key shorter than 32 bytes raises error"""
    test_key_hex = "abcd1234"

    encrypted_b64 = "dummy_encrypted_data"
    nonce_b64 = base64.b64encode(b"0" * 24).decode("utf-8")

    with pytest.raises(Exception) as exc_info:
        decrypt_secret(encrypted_b64, nonce_b64, test_key_hex)

    assert "Invalid key length" in str(exc_info.value)


def test_decrypt_secret_with_invalid_encrypted_data() -> None:
    """Test decryption with invalid encrypted data raises exception"""
    test_key_hex = "a" * 64
    encrypted_b64 = "invalid_base64_data"
    nonce_b64 = base64.b64encode(b"0" * 24).decode("utf-8")

    with pytest.raises(Exception):
        decrypt_secret(encrypted_b64, nonce_b64, test_key_hex)


def test_decrypt_secret_with_wrong_key() -> None:
    """Test decryption with wrong key raises CryptoError"""
    test_secret = "secret"
    correct_key_hex = "a" * 64
    wrong_key_hex = "b" * 64

    correct_key_bytes = bytes.fromhex(correct_key_hex)
    box = SecretBox(correct_key_bytes)

    encrypted = box.encrypt(test_secret.encode("utf-8"))

    encrypted_b64 = base64.b64encode(encrypted).decode("utf-8")
    nonce_b64 = base64.b64encode(encrypted.nonce).decode("utf-8")

    with pytest.raises(Exception):
        decrypt_secret(encrypted_b64, nonce_b64, wrong_key_hex)


def test_decrypt_secret_with_empty_secret() -> None:
    """Test decryption of empty string"""
    test_secret = ""
    test_key_hex = "a" * 64

    key_bytes = bytes.fromhex(test_key_hex)
    box = SecretBox(key_bytes)

    encrypted = box.encrypt(test_secret.encode("utf-8"))

    encrypted_b64 = base64.b64encode(encrypted).decode("utf-8")
    nonce_b64 = base64.b64encode(encrypted.nonce).decode("utf-8")

    decrypted = decrypt_secret(encrypted_b64, nonce_b64, test_key_hex)

    assert decrypted == ""
