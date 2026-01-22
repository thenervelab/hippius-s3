"""Mock OVH KMS server for E2E testing.

Implements a simple data key generation/decryption API that mimics OVH KMS behavior.
Uses XOR with a fixed key for "encryption" - this is NOT secure and is
only for testing purposes.
"""

import base64
import os
import secrets
import ssl
from pathlib import Path

from fastapi import FastAPI
from fastapi import HTTPException
from pydantic import BaseModel


app = FastAPI()

# Mock "HSM" master key - XOR-based wrap for testing only
MOCK_MASTER_KEY = os.environ.get(
    "MOCK_KMS_MASTER_KEY", "test-master-key-0123456789abcdef"
).encode()

# Mock OKMS ID for path matching
MOCK_OKMS_ID = os.environ.get("MOCK_KMS_OKMS_ID", "mock-okms-id")


class GenerateDataKeyRequest(BaseModel):
    name: str = "kek"
    size: int = 256


class GenerateDataKeyResponse(BaseModel):
    plaintext: str  # Base64-encoded plaintext key
    key: str  # "Wrapped" key (mock JWE-like format)


class DecryptDataKeyRequest(BaseModel):
    key: str  # The wrapped key from GenerateDataKeyResponse


class DecryptDataKeyResponse(BaseModel):
    plaintext: str  # Base64-encoded plaintext key


def xor_bytes(data: bytes) -> bytes:
    """XOR data with the mock master key (for testing only)."""
    key_repeated = (MOCK_MASTER_KEY * ((len(data) // len(MOCK_MASTER_KEY)) + 1))[
        : len(data)
    ]
    return bytes(a ^ b for a, b in zip(data, key_repeated, strict=True))


@app.post("/api/{okms_id}/v1/servicekey/{key_id}/datakey", response_model=GenerateDataKeyResponse)
async def generate_data_key(okms_id: str, key_id: str, request: GenerateDataKeyRequest):
    """Generate a new data key.

    Mimics OVH KMS datakey endpoint - generates a random key and returns
    both the plaintext (for immediate use) and wrapped version (for storage).
    """
    # Validate OKMS ID to catch path construction bugs in client
    if okms_id != MOCK_OKMS_ID:
        raise HTTPException(status_code=404, detail=f"Unknown OKMS ID: {okms_id}")

    # Validate key size (OVH supports 128, 192, 256 bit AES keys)
    if request.size not in (128, 192, 256):
        raise HTTPException(status_code=400, detail=f"Invalid key size: {request.size}. Must be 128, 192, or 256")

    # Generate random key (size is in bits, so divide by 8 for bytes)
    key_bytes = secrets.token_bytes(request.size // 8)

    # Simple XOR wrap (NOT SECURE - testing only!)
    wrapped = xor_bytes(key_bytes)

    # Return mock JWE-like format (prefix helps identify it as wrapped)
    wrapped_key = f"mock-jwe.{base64.b64encode(wrapped).decode()}"

    return GenerateDataKeyResponse(
        plaintext=base64.b64encode(key_bytes).decode(),
        key=wrapped_key,
    )


@app.post("/api/{okms_id}/v1/servicekey/{key_id}/datakey/decrypt", response_model=DecryptDataKeyResponse)
async def decrypt_data_key(okms_id: str, key_id: str, request: DecryptDataKeyRequest):
    """Decrypt (unwrap) a data key.

    Mimics OVH KMS datakey/decrypt endpoint.
    """
    # Validate OKMS ID to catch path construction bugs in client
    if okms_id != MOCK_OKMS_ID:
        raise HTTPException(status_code=404, detail=f"Unknown OKMS ID: {okms_id}")

    try:
        # Parse mock JWE format
        if not request.key.startswith("mock-jwe."):
            raise HTTPException(status_code=400, detail="Invalid wrapped key format")

        wrapped_b64 = request.key[len("mock-jwe."):]
        wrapped = base64.b64decode(wrapped_b64)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid wrapped key") from None

    # XOR unwrap (symmetric operation)
    plaintext = xor_bytes(wrapped)
    plaintext_b64 = base64.b64encode(plaintext).decode()

    return DecryptDataKeyResponse(plaintext=plaintext_b64)


@app.get("/health")
async def health():
    """Health check endpoint."""
    return {"status": "ok", "service": "mock-kms"}


if __name__ == "__main__":
    import uvicorn

    # Check for TLS certificates
    cert_path = os.environ.get("KMS_CERT_PATH", "/certs/server.crt")
    key_path = os.environ.get("KMS_KEY_PATH", "/certs/server.key")
    ca_path = os.environ.get("KMS_CA_PATH", "/certs/ca.crt")
    require_client_cert = os.environ.get("MOCK_KMS_REQUIRE_CLIENT_CERT", "false").lower() == "true"

    use_tls = Path(cert_path).exists() and Path(key_path).exists()
    has_ca = Path(ca_path).exists()

    if use_tls:
        # Determine client cert mode:
        # - CERT_REQUIRED: fail if no client cert (strict mTLS)
        # - CERT_OPTIONAL: validate client cert if provided, allow without (mTLS + healthcheck)
        # - CERT_NONE: don't request or validate client certs (TLS only)
        if require_client_cert and has_ca:
            ssl_cert_reqs = ssl.CERT_REQUIRED
            ssl_ca_certs = ca_path
            print(f"mTLS enabled: requiring client certificates (CA: {ca_path})")
        elif has_ca:
            # CERT_OPTIONAL: validates client certs if provided, allows healthcheck without cert
            ssl_cert_reqs = ssl.CERT_OPTIONAL
            ssl_ca_certs = ca_path
            print(f"mTLS enabled: client certificates optional but validated (CA: {ca_path})")
        else:
            ssl_cert_reqs = ssl.CERT_NONE
            ssl_ca_certs = None
            print("TLS enabled: no client certificate verification")

        uvicorn.run(
            app,
            host="0.0.0.0",
            port=8443,
            ssl_keyfile=key_path,
            ssl_certfile=cert_path,
            ssl_ca_certs=ssl_ca_certs,
            ssl_cert_reqs=ssl_cert_reqs,
        )
    else:
        # Run without TLS for simpler testing
        print("WARNING: Running without TLS (certificates not found)")
        uvicorn.run(app, host="0.0.0.0", port=8443)
