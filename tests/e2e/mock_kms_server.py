"""Mock OVH KMS server for E2E testing.

Implements a simple key wrap/unwrap API that mimics OVH KMS behavior.
Uses XOR with a fixed key for "encryption" - this is NOT secure and is
only for testing purposes.
"""

import base64
import os
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


class WrapRequest(BaseModel):
    plaintext: str  # Base64-encoded


class WrapResponse(BaseModel):
    ciphertext: str  # Base64-encoded


class UnwrapRequest(BaseModel):
    ciphertext: str  # Base64-encoded


class UnwrapResponse(BaseModel):
    plaintext: str  # Base64-encoded


def xor_bytes(data: bytes) -> bytes:
    """XOR data with the mock master key (for testing only)."""
    key_repeated = (MOCK_MASTER_KEY * ((len(data) // len(MOCK_MASTER_KEY)) + 1))[
        : len(data)
    ]
    return bytes(a ^ b for a, b in zip(data, key_repeated, strict=True))


@app.post("/v1/servicekey/{key_id}/wrap", response_model=WrapResponse)
async def wrap_key(key_id: str, request: WrapRequest):
    """Wrap a key.

    Mimics OVH KMS wrapKey endpoint.
    """
    try:
        plaintext = base64.b64decode(request.plaintext)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid base64 plaintext") from None

    # Simple XOR wrap (NOT SECURE - testing only!)
    wrapped = xor_bytes(plaintext)
    ciphertext = base64.b64encode(wrapped).decode()

    return WrapResponse(ciphertext=ciphertext)


@app.post("/v1/servicekey/{key_id}/unwrap", response_model=UnwrapResponse)
async def unwrap_key(key_id: str, request: UnwrapRequest):
    """Unwrap a key.

    Mimics OVH KMS unwrapKey endpoint.
    """
    try:
        wrapped = base64.b64decode(request.ciphertext)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid base64 ciphertext") from None

    # XOR unwrap (symmetric operation)
    plaintext = xor_bytes(wrapped)
    plaintext_b64 = base64.b64encode(plaintext).decode()

    return UnwrapResponse(plaintext=plaintext_b64)


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
