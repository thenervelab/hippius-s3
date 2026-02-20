import base64
import hashlib
import io
import os
import uuid
from datetime import datetime
from datetime import timezone

import httpx
from fastapi import FastAPI
from fastapi import File
from fastapi import UploadFile
from nacl.secret import SecretBox
from pydantic import BaseModel


app = FastAPI()

IPFS_API_URL = os.getenv("IPFS_API_URL", "http://ipfs:5001")

# Access key auth encryption setup
# Uses the same test key as gateway: HIPPIUS_AUTH_ENCRYPTION_KEY
AUTH_ENCRYPTION_KEY_HEX = os.getenv(
    "HIPPIUS_AUTH_ENCRYPTION_KEY",
    "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
)
AUTH_ENCRYPTION_KEY = bytes.fromhex(AUTH_ENCRYPTION_KEY_HEX)
MOCK_ACCESS_KEY_SECRET = "e2e_test_secret_for_hip_keys"
MOCK_ACCOUNT_ADDRESS = "5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty"

# Pre-encrypt the secret at startup so every /objectstore/tokens/auth/ response is consistent
_box = SecretBox(AUTH_ENCRYPTION_KEY)
_encrypted_blob = _box.encrypt(MOCK_ACCESS_KEY_SECRET.encode())
ENCRYPTED_SECRET_B64 = base64.b64encode(_encrypted_blob).decode()
# nonce is embedded in the encrypted blob; provide a dummy for API compat
NONCE_B64 = base64.b64encode(b"\x00" * 24).decode()


class TokenAuthRequest(BaseModel):
    accessKeyId: str


class TokenAuthResponse(BaseModel):
    valid: bool
    status: str
    account_address: str
    token_type: str
    encrypted_secret: str
    nonce: str


class UploadResponse(BaseModel):
    id: str
    original_name: str
    content_type: str
    size_bytes: int
    sha256_hex: str
    cid: str
    status: str
    file_url: str
    created_at: str
    updated_at: str


class FileStatusResponse(BaseModel):
    id: str
    original_name: str
    content_type: str
    size_bytes: int
    sha256_hex: str
    cid: str
    status: str
    file_url: str
    created_at: str
    updated_at: str


async def upload_to_ipfs(content: bytes) -> str:
    async with httpx.AsyncClient(timeout=30.0) as client:
        files = {"file": io.BytesIO(content)}
        response = await client.post(f"{IPFS_API_URL}/api/v0/add?pin=false", files=files)
        response.raise_for_status()
        result = response.json()
        return result["Hash"]


file_storage = {}


@app.post("/storage-control/upload/", response_model=UploadResponse)
async def upload_file(file: UploadFile = File(...), account_ss58: str = None):
    content = await file.read()
    file_id = str(uuid.uuid4())
    size_bytes = len(content)
    sha256_hex = hashlib.sha256(content).hexdigest()

    cid = await upload_to_ipfs(content)

    now = datetime.now(timezone.utc).isoformat()

    response = UploadResponse(
        id=file_id,
        original_name=file.filename or "unknown",
        content_type=file.content_type or "application/octet-stream",
        size_bytes=size_bytes,
        sha256_hex=sha256_hex,
        cid=cid,
        status="completed",
        file_url=f"https://mock-api/files/{file_id}",
        created_at=now,
        updated_at=now,
    )

    file_storage[file_id] = response
    return response


@app.get("/storage-control/files/{file_id}/", response_model=FileStatusResponse)
async def get_file_status(file_id: str):
    if file_id not in file_storage:
        upload_resp = file_storage.get(file_id)
        if not upload_resp:
            return FileStatusResponse(
                id=file_id,
                original_name="unknown",
                content_type="application/octet-stream",
                size_bytes=0,
                sha256_hex="",
                cid="QmMockCID",
                status="completed",
                file_url=f"https://mock-api/files/{file_id}",
                created_at=datetime.now(timezone.utc).isoformat(),
                updated_at=datetime.now(timezone.utc).isoformat(),
            )

    return FileStatusResponse(**file_storage[file_id].model_dump())


@app.post("/objectstore/tokens/auth/", response_model=TokenAuthResponse)
async def token_auth(payload: TokenAuthRequest):
    """Mock access key authentication endpoint.

    Accepts any hip_* key and returns a valid encrypted secret
    that the gateway can decrypt with HIPPIUS_AUTH_ENCRYPTION_KEY.
    """
    access_key = payload.accessKeyId
    if not access_key.startswith("hip_"):
        return TokenAuthResponse(
            valid=False,
            status="invalid",
            account_address="",
            token_type="",
            encrypted_secret="",
            nonce="",
        )

    return TokenAuthResponse(
        valid=True,
        status="active",
        account_address=MOCK_ACCOUNT_ADDRESS,
        token_type="master",
        encrypted_secret=ENCRYPTED_SECRET_B64,
        nonce=NONCE_B64,
    )


@app.get("/health")
async def health():
    return {"status": "ok"}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8001)
