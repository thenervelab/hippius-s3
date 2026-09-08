import base64
import hashlib
import os
import uuid
from datetime import datetime
from datetime import timezone

from fastapi import FastAPI
from fastapi import File
from fastapi import UploadFile
from mock_faults import install_fault_controller
from nacl.secret import SecretBox
from pydantic import BaseModel


app = FastAPI()
# WI-19 §4.4: env/endpoint-toggled fault modes (500 / slow / fail-after-N). See mock_faults.py.
fault = install_fault_controller(app, service="hippius-api")

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


file_storage = {}


@app.post("/storage-control/upload/", response_model=UploadResponse)
async def upload_file(file: UploadFile = File(...), account_ss58: str = None):
    await fault.gate("upload")
    content = await file.read()
    file_id = str(uuid.uuid4())
    size_bytes = len(content)
    sha256_hex = hashlib.sha256(content).hexdigest()

    cid = f"Qm{hashlib.sha256(content).hexdigest()[:44]}"

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
    await fault.gate("status")
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

    Convention used by the e2e stack:
      - `hip_sub_*`  -> sub-token (token_type="sub")
      - any other `hip_*` -> master (token_type="master")
      - anything else -> invalid
    All valid keys map to the same MOCK_ACCOUNT_ADDRESS so the sub-token
    enforcement logic sees the owner-match intra-account case.
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

    token_type = "sub" if access_key.startswith("hip_sub_") else "master"
    return TokenAuthResponse(
        valid=True,
        status="active",
        account_address=MOCK_ACCOUNT_ADDRESS,
        token_type=token_type,
        encrypted_secret=ENCRYPTED_SECRET_B64,
        nonce=NONCE_B64,
    )


# --------------------------------------------------------------------------- S3 billing plans
#
# Mirrors GET /api/s3/plans/accounts/ — one endpoint carrying both the catalog (`plans`) and the
# paginated account roll (`results`).
#
# Registered under BOTH paths because HIPPIUS_API_BASE_URL differs between environments: in prod it
# ends in /api so the client requests /api/s3/plans/accounts/, while against this mock the base has
# no prefix and the same relative path resolves to /s3/plans/accounts/. The `next` url we hand back
# is absolute-from-root, so page 2 arrives on the /api form either way.
#
# MOCK_ACCOUNT_ADDRESS starts on NO plan, so the default e2e stack exercises the unchanged
# pay-as-you-go path and every existing test keeps passing. POST /_plans swaps that at runtime.
plan_catalog: dict = {
    "pro": {
        "h256": "0x96e900000000000000000000000000000000000000000000000000000000028f",
        "storage_bytes": 10995116277760,
    },
    "business": {
        "h256": "0x44e0000000000000000000000000000000000000000000000000000000000c67",
        "storage_bytes": 54975581388800,
    },
}
plan_accounts: list[dict] = []


def _plans_page():
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "count": len(plan_accounts),
        "next": None,
        "previous": None,
        "plans": plan_catalog,
        "results": plan_accounts,
    }


@app.get("/s3/plans/accounts/")
async def s3_plan_accounts(page: int = 1, page_size: int = 500):
    await fault.gate("plans")
    return _plans_page()


@app.get("/api/s3/plans/accounts/")
async def s3_plan_accounts_api_prefixed(page: int = 1, page_size: int = 500):
    await fault.gate("plans")
    return _plans_page()


@app.post("/_plans")
async def set_plans(payload: dict):
    """Test hook. `{"plans": {...}, "results": [...]}` — either key may be omitted."""
    global plan_catalog, plan_accounts
    if "plans" in payload:
        plan_catalog = payload["plans"]
    if "results" in payload:
        plan_accounts = payload["results"]
    return {"plans": len(plan_catalog), "accounts": len(plan_accounts)}


@app.get("/health")
async def health():
    return {"status": "ok"}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8001)
