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
from pydantic import BaseModel

app = FastAPI()

IPFS_API_URL = os.getenv("IPFS_API_URL", "http://ipfs:5001")


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
async def upload_file(file: UploadFile = File(...)):
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


@app.get("/health")
async def health():
    return {"status": "ok"}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8001)
