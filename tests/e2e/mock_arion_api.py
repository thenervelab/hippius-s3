import hashlib
import time
import uuid

from fastapi import FastAPI
from fastapi import File
from fastapi import Form
from fastapi import HTTPException
from fastapi import UploadFile
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

app = FastAPI()

# In-memory store keyed by file_id (SHA256 of filename) -> {"bytes": bytes, "account_ss58": str, "upload_id": str}
_store: dict[str, dict] = {}


class UploadResult(BaseModel):
    upload_id: str
    file_id: str
    timestamp: int
    size_bytes: int


class DeleteResult(BaseModel):
    Success: dict


@app.post("/upload")
async def upload(file: UploadFile = File(...), account_ss58: str = Form(...)) -> UploadResult:
    content = await file.read()
    upload_id = uuid.uuid4().hex
    # Mirror real Arion: file_id = SHA256(filename)
    file_name = file.filename or "unknown"
    file_id = hashlib.sha256(file_name.encode()).hexdigest()
    _store[file_id] = {"bytes": content, "account_ss58": account_ss58, "upload_id": upload_id}
    return UploadResult(
        upload_id=upload_id,
        file_id=file_id,
        timestamp=int(time.time()),
        size_bytes=len(content),
    )


@app.get("/download/{account_ss58}/{file_id}")
async def download(account_ss58: str, file_id: str):
    # file_id = SHA256(filename) — same value stored as backend_identifier in chunk_backend
    entry = _store.get(file_id)
    if entry is None:
        raise HTTPException(status_code=404, detail="not found")
    import io

    return StreamingResponse(io.BytesIO(entry["bytes"]), media_type="application/octet-stream")


class CanUploadRequest(BaseModel):
    user_id: str
    size_bytes: int


class CanUploadResult(BaseModel):
    result: bool
    error: str | None = None


@app.post("/can_upload")
async def can_upload(body: CanUploadRequest) -> CanUploadResult:
    return CanUploadResult(result=True, error=None)


@app.delete("/delete/{user_id}/{file_id}")
async def delete(user_id: str, file_id: str) -> DeleteResult:
    entry = _store.pop(file_id, None)
    return DeleteResult(
        Success={"status": "deleted", "file_id": file_id, "user_id": user_id}
    )


@app.get("/health")
async def health():
    return {"status": "ok"}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8002)
