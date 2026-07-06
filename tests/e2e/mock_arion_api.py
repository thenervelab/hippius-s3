import hashlib
import time
import uuid

from fastapi import FastAPI
from fastapi import File
from fastapi import Form
from fastapi import HTTPException
from fastapi import UploadFile
from fastapi.responses import StreamingResponse
from mock_faults import install_fault_controller
from pydantic import BaseModel


app = FastAPI()
# WI-19 §4.4: env/endpoint-toggled fault modes (500 / slow / truncated-body / fail-after-N /
# can_upload=false). See mock_faults.py.
fault = install_fault_controller(app, service="arion")

# In-memory store keyed by upload_id -> {"bytes": bytes, "account_ss58": str, "file_id": str}
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
    await fault.gate("upload")
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


@app.get("/download/{account_ss58}/{identifier}")
async def download(account_ss58: str, identifier: str):
    directive = await fault.gate("download")
    # identifier = file_id (SHA256 hash) — stored as backend_identifier in chunk_backend
    entry = _store.get(identifier)
    if entry is None:
        raise HTTPException(status_code=404, detail="not found")
    import io

    data = entry["bytes"]
    # F8: hand back a truncated body so the agent's chunk verify sees a short/corrupt read.
    if directive.truncate_bytes:
        data = data[: directive.truncate_bytes]
    return StreamingResponse(io.BytesIO(data), media_type="application/octet-stream")


class CanUploadRequest(BaseModel):
    user_id: str
    size_bytes: int


class CanUploadResult(BaseModel):
    result: bool
    error: str | None = None


@app.post("/can_upload")
async def can_upload(body: CanUploadRequest) -> CanUploadResult:
    directive = await fault.gate("can_upload")
    if directive.reject:
        return CanUploadResult(result=False, error="injected can_upload denial")
    return CanUploadResult(result=True, error=None)


@app.delete("/delete/{user_id}/{identifier}")
async def delete(user_id: str, identifier: str) -> DeleteResult:
    await fault.gate("delete")
    _store.pop(identifier, None)
    return DeleteResult(Success={"status": "deleted", "file_id": identifier, "user_id": user_id})


@app.get("/health")
async def health():
    return {"status": "ok"}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8002)
