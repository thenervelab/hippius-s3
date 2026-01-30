import os
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

# In-memory store: file_id -> {"bytes": bytes, "account_ss58": str}
_store: dict[str, dict] = {}


class UploadResult(BaseModel):
    upload_id: str
    timestamp: int
    size_bytes: int


class DeleteResult(BaseModel):
    Success: dict


@app.post("/upload")
async def upload(file: UploadFile = File(...), account_ss58: str = Form(...)) -> UploadResult:
    content = await file.read()
    file_id = uuid.uuid4().hex
    _store[file_id] = {"bytes": content, "account_ss58": account_ss58}
    return UploadResult(
        upload_id=file_id,
        timestamp=int(time.time()),
        size_bytes=len(content),
    )


@app.get("/download/{account_ss58}/{file_id}")
async def download(account_ss58: str, file_id: str):
    entry = _store.get(file_id)
    if entry is None:
        raise HTTPException(status_code=404, detail="not found")
    import io

    return StreamingResponse(io.BytesIO(entry["bytes"]), media_type="application/octet-stream")


@app.delete("/delete/{file_id}")
async def delete(file_id: str) -> DeleteResult:
    entry = _store.pop(file_id, None)
    account = entry["account_ss58"] if entry else "unknown"
    return DeleteResult(
        Success={"status": "deleted", "file_id": file_id, "user_id": account}
    )


@app.get("/health")
async def health():
    return {"status": "ok"}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8002)
