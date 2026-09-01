import hashlib
import time
import uuid

from fastapi import FastAPI
from fastapi import File
from fastapi import Form
from fastapi import HTTPException
from fastapi import Request
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

# Distinct peers seen on /download. A new TCP connection always arrives from a new ephemeral
# source port, so len(_download_peers) counts connections while _download_requests counts
# requests. The gap between them is what proves the downloader reuses a keep-alive pool
# rather than handshaking per chunk — see test_Downloader_ConnectionReuse.py. Toxiproxy sits
# in front of us but relays 1:1, so a per-request downstream connection still shows up here
# as a distinct peer.
_download_peers: set[tuple[str, int]] = set()
_download_requests = 0


@app.middleware("http")
async def _track_download_connections(request: Request, call_next):
    global _download_requests
    if request.url.path.startswith("/download/") and request.client is not None:
        _download_peers.add((request.client.host, request.client.port))
        _download_requests += 1
    return await call_next(request)


@app.get("/debug/download_stats")
async def download_stats() -> dict:
    return {"connections": len(_download_peers), "requests": _download_requests}


@app.post("/debug/reset_download_stats")
async def reset_download_stats() -> dict:
    global _download_requests
    _download_peers.clear()
    _download_requests = 0
    return {"status": "reset"}


# ATS stand-in: docker-compose.e2e.yml points the api's ATS_CACHE_ENDPOINT here, so the gateway's
# purge middleware fires its PURGE fan-out at this mock instead of silently no-op'ing on an empty
# endpoint list. Recording (host, path) makes purge behaviour assertable end-to-end — see
# test_AtsPurgeSuppression.py. The catch-all is method-scoped to PURGE, so it cannot shadow the
# GET/POST routes above.
_purges: list[dict] = []


@app.get("/debug/purges")
async def purge_log() -> list[dict]:
    return _purges


@app.post("/debug/reset_purges")
async def reset_purges() -> dict:
    _purges.clear()
    return {"status": "reset"}


@app.api_route("/{purge_path:path}", methods=["PURGE"])
async def record_purge(purge_path: str, request: Request) -> dict:
    _purges.append({"host": request.headers.get("host", ""), "path": f"/{purge_path}", "ts": time.time()})
    return {"status": "purged"}


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
