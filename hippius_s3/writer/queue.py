from __future__ import annotations

from typing import Any
from typing import Iterable

from hippius_s3.queue import Chunk
from hippius_s3.queue import UploadChainRequest
from hippius_s3.queue import enqueue_upload_request


async def enqueue_upload(
    *,
    redis_client: Any,
    address: str,
    bucket_name: str,
    object_key: str,
    object_id: str,
    object_version: int,
    upload_id: str,
    chunk_ids: Iterable[int],
) -> None:
    payload = UploadChainRequest(
        address=address,
        bucket_name=bucket_name,
        object_key=object_key,
        object_id=object_id,
        object_version=int(object_version),
        chunks=[Chunk(id=int(i)) for i in chunk_ids],
        upload_id=str(upload_id),
    )
    await enqueue_upload_request(payload, redis_client)
