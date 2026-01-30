from __future__ import annotations

import json
import uuid
from datetime import datetime
from datetime import timezone
from typing import Any

from hippius_s3.utils import get_query


async def upsert_object_basic(
    db: Any,
    *,
    object_id: str,
    bucket_id: str,
    object_key: str,
    content_type: str,
    metadata: dict,
    md5_hash: str,
    size_bytes: int,
    storage_version: int,
    upload_backends: list[str] | None = None,
) -> Any:
    return await db.fetchrow(
        get_query("upsert_object_basic"),
        object_id,
        bucket_id,
        object_key,
        content_type,
        json.dumps(metadata),
        md5_hash,
        size_bytes,
        datetime.now(timezone.utc),
        int(storage_version),
        upload_backends,
    )


async def ensure_upload_row(
    db: Any, *, object_id: str, bucket_id: str, object_key: str, content_type: str, metadata: dict
) -> str:
    new_upload_id = uuid.uuid4()
    row = await db.fetchrow(
        get_query("create_multipart_upload"),
        new_upload_id,
        bucket_id,
        object_key,
        datetime.now(timezone.utc),
        content_type,
        json.dumps(metadata),
        datetime.now(timezone.utc),
        uuid.UUID(object_id),
    )
    return str(row["upload_id"]) if row else str(new_upload_id)
