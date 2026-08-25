from __future__ import annotations

import json
import uuid
from datetime import datetime
from datetime import timezone
from typing import Any

from hippius_s3.utils import get_query


async def set_object_version_address(
    db: Any,
    *,
    object_id: str,
    object_version: int,
    address: str,
    only_if_null: bool = False,
) -> None:
    """Persist the main-account address on an object version (s3-2.1).

    Written by the api in place of the PUT-time enqueue, so the Rust drain can rebuild
    the UploadChainRequest by object_id and enqueue it once the part replicates to ceph.

    only_if_null (AP-2): gate the UPDATE on `address IS NULL` so a caller whose version already
    carries an address (e.g. the append hot path) issues a no-op instead of a redundant write.
    """
    query = "set_object_version_address_if_null" if only_if_null else "set_object_version_address"
    await db.execute(
        get_query(query),
        object_id,
        int(object_version),
        address,
    )


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
    row = await db.fetchrow(
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
    # A PUT of this key must not leave a CopyObject alias shadowing it.
    await db.execute(get_query("delete_object_name"), bucket_id, object_key)
    return row


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
