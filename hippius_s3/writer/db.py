from __future__ import annotations

import json
import uuid
from collections.abc import Awaitable
from collections.abc import Callable
from datetime import datetime
from datetime import timezone
from typing import Any

import asyncpg

from hippius_s3.utils import get_query


OBJECT_VERSION_PK_CONSTRAINT = "object_versions_pkey"


async def retry_on_object_version_conflict(reserve: Callable[[], Awaitable[Any]], *, attempts: int = 3) -> Any:
    # The upsert_object_* queries allocate the next version as
    # GREATEST(objects.current_object_version, MAX(object_versions.object_version)) + 1. The
    # row-locked counter closes the PUT-vs-PUT race, but the MAX() floor is snapshot-stale under
    # READ COMMITTED, so a concurrent create_migration_version (which inserts a version WITHOUT
    # bumping current_object_version) can hand back a colliding version and raise
    # object_versions_pkey. Retrying re-reads the committed MAX and resolves it — but only if each
    # reserve() runs in its own autocommit statement / fresh transaction (a fresh snapshot). Scoped
    # to that one constraint so we never mask an unrelated unique violation, and bounded so a
    # persistent collision is a real error and surfaces.
    for attempt in range(attempts):
        try:
            return await reserve()
        except asyncpg.exceptions.UniqueViolationError as exc:
            if exc.constraint_name != OBJECT_VERSION_PK_CONSTRAINT or attempt == attempts - 1:
                raise
    raise RuntimeError("retry_on_object_version_conflict exhausted without returning")


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
