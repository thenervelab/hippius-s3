from __future__ import annotations

import uuid
from datetime import datetime
from datetime import timezone
from typing import Any

from hippius_s3.utils import get_query


async def upsert_part_placeholder(
    db: Any,
    *,
    object_id: str,
    upload_id: str,
    part_number: int,
    size_bytes: int,
    etag: str,
    placeholder_cid: str = "pending",
    chunk_size_bytes: int | None = None,
    object_version: int,
) -> None:
    """Ensure a parts row exists with NOT NULL fields before uploader runs.

    Creates or updates a parts row with placeholder CID and provided metadata.
    Safe to call multiple times (idempotent on (object_id, part_number)).
    """
    # Resolve CID ID for placeholder (e.g., "pending")
    cid_row = await db.fetchrow(get_query("upsert_cid"), placeholder_cid)
    cid_id = cid_row["id"] if cid_row else None

    now = datetime.now(timezone.utc)

    if chunk_size_bytes is None:
        await db.execute(
            """
            INSERT INTO parts (part_id, upload_id, part_number, ipfs_cid, size_bytes, etag, uploaded_at, object_id, object_version, cid_id)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            ON CONFLICT (object_id, object_version, part_number) DO UPDATE SET
                ipfs_cid = EXCLUDED.ipfs_cid,
                size_bytes = EXCLUDED.size_bytes,
                etag = EXCLUDED.etag,
                uploaded_at = EXCLUDED.uploaded_at,
                cid_id = EXCLUDED.cid_id
            """,
            str(uuid.uuid4()),
            upload_id,
            int(part_number),
            placeholder_cid,
            int(size_bytes),
            etag,
            now,
            object_id,
            int(object_version),
            cid_id,
        )
        return

    await db.execute(
        """
        INSERT INTO parts (part_id, upload_id, part_number, ipfs_cid, size_bytes, etag, uploaded_at, object_id, object_version, cid_id, chunk_size_bytes)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
        ON CONFLICT (object_id, object_version, part_number) DO UPDATE SET
            ipfs_cid = EXCLUDED.ipfs_cid,
            size_bytes = EXCLUDED.size_bytes,
            etag = EXCLUDED.etag,
            uploaded_at = EXCLUDED.uploaded_at,
            cid_id = EXCLUDED.cid_id,
            chunk_size_bytes = EXCLUDED.chunk_size_bytes
        """,
        str(uuid.uuid4()),
        upload_id,
        int(part_number),
        placeholder_cid,
        int(size_bytes),
        etag,
        now,
        object_id,
        int(object_version),
        cid_id,
        int(chunk_size_bytes),
    )
