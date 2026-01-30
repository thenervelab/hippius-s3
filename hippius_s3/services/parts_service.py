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
    chunk_size_bytes: int | None = None,
    object_version: int,
    chunk_cipher_sizes: list[int] | None = None,
) -> None:
    """Ensure a parts row exists with NOT NULL fields before uploader runs.

    Creates or updates a parts row and optionally bulk-inserts part_chunks
    placeholders so that insert_chunk_backend (which joins on part_chunks)
    can find them.

    Safe to call multiple times (idempotent on (object_id, part_number)).
    """
    now = datetime.now(timezone.utc)

    if chunk_size_bytes is None:
        part_id = await db.fetchval(
            """
            INSERT INTO parts (part_id, upload_id, part_number, size_bytes, etag, uploaded_at, object_id, object_version)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            ON CONFLICT (object_id, object_version, part_number) DO UPDATE SET
                size_bytes = EXCLUDED.size_bytes,
                etag = EXCLUDED.etag,
                uploaded_at = EXCLUDED.uploaded_at
            RETURNING part_id
            """,
            str(uuid.uuid4()),
            upload_id,
            int(part_number),
            int(size_bytes),
            etag,
            now,
            object_id,
            int(object_version),
        )
    else:
        part_id = await db.fetchval(
            """
            INSERT INTO parts (part_id, upload_id, part_number, size_bytes, etag, uploaded_at, object_id, object_version, chunk_size_bytes)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ON CONFLICT (object_id, object_version, part_number) DO UPDATE SET
                size_bytes = EXCLUDED.size_bytes,
                etag = EXCLUDED.etag,
                uploaded_at = EXCLUDED.uploaded_at,
                chunk_size_bytes = EXCLUDED.chunk_size_bytes
            RETURNING part_id
            """,
            str(uuid.uuid4()),
            upload_id,
            int(part_number),
            int(size_bytes),
            etag,
            now,
            object_id,
            int(object_version),
            int(chunk_size_bytes),
        )

    # Bulk-insert part_chunks placeholders if cipher sizes are provided
    if chunk_cipher_sizes is not None and part_id is not None:
        chunk_indexes = list(range(len(chunk_cipher_sizes)))
        if len(chunk_indexes) != len(chunk_cipher_sizes):
            raise ValueError(
                f"chunk_indexes length {len(chunk_indexes)} != chunk_cipher_sizes length {len(chunk_cipher_sizes)}"
            )
        await db.execute(
            get_query("insert_part_chunk_placeholders"),
            part_id,
            chunk_indexes,
            [int(s) for s in chunk_cipher_sizes],
        )
