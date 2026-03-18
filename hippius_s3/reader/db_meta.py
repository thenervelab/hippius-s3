from __future__ import annotations

from typing import Any
from typing import Optional


async def read_object_info(db: Any, bucket_name: str, object_key: str, account_id: str | None) -> dict | None:
    from hippius_s3.utils import get_query

    row = await db.fetchrow(get_query("get_object_for_download_with_permissions"), bucket_name, object_key)
    return dict(row) if row else None


async def read_parts_list(db: Any, object_id: str, object_version: Optional[int] = None) -> list[dict]:
    from hippius_s3.services.parts_catalog import PartsCatalog

    payload: dict[str, Any] = {"object_id": object_id}
    if object_version is not None:
        # Keep as Any to satisfy downstream JSON-agnostic consumers
        payload["object_version"] = int(object_version)
    return await PartsCatalog.build_initial_download_chunks(db, payload)


# Backwards-compat alias
read_parts_manifest = read_parts_list


async def read_part_plain_and_chunk_size(
    db: Any, object_id: str, part_number: int, object_version: int
) -> tuple[int, int]:
    from hippius_s3.metadata.meta_reader import read_db_meta

    dbm = await read_db_meta(db, object_id, int(part_number), int(object_version))
    if not dbm:
        return 0, 0
    ps = int(dbm.get("plain_size") or 0)
    cs = int(dbm.get("chunk_size_bytes") or 0)
    return ps, cs


async def read_parts_plain_and_chunk_sizes_batch(
    db: Any, object_id: str, part_numbers: list[int], object_version: int
) -> dict[int, tuple[int, int]]:
    """Batch-read plain_size and chunk_size for multiple parts in a single DB query."""
    if not part_numbers:
        return {}
    rows = await db.fetch(
        """
        SELECT part_number, size_bytes, chunk_size_bytes
        FROM parts
        WHERE object_id = $1 AND object_version = $2 AND part_number = ANY($3)
        """,
        object_id,
        int(object_version),
        part_numbers,
    )
    result: dict[int, tuple[int, int]] = {}
    for row in rows or []:
        pn = int(row["part_number"])
        ps = int(row["size_bytes"] or 0)
        cs = int(row["chunk_size_bytes"] or 0)
        result[pn] = (ps, cs)
    return result
