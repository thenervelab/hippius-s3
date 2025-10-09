from __future__ import annotations

from typing import Any


async def read_object_info(db: Any, bucket_name: str, object_key: str, account_id: str | None) -> dict | None:
    from hippius_s3.utils import get_query

    row = await db.fetchrow(get_query("get_object_for_download_with_permissions"), bucket_name, object_key, account_id)
    return dict(row) if row else None


async def read_parts_manifest(db: Any, object_id: str) -> list[dict]:
    from hippius_s3.services.manifest_service import ManifestService

    return await ManifestService.build_initial_download_chunks(db, {"object_id": object_id})


async def read_part_plain_and_chunk_size(db: Any, object_id: str, part_number: int) -> tuple[int, int]:
    from hippius_s3.metadata.meta_reader import read_db_meta

    dbm = await read_db_meta(db, object_id, int(part_number))
    if not dbm:
        return 0, 0
    ps = int(dbm.get("plain_size") or 0)
    cs = int(dbm.get("chunk_size_bytes") or 0)
    return ps, cs
