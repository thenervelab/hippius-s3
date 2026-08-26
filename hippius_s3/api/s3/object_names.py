"""Extra S3 keys for one object_id (same-bucket CopyObject)."""

from __future__ import annotations

from typing import Any
from typing import Literal

from hippius_s3.utils import get_query


DropKind = Literal["alias", "promoted", "last"]


async def drop_s3_name(db: Any, bucket_id: str, object_key: str) -> DropKind:
    """Remove one S3 name. Does not unpin. Raises on name conflicts (23505).

    alias: dest was only an extra name; ciphertext stays on object_id.
    promoted: primary name removed, an alias became the primary; same object_id.
    last: this was the last name; caller must soft-delete + unpin.
    """
    live = await db.fetchrow(
        get_query("get_live_object_id_by_key"),
        bucket_id,
        object_key,
    )
    if live is None:
        alias = await db.fetchrow(get_query("delete_object_name"), bucket_id, object_key)
        return "alias" if alias is not None else "last"

    promoted = await db.fetchrow(
        get_query("promote_object_name"),
        bucket_id,
        object_key,
    )
    if promoted is not None and promoted.get("object_id") is not None:
        return "promoted"
    return "last"
