"""Extra S3 keys for one object_id (same-bucket CopyObject)."""

from __future__ import annotations

from typing import Any
from typing import Literal

from hippius_s3.utils import get_query


DropKind = Literal["alias", "promoted", "last"]


async def drop_s3_name(db: Any, bucket_id: str, object_key: str) -> DropKind:
    """Remove one S3 name.

    alias: dest was only an extra name; object stays.
    promoted: primary name removed, an alias became the primary.
    last: this was the last name; caller must soft-delete + unpin.
    """
    alias = await db.fetchrow(get_query("delete_object_name"), bucket_id, object_key)
    if alias is not None:
        return "alias"

    promoted = await db.fetchrow(
        get_query("promote_object_name"),
        bucket_id,
        object_key,
    )
    if promoted is not None:
        return "promoted"

    return "last"
