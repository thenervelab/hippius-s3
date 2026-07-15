from __future__ import annotations

from collections.abc import Awaitable
from collections.abc import Callable
from typing import Any

import asyncpg


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
    #
    # Lives here (not in writer/db.py) so repositories/objects.py — imported by the gateway via
    # repositories/__init__ — can use it without dragging the whole writer/crypto/KMS import stack
    # into the gateway process.
    for attempt in range(attempts):
        try:
            return await reserve()
        except asyncpg.exceptions.UniqueViolationError as exc:
            # asyncpg populates constraint_name dynamically from the error fields (not in the type
            # stubs), so read it defensively — a missing/other constraint is not our race.
            if getattr(exc, "constraint_name", None) != OBJECT_VERSION_PK_CONSTRAINT or attempt == attempts - 1:
                raise
    raise RuntimeError("retry_on_object_version_conflict exhausted without returning")
