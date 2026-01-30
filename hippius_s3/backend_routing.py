"""Smart backend routing: config-aware intersection for uploads, data-driven for downloads/unpins."""

from __future__ import annotations

import logging
from typing import Any

from hippius_s3.utils import get_query


logger = logging.getLogger(__name__)


def compute_effective_backends(
    requested: list[str] | None,
    allowed: list[str],
    *,
    context: dict | None = None,
    raise_on_empty: bool = True,
) -> list[str] | None:
    """Intersect *requested* backends with the *allowed* config list.

    Returns
    -------
    - ``None`` when *requested* is ``None`` (caller should use config default).
    - The intersection list when *requested* is provided.
    - On empty intersection: raises ``ValueError`` if *raise_on_empty* is True,
      otherwise returns ``None`` so the caller can fall back to config.
    """
    if requested is None:
        return None

    allowed_set = set(allowed)
    effective = [b for b in requested if b in allowed_set]
    dropped = [b for b in requested if b not in allowed_set]

    if dropped:
        logger.warning(
            "Backends dropped (not in allowed config): dropped=%s requested=%s allowed=%s context=%s",
            dropped,
            requested,
            allowed,
            context,
        )

    if not effective:
        if raise_on_empty:
            raise ValueError(
                f"No backends remain after intersection: requested={requested} allowed={allowed} context={context}"
            )
        return None

    return effective


async def resolve_object_backends(
    db: Any,
    object_id: str,
    object_version: int | None = None,
) -> list[str]:
    """Query distinct backends that hold active data for an object.

    Returns an empty list when no backends are found (e.g. workers haven't
    finished yet — a race condition the caller should handle gracefully).
    """
    rows = await db.fetch(
        get_query("get_object_backends"),
        object_id,
        object_version,
    )
    return [str(r["backend"]) for r in rows] if rows else []
