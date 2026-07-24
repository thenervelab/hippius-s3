import json
import logging
from typing import Any


logger = logging.getLogger(__name__)


async def record_cached(conn: Any, object_id: Any, object_version: int, part_number: int) -> None:
    """Mark a part as materialized on the cache FS so the janitor's SQL discovery can find it.

    Best-effort: eviction discovery must never fail a PUT/GET pipeline. A lost row is
    backfilled by the janitor's walk sweep, so the cost of swallowing here is at most a
    delayed eviction — never a failed upload. That asymmetry is why this is one of the
    repo's few sanctioned swallows (see CLAUDE.md 9.3 on try/except discipline).
    """
    try:
        await conn.execute(
            """
            INSERT INTO fs_cache_inventory (object_id, object_version, part_number)
            VALUES ($1, $2, $3)
            ON CONFLICT (object_id, object_version, part_number) DO UPDATE SET cached_at = now()
            """,
            str(object_id),
            object_version,
            part_number,
        )
    except Exception as exc:
        logger.warning(
            "fs_cache_inventory record failed (walk sweep will backfill): %s v%s p%s: %s",
            object_id,
            object_version,
            part_number,
            exc,
        )


async def record_cached_batch(conn: Any, rows: list[tuple[Any, int, int]]) -> None:
    """Upsert many inventory rows in one statement, used by the walk sweep's backfill.

    Best-effort like `record_cached`: the walk sweep must never die on an advisory write.
    """
    if not rows:
        return

    object_ids = [str(oid) for oid, _, _ in rows]
    versions = [version for _, version, _ in rows]
    part_numbers = [part for _, _, part in rows]
    try:
        await conn.execute(
            """
            INSERT INTO fs_cache_inventory (object_id, object_version, part_number)
            SELECT * FROM unnest($1::text[], $2::bigint[], $3::bigint[])
            ON CONFLICT (object_id, object_version, part_number) DO UPDATE SET cached_at = now()
            """,
            object_ids,
            versions,
            part_numbers,
        )
    except Exception as exc:
        logger.warning("fs_cache_inventory batch record failed (walk sweep will backfill): %s rows: %s", len(rows), exc)


async def clear_cached(conn: Any, object_id: Any, object_version: int, part_number: int) -> None:
    """Drop a part from the inventory after the janitor evicts it from the cache FS.

    Raises on failure by design: the sole caller is the janitor, which handles per-item
    errors — a silently-retained row here would resurrect an already-deleted part as an
    eviction candidate.
    """
    await conn.execute(
        "DELETE FROM fs_cache_inventory WHERE object_id = $1 AND object_version = $2 AND part_number = $3",
        str(object_id),
        object_version,
        part_number,
    )


async def get_janitor_state(conn: Any, key: str) -> dict[str, Any] | None:
    """Read the janitor's durable JSONB state (e.g. the keyset eviction cursor)."""
    raw = await conn.fetchval("SELECT value FROM janitor_state WHERE key = $1", key)
    if raw is None:
        return None
    return json.loads(raw)


async def set_janitor_state(conn: Any, key: str, value: dict[str, Any]) -> None:
    """Upsert the janitor's durable JSONB state, bumping updated_at on replace."""
    await conn.execute(
        """
        INSERT INTO janitor_state (key, value)
        VALUES ($1, $2::jsonb)
        ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = now()
        """,
        key,
        json.dumps(value),
    )
