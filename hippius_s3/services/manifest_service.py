from __future__ import annotations

import asyncio
from typing import Any


class ManifestService:
    @staticmethod
    async def build_initial_download_chunks(db: Any, object_info: dict) -> list[dict]:
        """
        Build an initial manifest from the DB parts table (joined to cids), 0-based.
        Falls back to object-level CID (via objects.cid_id then objects.ipfs_cid) to synthesize base(0) when needed.
        """
        # Prefer explicit parts when available
        try:
            part_rows = await db.fetch(
                """
                SELECT p.part_number, c.cid, p.size_bytes
                FROM parts p
                JOIN cids c ON p.cid_id = c.id
                WHERE p.object_id = $1
                ORDER BY p.part_number
                """,
                object_info["object_id"],
            )
            built: list[dict] = [
                {
                    "part_number": int(r[0]),
                    "cid": (r[1] or "").strip(),
                    "size_bytes": int(r[2] or 0),
                }
                for r in part_rows
                if (r[1] or "").strip() and str(r[1]).strip().lower() not in {"none", "pending"}
            ]
            # If base(0) missing, synthesize it using object-level CID with computed base size
            if not any(int(c.get("part_number", -1)) == 0 for c in built):
                # Compute base size as total size minus sum of appended parts
                try:
                    total_size = int(object_info.get("size_bytes") or 0)
                except Exception:
                    total_size = 0
                appended_size = 0
                try:
                    appended_size = sum(
                        int(c.get("size_bytes") or 0) for c in built if int(c.get("part_number", -1)) >= 1
                    )
                except Exception:
                    appended_size = 0
                base_size = max(total_size - appended_size, 0)
                base_cid = await db.fetchval(
                    "SELECT c.cid FROM objects o JOIN cids c ON o.cid_id = c.id WHERE o.object_id = $1",
                    object_info["object_id"],
                )
                if not base_cid:
                    base_cid = await db.fetchval(
                        "SELECT ipfs_cid FROM objects WHERE object_id = $1",
                        object_info["object_id"],
                    )
                if base_cid and str(base_cid).strip().lower() not in {"", "none", "pending"}:
                    built.insert(
                        0,
                        {
                            "part_number": 0,
                            "cid": str(base_cid),
                            "size_bytes": int(base_size),
                        },
                    )
            if built:
                return built
        except Exception:
            # Fall through to object-level CID synthesis path
            pass

        # Synthesize a single-part manifest if object-level CID exists
        try:
            base_cid = await db.fetchval(
                "SELECT c.cid FROM objects o JOIN cids c ON o.cid_id = c.id WHERE o.object_id = $1",
                object_info["object_id"],
            )
            if not base_cid:
                base_cid = await db.fetchval(
                    "SELECT ipfs_cid FROM objects WHERE object_id = $1",
                    object_info["object_id"],
                )
            if base_cid and str(base_cid).strip().lower() not in {"", "none", "pending"}:
                return [
                    {
                        "part_number": 0,
                        "cid": str(base_cid),
                        "size_bytes": int(object_info.get("size_bytes") or 0),
                    }
                ]
        except Exception:
            pass

        return []

    @staticmethod
    async def wait_for_cids(
        db: Any,
        object_id: str,
        required_parts: set[int],
        *,
        attempts: int = 10,
        interval_sec: float = 0.5,
    ) -> list[dict]:
        """Wait briefly for parts to gain concrete CIDs; returns chunk dicts when ready or empty list."""
        for _ in range(attempts):
            try:
                rows = await db.fetch(
                    """
                    SELECT p.part_number, c.cid, p.size_bytes
                    FROM parts p
                    JOIN cids c ON p.cid_id = c.id
                    WHERE p.object_id = $1
                    ORDER BY p.part_number
                    """,
                    object_id,
                )

                def _valid(row: Any) -> bool:
                    try:
                        cid = row[1]
                        return isinstance(cid, str) and cid.strip().lower() not in {"", "none", "pending"}
                    except Exception:
                        return False

                chunks = [
                    {"part_number": int(r[0]), "cid": str(r[1]), "size_bytes": int(r[2] or 0)}
                    for r in rows
                    if int(r[0]) in required_parts and _valid(r)
                ]
                avail = {c["part_number"] for c in chunks}
                if required_parts.issubset(avail):
                    return chunks
            except Exception:
                pass
            await asyncio.sleep(interval_sec)
        return []
