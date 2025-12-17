from __future__ import annotations

import asyncio
import logging
from typing import Any


logger = logging.getLogger(__name__)


class ManifestService:
    @staticmethod
    async def build_initial_download_chunks(db: Any, object_info: dict) -> list[dict]:
        """
        Build manifest from parts table only.
        All objects (simple, append, MPU) are represented by parts rows.
        No object-level CID fallback used for streaming.
        """
        try:
            logger.debug(f"MANIFEST build_initial_download_chunks called for object_id={object_info['object_id']}")
            # Safely coerce optional object_version to int or None for SQL param $2
            ov_raw = object_info.get("object_version")
            try:
                ov_param = int(ov_raw)  # type: ignore[arg-type]
            except Exception:
                ov_param = None

            # CIDs are optional: v4+ deployments may be CID-less (deterministic chunk addressing),
            # but we still want to surface CIDs when present so IPFS-backed reads remain possible.
            rows = await db.fetch(
                """
                SELECT p.part_number,
                       COALESCE(c.cid, p.ipfs_cid) AS cid,
                       p.size_bytes::bigint AS size_bytes
                FROM objects o
                JOIN parts p
                  ON p.object_id = o.object_id
                 AND p.object_version = COALESCE($2, o.current_object_version)
                LEFT JOIN cids c ON p.cid_id = c.id
                WHERE o.object_id = $1
                ORDER BY p.part_number
                """,
                object_info["object_id"],
                ov_param,
            )
            # Avoid exploding logs for large multipart objects.
            preview = [(r[0], r[1], r[2]) for r in rows[:25]]
            logger.debug(
                "MANIFEST found %s parts rows (preview=%s%s)",
                len(rows),
                preview,
                "" if len(rows) <= 25 else "…",
            )

            manifest: list[dict] = []
            for r in rows:
                pn = int(r[0])
                cid_raw = r[1]
                cid: str | None = None
                if cid_raw is not None:
                    cid_str = cid_raw if isinstance(cid_raw, str) else str(cid_raw)
                    cid_str = cid_str.strip()
                    if cid_str and cid_str.lower() not in {"", "none", "pending"}:
                        cid = cid_str

                size = int(r[2] or 0)
                manifest.append({"part_number": pn, "cid": cid, "size_bytes": size})

            logger.debug(f"MANIFEST built manifest: {manifest}")
            return manifest

        except Exception:
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
                    SELECT p.part_number, COALESCE(c.cid, p.ipfs_cid) AS cid, p.size_bytes
                    FROM objects o
                    JOIN parts p
                      ON p.object_id = o.object_id
                     AND p.object_version = o.current_object_version
                    LEFT JOIN cids c ON p.cid_id = c.id
                    WHERE o.object_id = $1
                    ORDER BY p.part_number
                    """,
                    object_id,
                )

                def _valid(row: Any) -> bool:
                    try:
                        cid = row[1]
                        cid_str = cid if isinstance(cid, str) else str(cid or "")
                        return cid_str.strip().lower() not in {"", "none", "pending"}
                    except Exception:
                        return False

                chunks = [
                    {
                        "part_number": int(r[0]),
                        "cid": (r[1] if isinstance(r[1], str) else str(r[1] or "")),
                        "size_bytes": int(r[2] or 0),
                    }
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
