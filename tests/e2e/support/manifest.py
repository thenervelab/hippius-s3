import os
import time
from typing import Any
from typing import Optional

import psycopg


def _get_db_url() -> str:
    return os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/hippius")


def get_object_row(bucket: str, key: str) -> Optional[dict[str, Any]]:
    with psycopg.connect(_get_db_url()) as conn, conn.cursor() as cur:
        cur.execute(
            """
                SELECT ov.ipfs_cid, ov.append_version, ov.size_bytes
                FROM objects o
                JOIN object_versions ov
                  ON ov.object_id = o.object_id
                 AND ov.object_version = o.current_object_version
                JOIN buckets b ON b.bucket_id = o.bucket_id
                WHERE b.bucket_name = %s AND o.object_key = %s
                  AND o.deleted_at IS NULL
                LIMIT 1
                """,
            (bucket, key),
        )
        row = cur.fetchone()
        if not row:
            return None
        return {"ipfs_cid": row[0], "append_version": int(row[1] or 0), "size_bytes": int(row[2] or 0)}


def wait_for_object_cid(bucket: str, key: str, timeout_sec: float = 30.0, interval: float = 1.0) -> Optional[str]:
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        row = get_object_row(bucket, key)
        if row and row.get("ipfs_cid"):
            cid_val = str(row["ipfs_cid"])
            if cid_val.strip().lower() not in {"", "none", "pending"}:
                return cid_val
        time.sleep(interval)
    return None


# Backwards-compat aliases
get_manifest_row = get_object_row
wait_for_manifest_cid = wait_for_object_cid
