import os
import time
from typing import Any
from typing import Optional

import psycopg
import requests


def _get_db_url() -> str:
    return os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/hippius")


def get_manifest_row(bucket: str, key: str) -> Optional[dict[str, Any]]:
    with psycopg.connect(_get_db_url()) as conn, conn.cursor() as cur:
        cur.execute(
            """
                SELECT o.manifest_cid, o.append_version, o.size_bytes
                FROM objects o
                JOIN buckets b ON b.bucket_id = o.bucket_id
                WHERE b.bucket_name = %s AND o.object_key = %s
                LIMIT 1
                """,
            (bucket, key),
        )
        row = cur.fetchone()
        if not row:
            return None
        return {"manifest_cid": row[0], "append_version": int(row[1] or 0), "size_bytes": int(row[2] or 0)}


def wait_for_manifest_cid(bucket: str, key: str, timeout_sec: float = 30.0, interval: float = 1.0) -> Optional[str]:
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        row = get_manifest_row(bucket, key)
        if row and row.get("manifest_cid"):
            return str(row["manifest_cid"])
        time.sleep(interval)
    return None


def fetch_ipfs_json(cid: str) -> dict[str, Any]:
    # Use IPFS API cat endpoint instead of gateway to avoid subdomain redirects in tests
    api_url = os.getenv("HIPPIUS_E2E_IPFS_API_URL", "http://localhost:5001")
    url = f"{api_url.rstrip('/')}/api/v0/cat"
    resp = requests.post(url, params={"arg": cid}, timeout=10)
    resp.raise_for_status()
    data = resp.json()
    if not isinstance(data, dict):
        raise ValueError(f"Expected JSON object, got {type(data)}")
    return data
