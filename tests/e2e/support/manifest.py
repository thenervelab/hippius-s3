import os
import time
from typing import Any
from typing import Optional

import psycopg
import requests


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


def wait_for_object_cid_via_head(
    boto3_client: Any, bucket: str, key: str, *, timeout_sec: float = 30.0, interval: float = 0.5
) -> Optional[str]:
    """Poll HEAD until x-amz-ipfs-cid is set (not 'pending'), return the CID or None on timeout."""
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        try:
            head = boto3_client.head_object(Bucket=bucket, Key=key)
            cid = head["ResponseMetadata"]["HTTPHeaders"].get("x-amz-ipfs-cid")
            if cid and str(cid).strip().lower() != "pending":
                return str(cid)
        except Exception:
            pass
        time.sleep(interval)
    return None
