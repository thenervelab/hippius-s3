"""E2E test for the v5_missing_envelope_metadata race condition fix.

Validates that GETs succeed even when a concurrent overwrite creates a new
object version with a temporarily NULL envelope (kek_id/wrapped_dek).

The fix has two layers:
1. Write path: envelope is written immediately after version reservation (before chunks)
2. Read path: if envelope is still missing, falls back to the previous version

This test verifies both layers by:
- Uploading an object, then overwriting it so there are >=2 versions
- Artificially NULLing the envelope on the current version (simulating the race window)
- Verifying GET still succeeds (serves previous version via fallback)
- Verifying GET returns correct data after the envelope is restored
"""
from __future__ import annotations

import hashlib
import secrets
from typing import Any
from typing import Callable

import psycopg
import pytest

from tests.e2e.support.cache import clear_object_cache
from tests.e2e.support.cache import get_object_id_and_version


DB_DSN = "postgresql://postgres:postgres@localhost:5432/hippius"


def _null_envelope(object_id: str, object_version: int) -> None:
    """Set kek_id and wrapped_dek to NULL on a specific version, simulating a mid-write race."""
    with psycopg.connect(DB_DSN) as conn, conn.cursor() as cur:
        cur.execute(
            """
            UPDATE object_versions
               SET kek_id = NULL,
                   wrapped_dek = NULL
             WHERE object_id = %s AND object_version = %s
            """,
            (object_id, object_version),
        )
        conn.commit()


def _restore_envelope(object_id: str, object_version: int, kek_id: str, wrapped_dek: bytes) -> None:
    """Restore the envelope on a specific version."""
    with psycopg.connect(DB_DSN) as conn, conn.cursor() as cur:
        cur.execute(
            """
            UPDATE object_versions
               SET kek_id = %s,
                   wrapped_dek = %s
             WHERE object_id = %s AND object_version = %s
            """,
            (kek_id, wrapped_dek, object_id, object_version),
        )
        conn.commit()


def _get_envelope(object_id: str, object_version: int) -> tuple[str | None, bytes | None]:
    """Read the current envelope for a specific version."""
    with psycopg.connect(DB_DSN) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT kek_id, wrapped_dek
            FROM object_versions
            WHERE object_id = %s AND object_version = %s
            """,
            (object_id, object_version),
        )
        row = cur.fetchone()
        if not row:
            return None, None
        return (str(row[0]) if row[0] else None, bytes(row[1]) if row[1] else None)


@pytest.mark.local
def test_get_falls_back_to_previous_version_when_envelope_missing(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    """GET returns data from the previous version when current version has NULL envelope."""
    bucket = unique_bucket_name("envelope-race")
    cleanup_buckets(bucket)
    boto3_client.create_bucket(Bucket=bucket)
    key = "race-test.bin"

    # Upload v1
    body_v1 = secrets.token_bytes(64 * 1024)
    md5_v1 = hashlib.md5(body_v1).hexdigest()
    boto3_client.put_object(Bucket=bucket, Key=key, Body=body_v1)

    # Overwrite with v2 so we have a previous version to fall back to
    body_v2 = secrets.token_bytes(64 * 1024)
    md5_v2 = hashlib.md5(body_v2).hexdigest()
    boto3_client.put_object(Bucket=bucket, Key=key, Body=body_v2)

    # Verify v2 is served correctly before we break it
    resp = boto3_client.get_object(Bucket=bucket, Key=key)
    data = resp["Body"].read()
    assert hashlib.md5(data).hexdigest() == md5_v2

    # Now simulate the race: NULL out the envelope on the current version
    object_id, current_version = get_object_id_and_version(bucket, key)
    saved_kek_id, saved_wrapped_dek = _get_envelope(object_id, current_version)
    assert saved_kek_id is not None, "Envelope should exist before we break it"

    # Clear cache so the next GET must go through the reader path (not cache)
    clear_object_cache(object_id)

    _null_envelope(object_id, current_version)

    # GET should still succeed — falling back to previous version (v1 data)
    resp = boto3_client.get_object(Bucket=bucket, Key=key)
    data = resp["Body"].read()
    fallback_md5 = hashlib.md5(data).hexdigest()
    assert fallback_md5 == md5_v1, (
        f"Expected fallback to v{current_version - 1} (md5={md5_v1[:8]}), "
        f"got md5={fallback_md5[:8]}"
    )

    # Restore the envelope so cleanup can delete the object
    _restore_envelope(object_id, current_version, saved_kek_id, saved_wrapped_dek)


@pytest.mark.local
def test_get_fails_on_v1_with_missing_envelope(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    """Version 1 with missing envelope has no fallback — GET should return 500."""
    bucket = unique_bucket_name("envelope-v1")
    cleanup_buckets(bucket)
    boto3_client.create_bucket(Bucket=bucket)
    key = "v1-only.bin"

    body = secrets.token_bytes(32 * 1024)
    boto3_client.put_object(Bucket=bucket, Key=key, Body=body)

    object_id, current_version = get_object_id_and_version(bucket, key)
    assert current_version == 1
    saved_kek_id, saved_wrapped_dek = _get_envelope(object_id, current_version)

    clear_object_cache(object_id)
    _null_envelope(object_id, current_version)

    from botocore.exceptions import ClientError

    with pytest.raises(ClientError) as exc_info:
        boto3_client.get_object(Bucket=bucket, Key=key)
    assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 500

    # Restore so cleanup can delete the object
    _restore_envelope(object_id, current_version, saved_kek_id, saved_wrapped_dek)


@pytest.mark.local
def test_rapid_overwrite_get_never_500s(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    """Rapid PUT overwrites interleaved with GETs should never produce 500s.

    This tests the write-path fix: envelope is written before chunks, so
    there should be no window where a GET sees NULL envelope.
    """
    bucket = unique_bucket_name("rapid-overwrite")
    cleanup_buckets(bucket)
    boto3_client.create_bucket(Bucket=bucket)
    key = "dashboard.json"

    bodies = [secrets.token_bytes(16 * 1024) for _ in range(10)]

    # Seed v1
    boto3_client.put_object(Bucket=bucket, Key=key, Body=bodies[0])

    errors = []
    for i in range(1, len(bodies)):
        # Overwrite
        boto3_client.put_object(Bucket=bucket, Key=key, Body=bodies[i])
        # Immediately GET — if the race fix works, this should never 500
        try:
            resp = boto3_client.get_object(Bucket=bucket, Key=key)
            data = resp["Body"].read()
            # Data should match one of the versions (current or previous)
            data_md5 = hashlib.md5(data).hexdigest()
            valid_md5s = {hashlib.md5(b).hexdigest() for b in bodies[: i + 1]}
            assert data_md5 in valid_md5s, f"Got unexpected data md5={data_md5[:8]} on iteration {i}"
        except Exception as e:
            errors.append(f"Iteration {i}: {e}")

    assert not errors, f"Got {len(errors)} errors during rapid overwrite:\n" + "\n".join(errors)
