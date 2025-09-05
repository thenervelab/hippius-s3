"""E2E test for ListObjects (GET /{bucket})."""

from typing import Any
from typing import Callable

import pytest


def test_list_objects_with_prefix(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    bucket_name = unique_bucket_name("list-objects")
    cleanup_buckets(bucket_name)

    boto3_client.create_bucket(Bucket=bucket_name)

    items = {
        "docs/readme.txt": b"a",
        "docs/guide.txt": b"b",
        "images/logo.png": b"c",
    }
    for key, data in items.items():
        boto3_client.put_object(Bucket=bucket_name, Key=key, Body=data, ContentType="application/octet-stream")

    # Full list
    full = boto3_client.list_objects_v2(Bucket=bucket_name)
    keys = {o["Key"] for o in full.get("Contents", [])}
    assert keys == set(items.keys())

    # Prefix list
    pref = boto3_client.list_objects_v2(Bucket=bucket_name, Prefix="docs/")
    pref_keys = {o["Key"] for o in pref.get("Contents", [])}
    assert pref_keys == {"docs/readme.txt", "docs/guide.txt"}

