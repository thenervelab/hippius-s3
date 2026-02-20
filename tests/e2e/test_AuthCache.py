"""E2E test for auth cache — verify that authenticated requests cache the auth response in Redis."""

import base64
import time
from typing import Any
from typing import Callable

import redis  # type: ignore[import-untyped]

from .conftest import is_real_aws


AUTH_CACHE_PREFIX = "hippius_auth:"


def _get_auth_cache_keys(r: redis.Redis) -> list[str]:
    """Return all auth cache keys currently in Redis."""
    keys = []
    for k in r.scan_iter(match=f"{AUTH_CACHE_PREFIX}*"):
        keys.append(k.decode() if isinstance(k, bytes) else k)
    return keys


def test_auth_cache_populated_on_authenticated_request(
    docker_services: Any,
    boto3_client: Any,
    test_seed_phrase: str,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    """An authenticated request should populate the auth cache in Redis."""
    if is_real_aws():
        return

    r = redis.Redis.from_url("redis://localhost:6379/0")

    access_key = base64.b64encode(test_seed_phrase.encode()).decode()
    cache_key = f"{AUTH_CACHE_PREFIX}{access_key}"

    # Clear any existing cache entry
    r.delete(cache_key)
    assert not r.exists(cache_key)

    bucket_name = unique_bucket_name("auth-cache")
    cleanup_buckets(bucket_name)
    boto3_client.create_bucket(Bucket=bucket_name)

    # First request — should call the Hippius API and populate the cache
    boto3_client.put_object(
        Bucket=bucket_name,
        Key="cache-test.txt",
        Body=b"auth cache test",
    )

    assert r.exists(cache_key), "Auth cache key should exist after first authenticated request"

    cached_value = r.get(cache_key)
    assert cached_value is not None
    assert b"valid" in cached_value  # TokenAuthResponse JSON should contain the 'valid' field

    ttl = r.ttl(cache_key)
    assert 0 < ttl <= 60, f"Auth cache TTL should be between 1-60 seconds, got {ttl}"


def test_auth_cache_reused_on_second_request(
    docker_services: Any,
    boto3_client: Any,
    test_seed_phrase: str,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    """A second authenticated request within the TTL window should reuse the cached auth."""
    if is_real_aws():
        return

    r = redis.Redis.from_url("redis://localhost:6379/0")

    access_key = base64.b64encode(test_seed_phrase.encode()).decode()
    cache_key = f"{AUTH_CACHE_PREFIX}{access_key}"

    # Clear any existing cache entry
    r.delete(cache_key)

    bucket_name = unique_bucket_name("auth-cache-reuse")
    cleanup_buckets(bucket_name)
    boto3_client.create_bucket(Bucket=bucket_name)

    # First request — populates cache
    boto3_client.put_object(
        Bucket=bucket_name,
        Key="first.txt",
        Body=b"first request",
    )

    assert r.exists(cache_key)
    ttl_after_first = r.ttl(cache_key)
    first_value = r.get(cache_key)

    # Small delay to let TTL tick down visibly
    time.sleep(1)

    # Second request — should reuse cache (TTL gets refreshed on cache miss, stays same on hit)
    boto3_client.put_object(
        Bucket=bucket_name,
        Key="second.txt",
        Body=b"second request",
    )

    assert r.exists(cache_key)
    second_value = r.get(cache_key)

    # The cached value should be identical (same auth response)
    assert first_value == second_value, "Cached auth response should be the same across requests"

    # TTL should have decreased (cache was hit, not refreshed)
    ttl_after_second = r.ttl(cache_key)
    assert ttl_after_second < ttl_after_first, "TTL should decrease on cache hit (not refreshed)"


def test_auth_cache_expires_after_ttl(
    docker_services: Any,
    boto3_client: Any,
    test_seed_phrase: str,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    """Auth cache entry should expire after TTL, and be repopulated on next request."""
    if is_real_aws():
        return

    r = redis.Redis.from_url("redis://localhost:6379/0")

    access_key = base64.b64encode(test_seed_phrase.encode()).decode()
    cache_key = f"{AUTH_CACHE_PREFIX}{access_key}"

    # Clear and set a short-lived entry to simulate expiry
    r.delete(cache_key)

    bucket_name = unique_bucket_name("auth-cache-expiry")
    cleanup_buckets(bucket_name)
    boto3_client.create_bucket(Bucket=bucket_name)

    # First request — populates cache
    boto3_client.put_object(
        Bucket=bucket_name,
        Key="expire-test.txt",
        Body=b"expiry test",
    )

    assert r.exists(cache_key)

    # Force expire the cache entry
    r.delete(cache_key)
    assert not r.exists(cache_key)

    # Next request should re-populate the cache
    boto3_client.put_object(
        Bucket=bucket_name,
        Key="expire-test-2.txt",
        Body=b"expiry test 2",
    )

    assert r.exists(cache_key), "Auth cache should be repopulated after expiry"
    ttl = r.ttl(cache_key)
    assert 0 < ttl <= 60, f"Freshly populated auth cache should have full TTL, got {ttl}"
