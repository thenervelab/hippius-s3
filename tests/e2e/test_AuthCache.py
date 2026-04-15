"""E2E tests for gateway auth cache (SigV4 access key flow).

These tests verify that cached_auth() in the gateway correctly caches
TokenAuthResponse objects in Redis when using hip_* access key authentication.
"""

import datetime
import time

import httpx
import pytest
import redis


GATEWAY_URL = "http://localhost:8080"
REDIS_URL = "redis://localhost:6379/0"
AUTH_CACHE_PREFIX = "hippius_auth:"


@pytest.fixture
def redis_client():
    """Direct Redis connection to verify cache state."""
    r = redis.Redis.from_url(REDIS_URL, decode_responses=True)
    yield r
    r.close()


def _sigv4_headers(access_key: str) -> dict[str, str]:
    """Build minimal SigV4 headers that trigger cached_auth() before signature validation."""
    amz_date = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    date_scope = amz_date[:8]
    return {
        "Authorization": (
            f"AWS4-HMAC-SHA256 Credential={access_key}/{date_scope}/us-east-1/s3/aws4_request, "
            "SignedHeaders=host;x-amz-date, Signature=0000000000000000000000000000000000000000000000000000000000000000"
        ),
        "x-amz-date": amz_date,
        "x-amz-content-sha256": "UNSIGNED-PAYLOAD",
    }


@pytest.mark.e2e
@pytest.mark.hippius_cache
def test_auth_cache_populated_on_request(redis_client: redis.Redis) -> None:
    """Sending a SigV4 request populates hippius_auth:{key} in Redis."""
    key = f"hip_cache_pop_{int(time.time())}"
    cache_key = f"{AUTH_CACHE_PREFIX}{key}"

    # Ensure clean state
    redis_client.delete(cache_key)

    # Send request — response status is irrelevant; cache is populated in auth step
    with httpx.Client(timeout=10) as client:
        client.get(f"{GATEWAY_URL}/test-bucket/test-key", headers=_sigv4_headers(key))

    cached = redis_client.get(cache_key)
    assert cached is not None, f"Expected cache key {cache_key} to exist in Redis"

    ttl = redis_client.ttl(cache_key)
    assert 0 < ttl <= 60, f"Expected TTL between 1-60s, got {ttl}"


@pytest.mark.e2e
@pytest.mark.hippius_cache
def test_auth_cache_reused_on_second_request(redis_client: redis.Redis) -> None:
    """Two consecutive SigV4 requests reuse the same cached value."""
    key = f"hip_cache_reuse_{int(time.time())}"
    cache_key = f"{AUTH_CACHE_PREFIX}{key}"

    redis_client.delete(cache_key)

    with httpx.Client(timeout=10) as client:
        client.get(f"{GATEWAY_URL}/test-bucket/test-key", headers=_sigv4_headers(key))

    first_value = redis_client.get(cache_key)
    first_ttl = redis_client.ttl(cache_key)
    assert first_value is not None

    time.sleep(1)

    with httpx.Client(timeout=10) as client:
        client.get(f"{GATEWAY_URL}/test-bucket/test-key", headers=_sigv4_headers(key))

    second_value = redis_client.get(cache_key)
    second_ttl = redis_client.ttl(cache_key)

    assert second_value == first_value, "Cached value should be identical across requests"
    assert second_ttl < first_ttl, "TTL should decrease between requests (cache reused, not refreshed)"


@pytest.mark.e2e
@pytest.mark.hippius_cache
def test_auth_cache_expires_and_repopulates(redis_client: redis.Redis) -> None:
    """After manual deletion, the cache key is repopulated on next request."""
    key = f"hip_cache_expire_{int(time.time())}"
    cache_key = f"{AUTH_CACHE_PREFIX}{key}"

    redis_client.delete(cache_key)

    with httpx.Client(timeout=10) as client:
        client.get(f"{GATEWAY_URL}/test-bucket/test-key", headers=_sigv4_headers(key))

    assert redis_client.exists(cache_key), "Cache should be populated after first request"

    # Simulate expiry by deleting
    redis_client.delete(cache_key)
    assert not redis_client.exists(cache_key), "Cache should be gone after deletion"

    # Re-request should repopulate
    with httpx.Client(timeout=10) as client:
        client.get(f"{GATEWAY_URL}/test-bucket/test-key", headers=_sigv4_headers(key))

    assert redis_client.exists(cache_key), "Cache should be repopulated after re-request"
    ttl = redis_client.ttl(cache_key)
    assert 0 < ttl <= 60, f"Fresh TTL expected, got {ttl}"
