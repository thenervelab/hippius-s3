"""Fixtures for the live-staging drain e2e suite.

These tests run against the real staging gateway (``https://s3-staging.hippius.com``)
with real credentials — no docker-compose, no mocks. They assert the *outcome* of
the SSD->CephFS drain (an uploaded object becomes durable and readable), so they are
the acceptance test for the hippius-drain service regardless of its internals.

The suite is GATED: it stays dormant until ``HIPPIUS_DRAIN_LIVE=1`` is set, because
until the drain is deployed AND speaks the api's on-disk layout, staging PUTs do not
complete (they 502 / never drain). See README.md for the deploy blocker.
"""

import os
import uuid
import warnings
from collections.abc import Iterator

import boto3  # type: ignore[import-untyped]
import pytest
import urllib3
from botocore.config import Config  # type: ignore[import-untyped]


DEFAULT_ENDPOINT = "https://s3-staging.hippius.com"


def drain_live() -> bool:
    """Whether the drain is deployed and functional in staging.

    Opt-in via ``HIPPIUS_DRAIN_LIVE=1`` so the suite flips from skipped to enforced
    the moment the drain actually drains — without that, these assertions would fail
    on the known "uploads stage locally and never drain" state, not on a regression.
    """
    return os.getenv("HIPPIUS_DRAIN_LIVE") == "1"


def pytest_collection_modifyitems(config, items):  # type: ignore[no-untyped-def]
    """Skip the whole suite until the drain is live, with an actionable reason."""
    if drain_live():
        return
    skip = pytest.mark.skip(
        reason="drain not live in staging (set HIPPIUS_DRAIN_LIVE=1 once hippius-drain is deployed and "
        "drains the api's object_id/vN/part_N layout — see tests/staging/README.md)"
    )
    for item in items:
        item.add_marker(skip)


@pytest.fixture(scope="session")
def endpoint_url() -> str:
    """The staging S3 endpoint (override with ``HIPPIUS_S3_ENDPOINT``)."""
    return os.getenv("HIPPIUS_S3_ENDPOINT", DEFAULT_ENDPOINT).rstrip("/")


def _tls_verify():  # type: ignore[no-untyped-def]
    """Resolve TLS verification — secure by default.

    Returns a CA-bundle path (``HIPPIUS_S3_CA_BUNDLE``) when given, else ``True``
    (full verification). Verification is only disabled when ``HIPPIUS_S3_INSECURE=1``
    is set explicitly — the documented workaround for the staging gateway cert whose
    SAN is ``us-east-1.hippius.com`` rather than the gateway hostname. Prefer adding
    the staging CA to the trust store or pointing at it via the bundle env over the
    insecure flag; the flag exists so the suite is runnable today, not as a default.
    """
    ca_bundle = os.getenv("HIPPIUS_S3_CA_BUNDLE")
    if ca_bundle:
        return ca_bundle
    if os.getenv("HIPPIUS_S3_INSECURE") == "1":
        warnings.simplefilter("ignore", urllib3.exceptions.InsecureRequestWarning)
        return False
    return True


@pytest.fixture(scope="session")
def s3(endpoint_url: str):  # type: ignore[no-untyped-def]
    """A boto3 S3 client pointed at staging.

    Credentials come from the standard ``AWS_*`` environment variables; TLS
    verification is resolved by [`_tls_verify`] (secure unless explicitly opted out).
    """
    if not os.getenv("AWS_ACCESS_KEY_ID") or not os.getenv("AWS_SECRET_ACCESS_KEY"):
        pytest.skip("AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY must be set for the staging suite")
    config = Config(
        signature_version="s3v4",
        s3={"addressing_style": "path"},
        retries={"max_attempts": 2, "mode": "standard"},
    )
    return boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        region_name=os.getenv("AWS_DEFAULT_REGION", "decentralized"),
        config=config,
        verify=_tls_verify(),
    )


@pytest.fixture
def bucket(s3) -> Iterator[str]:  # type: ignore[no-untyped-def]
    """A unique, auto-cleaned bucket for one test.

    The name carries a UUID because bucket names are globally unique in this stack —
    a fixed name collides with leftovers from a prior run (observed as
    ``BucketAlreadyExists``). Cleanup is best-effort so a mid-test failure never
    blocks the next run; leftover empty buckets are harmless.
    """
    name = f"drain-e2e-{uuid.uuid4().hex[:16]}"
    s3.create_bucket(Bucket=name)
    try:
        yield name
    finally:
        _empty_and_delete(s3, name)


def _empty_and_delete(s3, name: str) -> None:  # type: ignore[no-untyped-def]
    """Delete every object then the bucket; swallow errors (best-effort teardown)."""
    try:
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=name):
            keys = [{"Key": obj["Key"]} for obj in page.get("Contents", [])]
            if keys:
                s3.delete_objects(Bucket=name, Delete={"Objects": keys})
        s3.delete_bucket(Bucket=name)
    except Exception:  # noqa: BLE001 — teardown must never mask a test failure
        pass
