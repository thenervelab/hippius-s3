from __future__ import annotations

from hippius_s3.main import factory
from hippius_s3.reserved_bucket_names import RESERVED_BUCKET_SEGMENTS
from tests.unit.routing_helpers import leaf_routes


def _first_segment(path: str) -> str:
    return path.strip("/").split("/", 1)[0]


def _segments_shadowing_the_s3_catch_all() -> set[str]:
    """First static path segments the api resolves before it ever reaches `/{bucket_name}`.

    Starlette matches routes in registration order, so only what is registered ahead of the S3
    catch-all can shadow it. `/static` is mounted after and is therefore shadowed BY S3 rather
    than shadowing it — walking the real route list is what keeps that distinction honest.
    """
    segments: set[str] = set()
    for route in leaf_routes(factory()):
        path = getattr(route, "path", "")
        segment = _first_segment(path)
        if not segment:
            continue
        if segment.startswith("{"):
            return segments
        segments.add(segment)
    raise AssertionError("no `/{bucket_name}` catch-all in the api route table — this test is blind")


def test_every_api_segment_shadowing_the_s3_catch_all_is_a_reserved_bucket_name() -> None:
    """A bucket on one of these names is unreachable for any key the shadowing route also matches,
    yet it still holds the globally-unique name forever — the prod incident this set exists for.

    The gateway forwards every path it does not serve itself, so the api's routing table strands
    bucket names exactly like the gateway's does.
    """
    shadowing = _segments_shadowing_the_s3_catch_all()

    # Proves the walk actually reached real routes rather than breaking on the first entry.
    assert {"health", "user", "docs"} <= shadowing

    assert shadowing <= RESERVED_BUCKET_SEGMENTS, (
        f"api routes shadow bucket names that are not reserved: {sorted(shadowing - RESERVED_BUCKET_SEGMENTS)}"
    )
