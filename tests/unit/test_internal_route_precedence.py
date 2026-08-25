"""Which route the api app matches for `/internal/parts/...`, and whether it exists at all.

Both directions are load-bearing and neither was covered.

Starlette matches routes in registration order, and `/internal/parts/{a}/{b}/{c}/chunks/{d}`
overlaps the S3 catch-all `/{bucket}/{key:path}`. If the S3 router were ever registered
first, a peer fetch would be answered as a GetObject on a bucket named `internal` — the peer
tier would go dark with no error anywhere. If the internal route wins, as it must, then the
name `internal` is unreachable as a bucket, which is the other half of why this ordering
needs a test rather than a comment.

The reverse direction matters more since the auth fix: with serving off, the route must be
ABSENT, not mounted-and-unauthenticated. "Mounted but the check is disabled" is exactly the
state this change removes, so a test that only checked for 404 would pass in it.
"""

from __future__ import annotations

from fastapi import FastAPI
from starlette.routing import Match

from hippius_s3.config import reset_config
from tests.unit.routing_helpers import leaf_routes
from tests.unit.routing_helpers import route_names


OBJ = "466916c0-d61b-4518-b81b-9576b574270a"
INTERNAL_PATH = f"/internal/parts/{OBJ}/1/1/chunks/0"
# Must be a WELL-FORMED secret (64 hex), not a readable placeholder: `factory()` validates it at
# boot, so a stand-in like "a-configured-secret" fails startup and every test here dies before it
# can assert anything about route ordering.
SECRET = "a" * 64


def _api_app(monkeypatch, *, serve_enabled: str, secret: str) -> FastAPI:
    """The real api app, built the way uvicorn builds it.

    Built through `factory()` rather than by re-including the routers on a bare app, because
    the property under test IS the registration order in `main.py` — a test that re-declared
    that order would assert its own copy of it and pass however main.py changed.
    """
    monkeypatch.setenv("HIPPIUS_PEER_SERVE_ENABLED", serve_enabled)
    monkeypatch.setenv("HIPPIUS_INTERNAL_PEER_SECRET", secret)
    reset_config()

    from hippius_s3.main import factory

    return factory()


def _matching_routes(app: FastAPI, path: str) -> list[str]:
    """Names of every route that would accept `GET path`, in the order Starlette tries them."""
    scope = {
        "type": "http",
        "method": "GET",
        "path": path,
        "root_path": "",
        "headers": [],
        "query_string": b"",
    }
    return [route.name for route in leaf_routes(app) if route.matches(scope)[0] == Match.FULL]


def test_the_internal_route_beats_the_s3_catch_all(monkeypatch) -> None:
    app = _api_app(monkeypatch, serve_enabled="true", secret=SECRET)

    matches = _matching_routes(app, INTERNAL_PATH)

    assert matches[0] == "get_local_chunk"
    assert len(matches) > 1, (
        "the S3 catch-all no longer matches this path, so the assertion above is vacuous — "
        "it proves route ORDER only while both routes compete for it"
    )


def test_an_ordinary_object_key_is_not_captured_by_the_internal_route(monkeypatch) -> None:
    """The internal route must be narrow: five fixed-shape segments, not a prefix grab."""
    app = _api_app(monkeypatch, serve_enabled="true", secret=SECRET)

    assert "get_local_chunk" not in _matching_routes(app, "/my-bucket/some/key.txt")


def test_the_route_is_absent_when_serving_is_off(monkeypatch) -> None:
    app = _api_app(monkeypatch, serve_enabled="false", secret=SECRET)

    assert "get_local_chunk" not in route_names(app)


def test_the_route_is_absent_when_no_secret_is_configured(monkeypatch) -> None:
    """Enabled with an empty secret is a misconfiguration, and it must fail closed.

    Mounting here would recreate the original defect exactly — an unauthenticated endpoint on
    a path the gateway proxies — so the flag alone is not enough to bring the route up.
    """
    app = _api_app(monkeypatch, serve_enabled="true", secret="")

    assert "get_local_chunk" not in route_names(app)
