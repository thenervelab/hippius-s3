"""X-Hippius-Node: the serving node on every response, so a slow or failed GET can be tied to
the pod that served it without a log search. Present iff NODE_NAME is set, on streaming and
error responses alike — the same coverage the ray id header has."""

from __future__ import annotations

import pytest
from fastapi import FastAPI
from fastapi import Response
from fastapi.responses import PlainTextResponse
from fastapi.responses import StreamingResponse
from starlette.testclient import TestClient

from hippius_s3.gateway.middlewares.ray_id import ray_id_middleware


def _app() -> FastAPI:
    app = FastAPI()
    app.middleware("http")(ray_id_middleware)

    @app.get("/plain")
    async def _plain() -> Response:
        return PlainTextResponse("ok")

    @app.get("/stream")
    async def _stream() -> Response:
        return StreamingResponse(iter([b"a", b"b"]))

    @app.get("/error")
    async def _error() -> Response:
        return Response(status_code=503)

    return app


@pytest.mark.parametrize(
    ("path", "status"),
    [("/plain", 200), ("/stream", 200), ("/error", 503)],
)
def test_the_node_header_is_set_when_node_name_is(monkeypatch, path: str, status: int) -> None:
    monkeypatch.setenv("NODE_NAME", "k8s-v3-node2")

    response = TestClient(_app()).get(path)

    assert response.status_code == status
    assert response.headers["X-Hippius-Node"] == "k8s-v3-node2"
    assert response.headers["X-Hippius-Ray-ID"], "the ray id header is still there"


@pytest.mark.parametrize("path", ["/plain", "/stream", "/error"])
def test_the_node_header_is_absent_without_a_node_name(monkeypatch, path: str) -> None:
    monkeypatch.delenv("NODE_NAME", raising=False)

    response = TestClient(_app()).get(path)

    assert "X-Hippius-Node" not in response.headers
    assert response.headers["X-Hippius-Ray-ID"]


def test_an_empty_node_name_counts_as_unset(monkeypatch) -> None:
    monkeypatch.setenv("NODE_NAME", "")

    response = TestClient(_app()).get("/plain")

    assert "X-Hippius-Node" not in response.headers
