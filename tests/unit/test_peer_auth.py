"""`is_authorized_peer_fetch` is the single predicate the merged app's middlewares use
to exempt peer chunk fetches from the S3 pipeline. These pin its fail-closed shape; the
end-to-end handshake (fetcher against the mounted route) lives in test_peer_handshake.py.
"""

from typing import Any


class TestPeerFetchExemption:
    """The merged app exempts /internal/parts from the S3 pipeline ONLY for requests
    presenting the valid peer secret — fail-closed in every other case."""

    def _request(self, path: str, secret: str | None, method: str = "GET") -> Any:
        from starlette.requests import Request as StarletteRequest

        headers = []
        if secret is not None:
            headers.append((b"x-hippius-peer-auth", secret.encode()))
        scope = {
            "type": "http",
            "method": method,
            "path": path,
            "raw_path": path.encode(),
            "query_string": b"",
            "headers": headers,
            "client": ("10.0.0.9", 4242),
        }
        return StarletteRequest(scope)

    def test_valid_secret_on_internal_path_is_exempt(self, monkeypatch: Any) -> None:
        from hippius_s3 import config as config_mod
        from hippius_s3.peer_auth import is_authorized_peer_fetch

        secret = "ab" * 32
        monkeypatch.setattr(config_mod.get_config(), "internal_peer_secret", secret)
        monkeypatch.setattr(config_mod.get_config(), "peer_serve_enabled", True)
        assert is_authorized_peer_fetch(self._request("/internal/parts/obj/1/1/chunks/0", secret)) is True

    def test_wrong_secret_and_wrong_path_fail_closed(self, monkeypatch: Any) -> None:
        from hippius_s3 import config as config_mod
        from hippius_s3.peer_auth import is_authorized_peer_fetch

        secret = "ab" * 32
        monkeypatch.setattr(config_mod.get_config(), "internal_peer_secret", secret)
        monkeypatch.setattr(config_mod.get_config(), "peer_serve_enabled", True)
        assert is_authorized_peer_fetch(self._request("/internal/parts/obj/1/1/chunks/0", "cd" * 32)) is False
        assert is_authorized_peer_fetch(self._request("/internal/parts/obj/1/1/chunks/0", None)) is False
        assert is_authorized_peer_fetch(self._request("/some-bucket/key", secret)) is False
        # dot-segment smuggling into the internal prefix is judged on the routing view
        assert is_authorized_peer_fetch(self._request("/bucket/../internal/parts/x/1/1/chunks/0", secret)) is True

    def test_non_get_and_serve_disabled_fail_closed(self, monkeypatch: Any) -> None:
        """The exemption is scoped to what the peer tier actually does: GET chunk reads,
        on pods that opted into serving. A valid secret must not unlock write methods or
        pods where the route is not mounted."""
        from hippius_s3 import config as config_mod
        from hippius_s3.peer_auth import is_authorized_peer_fetch

        secret = "ab" * 32
        monkeypatch.setattr(config_mod.get_config(), "internal_peer_secret", secret)
        monkeypatch.setattr(config_mod.get_config(), "peer_serve_enabled", True)
        for method in ("PUT", "POST", "DELETE", "HEAD"):
            assert is_authorized_peer_fetch(self._request("/internal/parts/obj/1/1/chunks/0", secret, method)) is False

        monkeypatch.setattr(config_mod.get_config(), "peer_serve_enabled", False)
        assert is_authorized_peer_fetch(self._request("/internal/parts/obj/1/1/chunks/0", secret)) is False

    def test_no_configured_secret_never_authorizes(self, monkeypatch: Any) -> None:
        from hippius_s3 import config as config_mod
        from hippius_s3.peer_auth import is_authorized_peer_fetch

        monkeypatch.setattr(config_mod.get_config(), "internal_peer_secret", "")
        assert is_authorized_peer_fetch(self._request("/internal/parts/obj/1/1/chunks/0", "")) is False
