"""Tests for ATS-related config fields."""

import pytest

from gateway import config as gateway_config


@pytest.fixture(autouse=True)  # type: ignore[misc]
def _reset_config() -> None:
    gateway_config._config = None
    yield
    gateway_config._config = None


def test_ats_cache_endpoint_defaults_to_empty(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("ATS_CACHE_ENDPOINT", raising=False)
    cfg = gateway_config.GatewayConfig()
    assert cfg.ats_cache_endpoint == ""


def test_ats_cache_endpoint_loaded_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ATS_CACHE_ENDPOINT", "http://192.168.1.155:8080")
    cfg = gateway_config.GatewayConfig()
    assert cfg.ats_cache_endpoint == "http://192.168.1.155:8080"
