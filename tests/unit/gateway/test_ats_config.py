"""Tests for ATS-related config fields (folded into hippius_s3.config.Config)."""

import pytest

from hippius_s3 import config as config_mod


@pytest.fixture(autouse=True)  # type: ignore[misc]
def _reset_config() -> None:
    config_mod.reset_config()
    yield
    config_mod.reset_config()


def test_ats_cache_endpoints_defaults_to_empty(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("ATS_CACHE_ENDPOINT", raising=False)
    assert config_mod.get_config().ats_cache_endpoints == []


def test_ats_cache_endpoints_single_value(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ATS_CACHE_ENDPOINT", "http://192.168.1.155:8080")
    assert config_mod.get_config().ats_cache_endpoints == ["http://192.168.1.155:8080"]


def test_ats_cache_endpoints_csv(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(
        "ATS_CACHE_ENDPOINT",
        "http://ats-0.local:8080,http://ats-1.local:8080,http://ats-2.local:8080",
    )
    assert config_mod.get_config().ats_cache_endpoints == [
        "http://ats-0.local:8080",
        "http://ats-1.local:8080",
        "http://ats-2.local:8080",
    ]


def test_ats_cache_endpoints_trims_whitespace_and_empties(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ATS_CACHE_ENDPOINT", " http://a:8080 , , http://b:8080 ,")
    assert config_mod.get_config().ats_cache_endpoints == ["http://a:8080", "http://b:8080"]
