"""Every worker the e2e stack starts must actually be able to start.

`arion-unpinner` and `purger` crash-looped in e2e from the day they were added and nobody
noticed for months. Two things had to be true at once:

1. The base compose gives every service ``env_file: [.env.defaults, .env]``, and CI writes `.env`
   from `.env.example`. Neither file defines ENVIRONMENT, which `config.env("ENVIRONMENT")`
   requires with no default — so `get_config()` raises at import. `docker-compose.e2e.yml` fixes
   that by swapping `.env` for `.env.test-docker`, but only for the services it happens to list.
   The two that were never added got no test env and died on the first line of the worker.
2. The failure was invisible: their healthcheck was `exit 0`, and with ENABLE_WATCHFILES=true PID 1
   is watchfiles, which stays up after its python child dies. So the container stayed "Up",
   reported "Healthy", `docker compose up --wait` was satisfied, and the stack looked green with a
   dead worker inside it.

These tests pin both halves so the next worker added to the compose file cannot repeat it.
"""

from __future__ import annotations

import pathlib

import pytest
import yaml


REPO = pathlib.Path(__file__).resolve().parents[2]

# Services that are infrastructure or test doubles rather than our application code. Everything
# else in the base compose file runs hippius_s3 and therefore needs a loadable config.
_NON_APP_SERVICES = {"base", "db", "toxiproxy", "drain-agent", "drain-allocator"}


def _services(*names: str) -> dict:
    merged: dict = {}
    for name in names:
        doc = yaml.safe_load((REPO / name).read_text())
        for svc, body in (doc.get("services") or {}).items():
            merged.setdefault(svc, {}).update(body or {})
    return merged


def _app_services(compose: dict) -> dict:
    return {
        name: body
        for name, body in compose.items()
        if name not in _NON_APP_SERVICES and not name.startswith(("redis", "mock-"))
    }


@pytest.fixture(scope="module")
def base() -> dict:
    return yaml.safe_load((REPO / "docker-compose.yml").read_text())["services"]


@pytest.fixture(scope="module")
def e2e() -> dict:
    return yaml.safe_load((REPO / "docker-compose.e2e.yml").read_text())["services"]


def test_env_defaults_does_not_define_the_required_environment_var() -> None:
    """The premise of the whole failure: nothing a bare `docker compose up` reads sets it.

    If this ever becomes false the tests below are still correct, just no longer load-bearing.
    """
    for name in (".env.defaults", ".env.example"):
        text = (REPO / name).read_text()
        assert not any(line.startswith("ENVIRONMENT=") for line in text.splitlines()), (
            f"{name} now sets ENVIRONMENT — re-check whether the e2e overrides are still needed"
        )


def test_every_app_service_gets_the_test_env_in_e2e(base: dict, e2e: dict) -> None:
    """A service inheriting the base `.env` in e2e has no ENVIRONMENT and dies on startup."""
    missing = []
    for name, body in _app_services(base).items():
        if ".env" not in (body.get("env_file") or []):
            continue
        override = e2e.get(name) or {}
        if ".env.test-docker" not in (override.get("env_file") or []):
            missing.append(name)

    assert not missing, (
        "docker-compose.e2e.yml must give these services `.env.test-docker`, or they start with no "
        f"ENVIRONMENT and crash in get_config(): {sorted(missing)}"
    )


def test_no_app_service_keeps_a_placebo_healthcheck(base: dict) -> None:
    """`exit 0` reports Healthy for a container whose worker is dead — that is what hid this."""
    placebo = [
        name
        for name, body in _app_services(base).items()
        if "exit 0" in " ".join((body.get("healthcheck") or {}).get("test") or [])
    ]
    assert not placebo, f"these healthchecks pass unconditionally and cannot detect a dead worker: {sorted(placebo)}"


def test_worker_healthchecks_actually_load_the_config(base: dict) -> None:
    """The probe has to exercise the thing that broke: constructing Config."""
    for name, body in _app_services(base).items():
        hc = body.get("healthcheck")
        if not hc:
            continue
        test = " ".join(hc.get("test") or [])
        if "get_config" not in test:
            continue
        assert "hippius_s3.config" in test, f"{name}: healthcheck must import the real config module"


def test_e2e_diagnostics_cover_every_app_service(base: dict) -> None:
    """A crash-looping worker is only debuggable if its log is in the artifacts.

    `purger` was absent from this list, which is why its identical crash never showed up even
    once the unpinner's had.
    """
    conftest = (REPO / "tests/e2e/conftest.py").read_text()
    dumped = conftest.split("for svc in [", 1)[1].split("]", 1)[0]
    missing = [name for name in _app_services(base) if f'"{name}"' not in dumped]
    assert not missing, f"tests/e2e/conftest.py must dump logs for: {sorted(missing)}"
