"""The peer secret is refused at boot if the client could not put it on the wire.

`httpx` ascii-encodes header values and raises `UnicodeEncodeError` doing it. That is a
`ValueError`, so `PeerChunkFetcher.__call__`'s `except (httpx.HTTPError, OSError)` does not catch
it: a non-ASCII secret turns every peer read into a hard failure on the GET path rather than the
documented fallback to the pool. Widening that except would be worse than fixing it here — it
would convert an operator typo into a silently dark peer tier, which is the exact failure mode
this feature's other guards exist to remove.

So the check belongs at startup, where the log line can name the variable and the operator is
already looking.
"""

from __future__ import annotations

import pytest

from tests.unit.routing_helpers import route_names
from fastapi import FastAPI

from hippius_s3.config import reset_config
from hippius_s3.peer_auth import PeerSecretError
from hippius_s3.peer_auth import validate_peer_secret


# `openssl rand -hex 32` shaped, for the cases that need a secret the validator accepts.
GENERATED = "3f8c1d9e0b7a6452e1d3c8f90a2b4d6e8f0a1c3e5d7b9f2a4c6e8d0b1f3a5c7e"


def test_an_empty_secret_is_accepted_because_it_means_the_feature_is_off() -> None:
    """The regression that would take prod down on deploy.

    Empty already carries meaning: it is how `factory()` decides not to mount the peer route at
    all. Prod currently runs with the feature off, so treating empty as a misconfiguration would
    turn this validation into a boot failure for every pod in the fleet.
    """
    validate_peer_secret("")


def test_a_well_formed_hex_secret_is_accepted() -> None:
    validate_peer_secret("a" * 64)
    validate_peer_secret("0123456789abcdef" * 4)
    # The pattern permits A-F, so an operator using `tr a-f A-F` or a capitalised paste must not
    # be rejected. Permission that nothing exercises is permission nobody knows they have.
    validate_peer_secret(GENERATED.upper())


@pytest.mark.parametrize(
    "secret",
    [
        "sécret" + "a" * 58,  # the actual failure: a non-ASCII byte httpx cannot encode
        "\xff" * 64,
        "z" * 64,  # ASCII but not hex
        "a" * 63,  # right alphabet, wrong length
        "a" * 65,
        "  " + "a" * 62,  # leading whitespace, a copy-paste artefact
    ],
)
def test_a_malformed_secret_is_refused(secret: str) -> None:
    with pytest.raises(PeerSecretError):
        validate_peer_secret(secret)


def test_the_error_names_the_variable_and_the_expected_shape() -> None:
    """The whole point of failing at boot is that the log line makes the fix obvious.

    An error that says only "invalid secret" costs the operator the round-trip this check was
    supposed to save.
    """
    with pytest.raises(PeerSecretError) as exc:
        validate_peer_secret("nope")

    message = str(exc.value)
    assert "HIPPIUS_INTERNAL_PEER_SECRET" in message
    assert "64" in message
    assert "hex" in message.lower()


def test_the_error_does_not_echo_the_secret() -> None:
    """A boot failure is logged, and logs are shipped to Loki and read by more people than hold
    the secret. A validation error that pastes the rejected value into the log turns a typo into
    a disclosure — and the near-miss case (one wrong character) would leak an almost-correct one.
    """
    with pytest.raises(PeerSecretError) as exc:
        validate_peer_secret("deadbeef" * 7 + "zzzzzzzz")

    assert "deadbeef" not in str(exc.value)


# ------------------------------------------------------------------ actually wired into boot
#
# Everything above tests the validator as a function. None of it proves `factory()` CALLS it —
# verified by deleting the call, which left the whole suite green. A correct check nothing
# reaches is the same defect as no check, and it is the failure this task has already hit once
# (a header constant each side agreed with itself about). These go through the real boot path.


def _boot(monkeypatch, secret: str, *, serving: str) -> FastAPI:
    monkeypatch.setenv("HIPPIUS_INTERNAL_PEER_SECRET", secret)
    monkeypatch.setenv("HIPPIUS_PEER_SERVE_ENABLED", serving)
    reset_config()

    from hippius_s3.main import factory

    return factory()


def test_a_non_ascii_secret_fails_the_api_boot(monkeypatch) -> None:
    """Not merely "the validator would reject it" — the api must refuse to come up."""
    with pytest.raises(PeerSecretError, match="HIPPIUS_INTERNAL_PEER_SECRET"):
        _boot(monkeypatch, "sécret" + "a" * 58, serving="true")


def test_a_malformed_secret_fails_the_api_boot_even_with_serving_off(monkeypatch) -> None:
    """Serving off does not make a broken secret harmless: it is the FETCH path that raises.

    A pod that only fetches from peers still puts the secret on the wire, so gating the check
    on the serve flag would leave the failure it exists to prevent wide open.
    """
    with pytest.raises(PeerSecretError, match="HIPPIUS_INTERNAL_PEER_SECRET"):
        _boot(monkeypatch, "not-hex", serving="false")


def test_a_valid_hex_secret_boots_with_the_route_mounted(monkeypatch) -> None:
    app = _boot(monkeypatch, GENERATED, serving="true")

    assert "get_local_chunk" in route_names(app)


@pytest.mark.parametrize("serving", ["true", "false"])
def test_an_empty_secret_boots_with_the_route_absent(monkeypatch, serving: str) -> None:
    """THE regression to protect: getting this wrong takes prod down on deploy.

    Empty is what every environment runs today. It must load cleanly and simply not mount the
    route — under either setting of the serve flag, since the flag alone must never be able to
    crash a pod.
    """
    app = _boot(monkeypatch, "", serving=serving)

    assert "get_local_chunk" not in route_names(app)
