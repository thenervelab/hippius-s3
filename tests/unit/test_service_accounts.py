"""The service-account allowlist: parsing (hippius_s3.config) and the predicate.

Everything here guards one property — an account is exempt from billing if and only if its
exact SS58 address was spelled correctly in HIPPIUS_SERVICE_ACCOUNT_IDS. Every other input
must land on "billed".
"""

from typing import Any

import pytest

import hippius_s3.config as config_mod
from hippius_s3.config import _parse_service_accounts
from hippius_s3.config import get_config
from hippius_s3.services.service_accounts import is_service_account


ALICE = "5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY"
BOB = "5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty"


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------


def test_parses_multiple_addresses() -> None:
    assert _parse_service_accounts(f"{ALICE},{BOB}") == frozenset({ALICE, BOB})


@pytest.mark.parametrize("raw", ["", "   ", ",", ",,,", "  ,  ,  "])
def test_blank_input_yields_empty_allowlist(raw: str) -> None:
    """Fail-closed: an unset or degenerate secret must exempt nobody, never everybody."""
    assert _parse_service_accounts(raw) == frozenset()


def test_none_yields_empty_allowlist() -> None:
    assert _parse_service_accounts(None) == frozenset()


def test_tolerates_whitespace_and_quoting() -> None:
    """kubectl create secret --from-literal and .env files both leak these in."""
    assert _parse_service_accounts(f'  {ALICE} , "{BOB}" ') == frozenset({ALICE, BOB})


def test_deduplicates() -> None:
    assert _parse_service_accounts(f"{ALICE},{ALICE},{ALICE}") == frozenset({ALICE})


@pytest.mark.parametrize(
    "bad",
    [
        "not-an-address",
        "anonymous",
        "0xdeadbeef",
        # a real address with one character dropped — the near-miss a hand-edit produces
        ALICE[:-1],
        # base58 excludes 0/O/I/l; a transcription typo lands here
        ALICE[:-1] + "0",
    ],
)
def test_invalid_entry_raises_rather_than_being_dropped(bad: str) -> None:
    """A silently-dropped entry demotes an internal account back to 'billed', which surfaces
    hours later as a DLQ full of 402s with nothing pointing at the typo. Fail at boot instead."""
    with pytest.raises(ValueError, match="invalid SS58"):
        _parse_service_accounts(bad)


def test_one_bad_entry_rejects_the_whole_list() -> None:
    """No partial application: half an allowlist is a silently wrong allowlist."""
    with pytest.raises(ValueError):
        _parse_service_accounts(f"{ALICE},garbage,{BOB}")


def test_parse_result_is_immutable() -> None:
    """The config singleton is shared process-wide; a mutable allowlist would let any caller
    grant itself an exemption at runtime."""
    parsed = _parse_service_accounts(ALICE)
    assert isinstance(parsed, frozenset)
    with pytest.raises(AttributeError):
        parsed.add(BOB)  # ty: ignore[unresolved-attribute]


# ---------------------------------------------------------------------------
# Predicate
# ---------------------------------------------------------------------------


def test_exact_match_is_a_service_account() -> None:
    assert is_service_account(ALICE, frozenset({ALICE, BOB})) is True


def test_unlisted_account_is_not() -> None:
    assert is_service_account(BOB, frozenset({ALICE})) is False


@pytest.mark.parametrize("empty", [None, ""])
def test_missing_address_is_never_a_service_account(empty: Any) -> None:
    """An unauthenticated or half-populated request must not match an allowlist that
    happens to contain a falsy entry."""
    assert is_service_account(empty, frozenset({ALICE, ""})) is False


def test_empty_allowlist_exempts_nobody() -> None:
    assert is_service_account(ALICE, frozenset()) is False


def test_match_is_case_sensitive() -> None:
    """SS58 is base58, where case is significant: two addresses differing only in case are two
    different accounts. Case-insensitive matching would hand a stranger the exemption."""
    flipped = ALICE[0] + ALICE[1].swapcase() + ALICE[2:]
    assert flipped != ALICE
    assert is_service_account(flipped, frozenset({ALICE})) is False


def test_prefix_does_not_match() -> None:
    assert is_service_account(ALICE[:-3], frozenset({ALICE})) is False


def test_superstring_does_not_match() -> None:
    """Guards against a substring/startswith implementation: an attacker-registered address
    that merely contains an allowlisted one must not inherit the exemption."""
    assert is_service_account(ALICE + "X", frozenset({ALICE})) is False


def test_whitespace_padded_address_does_not_match() -> None:
    """Only the parser strips. A runtime address arriving with padding is a different string
    and must not be normalised into a match."""
    assert is_service_account(f" {ALICE} ", frozenset({ALICE})) is False


# ---------------------------------------------------------------------------
# Config integration
# ---------------------------------------------------------------------------


@pytest.fixture  # type: ignore[misc]
def restore_config_singleton() -> Any:
    """get_config memoizes; these tests rebuild it, so put the original back afterwards or
    every later test in the session sees the doctored config."""
    original = config_mod._config_singleton
    yield
    config_mod._config_singleton = original


def test_allowlist_survives_outside_the_test_environment(monkeypatch: Any, restore_config_singleton: Any) -> None:
    """enable_bypass_credit_check is deliberately clamped off unless ENVIRONMENT=test. The
    allowlist must NOT inherit that clamp — it is the production mechanism, not a test escape —
    while the blanket bypass must stay clamped. This test fails if someone 'consistently'
    extends the clamp to both.
    """
    monkeypatch.setenv("ENVIRONMENT", "production")
    monkeypatch.setenv("HIPPIUS_SERVICE_ACCOUNT_IDS", f"{ALICE},{BOB}")
    monkeypatch.setenv("HIPPIUS_BYPASS_CREDIT_CHECK", "true")
    config_mod._config_singleton = None

    cfg = get_config()

    assert cfg.service_account_ids == frozenset({ALICE, BOB})
    assert cfg.enable_bypass_credit_check is False, "the blanket test bypass must stay clamped in production"


def test_unset_env_gives_an_empty_allowlist(monkeypatch: Any, restore_config_singleton: Any) -> None:
    monkeypatch.delenv("HIPPIUS_SERVICE_ACCOUNT_IDS", raising=False)
    config_mod._config_singleton = None

    assert get_config().service_account_ids == frozenset()


def test_malformed_env_fails_startup(monkeypatch: Any, restore_config_singleton: Any) -> None:
    """A bad allowlist must stop the process at boot, where it is one log line, rather than
    degrade quietly into 'everything is billed'."""
    monkeypatch.setenv("HIPPIUS_SERVICE_ACCOUNT_IDS", "definitely-not-ss58")
    config_mod._config_singleton = None

    with pytest.raises(ValueError, match="invalid SS58"):
        get_config()
