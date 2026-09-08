"""Boolean feature-flag parsing.

`HIPPIUS_ENABLE_BILLING_PLANS` decides whether a billing feature is live, and its value travels
GitHub secret -> `kubectl create secret --from-literal` -> envFrom -> os.environ. Every hop can add
quoting or whitespace, and an operator may reasonably type `1` or `True`.

The repo's older flags use `x.lower() == "true"`, which reads `1` as FALSE. For this switch that
failure is invisible: you set it, redeploy, and nothing happens. Hence the explicit parser.
"""

import pytest

from hippius_s3.config import _parse_bool


@pytest.mark.parametrize("value", ["true", "True", "TRUE", "  true  ", "1", "yes", "Yes", "y", "on", "ON"])
def test_truthy_values(value: str) -> None:
    assert _parse_bool(value) is True


@pytest.mark.parametrize("value", ["false", "False", "FALSE", "0", "no", "n", "off", "", "   "])
def test_falsy_values(value: str) -> None:
    assert _parse_bool(value) is False


def test_unset_is_false() -> None:
    """An unset GitHub secret interpolates to an empty string, which must mean off — never a
    startup failure that takes the fleet down for a flag nobody set."""
    assert _parse_bool(None) is False
    assert _parse_bool("") is False


@pytest.mark.parametrize("value", ['"true"', "'true'", '"1"', "'False'"])
def test_quotes_are_stripped(value: str) -> None:
    """`--from-literal=KEY='true'` and a quoted YAML value both reach us with the quotes attached."""
    assert _parse_bool(value) is (value.strip("\"'").lower() in {"true", "1"})


@pytest.mark.parametrize("value", ["ture", "enabled", "2", "-1", "maybe", "TRUE!", "t rue"])
def test_a_typo_raises_rather_than_silently_defaulting(value: str) -> None:
    """The whole point. `HIPPIUS_ENABLE_BILLING_PLANS=ture` silently meaning False is the worst
    outcome: the flag is flipped, the deploy is green, and nothing changes with no explanation."""
    with pytest.raises(ValueError, match="expected a boolean"):
        _parse_bool(value)


def test_the_error_names_the_accepted_values() -> None:
    with pytest.raises(ValueError) as exc:
        _parse_bool("ture")

    message = str(exc.value)
    assert "'true'" in message and "'1'" in message and "'false'" in message
    assert "'ture'" in message, "the rejected value must appear so the fix is obvious"


def test_billing_plans_defaults_to_off(monkeypatch: pytest.MonkeyPatch) -> None:
    """Ship-disabled is the contract: the feature must never be live because someone forgot to set
    something."""
    from hippius_s3.config import Config

    monkeypatch.delenv("HIPPIUS_ENABLE_BILLING_PLANS", raising=False)
    assert Config().enable_billing_plans is False


@pytest.mark.parametrize("value,expected", [("1", True), ("True", True), ("0", False), ("False", False)])
def test_billing_plans_reads_the_env_var(monkeypatch: pytest.MonkeyPatch, value: str, expected: bool) -> None:
    from hippius_s3.config import Config

    monkeypatch.setenv("HIPPIUS_ENABLE_BILLING_PLANS", value)
    assert Config().enable_billing_plans is expected
