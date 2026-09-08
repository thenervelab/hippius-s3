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


# ---------------------------------------------------------------------------
# The switch is held as TWO GitHub secrets, HIPPIUS_ENABLE_BILLING_PLANS_STAGING and
# ..._PROD, so staging and production move independently. Each deploy workflow writes only its own
# key into that cluster's Secret, and the pod then selects by its own ENVIRONMENT.
# ---------------------------------------------------------------------------

BILLING_VARS = (
    "HIPPIUS_ENABLE_BILLING_PLANS",
    "HIPPIUS_ENABLE_BILLING_PLANS_STAGING",
    "HIPPIUS_ENABLE_BILLING_PLANS_PROD",
)


@pytest.fixture
def clean_env(monkeypatch: pytest.MonkeyPatch) -> pytest.MonkeyPatch:
    for var in BILLING_VARS:
        monkeypatch.delenv(var, raising=False)
    return monkeypatch


def resolve() -> bool:
    from hippius_s3.config import Config

    return Config().enable_billing_plans


def test_defaults_to_off_when_nothing_is_set(clean_env: pytest.MonkeyPatch) -> None:
    """Ship-disabled is the contract: the feature must never be live because someone forgot to set
    something."""
    clean_env.setenv("ENVIRONMENT", "production")
    assert resolve() is False


@pytest.mark.parametrize("value,expected", [("1", True), ("True", True), ("0", False), ("False", False)])
def test_the_unsuffixed_var_still_works_for_local_and_tests(
    clean_env: pytest.MonkeyPatch, value: str, expected: bool
) -> None:
    clean_env.setenv("ENVIRONMENT", "local")
    clean_env.setenv("HIPPIUS_ENABLE_BILLING_PLANS", value)
    assert resolve() is expected


def test_staging_reads_the_staging_secret(clean_env: pytest.MonkeyPatch) -> None:
    clean_env.setenv("ENVIRONMENT", "staging")
    clean_env.setenv("HIPPIUS_ENABLE_BILLING_PLANS_STAGING", "true")
    assert resolve() is True


def test_production_reads_the_prod_secret(clean_env: pytest.MonkeyPatch) -> None:
    """ENVIRONMENT is "production" but the secret suffix is PROD — an uppercase of ENVIRONMENT would
    look for _PRODUCTION, find nothing, and silently leave the feature off."""
    clean_env.setenv("ENVIRONMENT", "production")
    clean_env.setenv("HIPPIUS_ENABLE_BILLING_PLANS_PROD", "true")
    assert resolve() is True


def test_production_cannot_read_stagings_flag(clean_env: pytest.MonkeyPatch) -> None:
    """THE safety property. Each workflow seeds only its own key, but if both ever landed in one
    cluster a production pod must still refuse to enable itself from staging's value."""
    clean_env.setenv("ENVIRONMENT", "production")
    clean_env.setenv("HIPPIUS_ENABLE_BILLING_PLANS_STAGING", "true")
    clean_env.setenv("HIPPIUS_ENABLE_BILLING_PLANS_PROD", "false")
    assert resolve() is False


def test_staging_cannot_read_productions_flag(clean_env: pytest.MonkeyPatch) -> None:
    clean_env.setenv("ENVIRONMENT", "staging")
    clean_env.setenv("HIPPIUS_ENABLE_BILLING_PLANS_STAGING", "false")
    clean_env.setenv("HIPPIUS_ENABLE_BILLING_PLANS_PROD", "true")
    assert resolve() is False


def test_the_environment_specific_secret_beats_the_unsuffixed_one(clean_env: pytest.MonkeyPatch) -> None:
    clean_env.setenv("ENVIRONMENT", "staging")
    clean_env.setenv("HIPPIUS_ENABLE_BILLING_PLANS", "true")
    clean_env.setenv("HIPPIUS_ENABLE_BILLING_PLANS_STAGING", "false")
    assert resolve() is False


def test_an_empty_environment_secret_falls_through_rather_than_forcing_false(
    clean_env: pytest.MonkeyPatch,
) -> None:
    """An unset GitHub secret interpolates to '' through --from-literal. That means "not configured
    here", not "explicitly disabled" — otherwise adding the secretKeyRef before creating the secret
    would silently pin the feature off and mask the unsuffixed fallback."""
    clean_env.setenv("ENVIRONMENT", "staging")
    clean_env.setenv("HIPPIUS_ENABLE_BILLING_PLANS_STAGING", "")
    clean_env.setenv("HIPPIUS_ENABLE_BILLING_PLANS", "true")
    assert resolve() is True


def test_an_unknown_environment_falls_back_to_the_unsuffixed_var(clean_env: pytest.MonkeyPatch) -> None:
    """e2e runs with ENVIRONMENT=test, which has no dedicated secret."""
    clean_env.setenv("ENVIRONMENT", "test")
    clean_env.setenv("HIPPIUS_ENABLE_BILLING_PLANS_STAGING", "true")
    assert resolve() is False


def test_a_typo_in_the_environment_secret_still_raises(clean_env: pytest.MonkeyPatch) -> None:
    clean_env.setenv("ENVIRONMENT", "staging")
    clean_env.setenv("HIPPIUS_ENABLE_BILLING_PLANS_STAGING", "ture")
    with pytest.raises(ValueError, match="expected a boolean"):
        resolve()


def test_each_workflow_seeds_only_its_own_secret() -> None:
    """The first line of defence is that staging's Secret never contains the prod key at all.

    A copy-paste that seeded both from one workflow would put production's flag inside the staging
    cluster, where a future refactor of the selection logic could reach it.
    """
    import pathlib

    staging = pathlib.Path(".github/workflows/staging-deploy.yaml").read_text()
    production = pathlib.Path(".github/workflows/production-deploy.yaml").read_text()

    assert "HIPPIUS_ENABLE_BILLING_PLANS_STAGING" in staging
    assert "HIPPIUS_ENABLE_BILLING_PLANS_PROD" not in staging
    assert "HIPPIUS_ENABLE_BILLING_PLANS_PROD" in production
    assert "HIPPIUS_ENABLE_BILLING_PLANS_STAGING" not in production
