import os
from pathlib import Path

import dotenv
import pytest


_project_root = Path(__file__).parents[2]
dotenv.load_dotenv(_project_root / ".env.defaults", override=True)
dotenv.load_dotenv(_project_root / ".env.test-local", override=True)
os.environ["HIPPIUS_BYPASS_CREDIT_CHECK"] = "true"


@pytest.fixture(autouse=True)
def _reset_config_singleton() -> "object":
    # get_config() is now memoized; drop the cached instance around every test so env mutations
    # (monkeypatch.setenv / setattr on a returned config) don't leak across cases.
    from hippius_s3 import config as _config

    _config.reset_config()
    yield
    _config.reset_config()
