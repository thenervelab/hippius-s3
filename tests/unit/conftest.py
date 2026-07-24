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


@pytest.fixture(autouse=True)
def _reset_janitor_pressure_globals() -> "object":
    # _pressure_mode reads module-global disk/pool state. The pool-fullness gate added
    # _fs_pool_percent_used, which _update_disk_metrics writes DIRECTLY (not via monkeypatch), so a
    # test that drives the pool high would bleed Critical into later pressure cases — across files.
    # Reset the pressure globals around every test that imported the janitor. Guarded on sys.modules
    # so non-janitor unit tests don't pay the import.
    import sys

    def _reset() -> None:
        mod = sys.modules.get("workers.run_janitor_in_loop")
        if mod is not None:
            mod._fs_pool_percent_used = None
            mod._prev_pressure_mode = 0
            mod._fs_pressure_mode = 0

    _reset()
    yield
    _reset()
