import os
from pathlib import Path
from typing import Generator

import dotenv
import pytest


@pytest.fixture(scope="session", autouse=True)
def _load_test_env() -> Generator[None, None, None]:
    """Load test environment variables from base + local env files."""
    project_root = Path(__file__).parents[2]
    dotenv.load_dotenv(project_root / ".env.defaults", override=True)
    dotenv.load_dotenv(project_root / ".env.test-local", override=True)
    os.environ["HIPPIUS_BYPASS_CREDIT_CHECK"] = "true"
    yield
