import os
from pathlib import Path

import dotenv


_project_root = Path(__file__).parents[2]
dotenv.load_dotenv(_project_root / ".env.defaults", override=True)
dotenv.load_dotenv(_project_root / ".env.test-local", override=True)
os.environ["HIPPIUS_BYPASS_CREDIT_CHECK"] = "true"
