import os
from pathlib import Path

import dotenv


_project_root = Path(__file__).parents[2]
dotenv.load_dotenv(_project_root / ".env.defaults", override=True)
dotenv.load_dotenv(_project_root / ".env.test-local", override=True)
os.environ["HIPPIUS_BYPASS_CREDIT_CHECK"] = "true"
os.environ.setdefault("HIPPIUS_IPFS_API_URLS", "http://127.0.0.1:5001")
