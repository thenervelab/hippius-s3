from __future__ import annotations

import os


# The e2e Postgres publishes on the host's 5432 by default, which collides with a developer's own
# local Postgres. Overriding this lets the compose stack remap the host port (see
# docker-compose.e2e-local.yml) without every DB assertion silently querying the wrong database.
DEFAULT_DSN = os.getenv("HIPPIUS_E2E_DB_DSN", "postgresql://postgres:postgres@localhost:5432/hippius")
