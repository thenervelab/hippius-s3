"""Shared app-state fakes for acl_middleware's owner-suspension lookup (issue #421).

acl_middleware now consults account_suspensions (via app.state.postgres_pool +
app.state.redis_client) whenever a bucket owner is resolved. Test apps that register
acl_middleware install these no-suspension fakes so pre-existing scenarios behave as
before; suspension-specific tests build their own state.
"""

from __future__ import annotations

from typing import Any


class NoSuspensionPool:
    async def fetchrow(self, query: str, *args: Any) -> Any:
        return None


class NoopRedis:
    async def get(self, key: str) -> Any:
        return None

    async def setex(self, key: str, ttl: int, value: Any) -> None:
        return None

    async def delete(self, key: str) -> None:
        return None


def install_no_suspension_state(app: Any) -> None:
    app.state.postgres_pool = NoSuspensionPool()
    app.state.redis_client = NoopRedis()
