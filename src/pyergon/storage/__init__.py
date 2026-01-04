"""Storage backends for durable workflow state persistence.

Provides multiple storage implementations behind a common interface:
    - ExecutionLog: Abstract interface
    - SqliteExecutionLog: SQLite-backed storage
    - RedisExecutionLog: Redis-backed distributed storage
    - InMemoryExecutionLog: In-memory storage for testing

Design: Adapter Pattern + Dependency Inversion (SOLID)
    All storage implementations adapt to ExecutionLog interface.
    Clients depend on abstraction, not concrete implementations,
    enabling easy swapping between storage backends.
"""

from pyergon.storage.base import (
    ExecutionLog,
    TimerNotificationSource,
    WorkNotificationSource,
)

# Lazy imports to avoid circular dependency:
# pyergon.core.context imports from pyergon.storage.base, so we can't
# import storage implementations here (they import from pyergon.core)


def __getattr__(name: str):
    """Lazy import storage implementations to avoid circular imports."""
    if name == "InMemoryExecutionLog":
        from pyergon.storage.memory import InMemoryExecutionLog

        return InMemoryExecutionLog
    elif name == "RedisExecutionLog":
        from pyergon.storage.redis import RedisExecutionLog

        return RedisExecutionLog
    elif name == "SqliteExecutionLog":
        from pyergon.storage.sqlite import SqliteExecutionLog

        return SqliteExecutionLog
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "ExecutionLog",
    "WorkNotificationSource",
    "TimerNotificationSource",
    "SqliteExecutionLog",
    "RedisExecutionLog",
    "InMemoryExecutionLog",
]
