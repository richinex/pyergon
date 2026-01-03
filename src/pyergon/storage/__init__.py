"""
Storage layer for ergon durable execution.

This module provides storage backends for persisting workflow state:
- ExecutionLog: Abstract interface (Protocol using ABC)
- SqliteExecutionLog: SQLite-backed storage
- RedisExecutionLog: Redis-backed distributed storage
- InMemoryExecutionLog: In-memory storage for testing

Design Pattern: Adapter Pattern (Chapter 10)
All storage implementations adapt to the ExecutionLog interface,
allowing clients to work with any storage backend uniformly.

Design Principle: Dependency Inversion (SOLID)
Clients depend on ExecutionLog abstraction, not concrete implementations.
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
