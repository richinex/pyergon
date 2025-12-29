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

from ergon.storage.base import (
    ExecutionLog,
    WorkNotificationSource,
    TimerNotificationSource,
)
from ergon.storage.sqlite import SqliteExecutionLog
from ergon.storage.redis import RedisExecutionLog
from ergon.storage.memory import InMemoryExecutionLog

__all__ = [
    "ExecutionLog",
    "WorkNotificationSource",
    "TimerNotificationSource",
    "SqliteExecutionLog",
    "RedisExecutionLog",
    "InMemoryExecutionLog",
]
