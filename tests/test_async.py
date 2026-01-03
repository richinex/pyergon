"""
Async tests for Ergon Python bindings - Phase 2

Tests async storage operations.
"""

import asyncio
import uuid

import pytest

import pyergon


@pytest.mark.asyncio
async def test_in_memory_get_invocation_not_found():
    """Test that get_invocation returns None for non-existent invocation."""
    storage = pyergon.InMemoryExecutionLog()

    flow_id = str(uuid.uuid4())
    result = await storage.get_invocation(flow_id, step=0)

    assert result is None


@pytest.mark.asyncio
async def test_in_memory_get_invocations_for_flow_empty():
    """Test that get_invocations_for_flow returns empty list for non-existent flow."""
    storage = pyergon.InMemoryExecutionLog()

    flow_id = str(uuid.uuid4())
    result = await storage.get_invocations_for_flow(flow_id)

    assert result == []


@pytest.mark.asyncio
async def test_in_memory_reset():
    """Test that reset() works without errors."""
    storage = pyergon.InMemoryExecutionLog()

    # Reset should succeed even on empty storage
    await storage.reset()


@pytest.mark.asyncio
async def test_sqlite_get_invocation_not_found():
    """Test that get_invocation returns None for non-existent invocation."""
    storage = await pyergon.SqliteExecutionLog.in_memory()

    flow_id = str(uuid.uuid4())
    result = await storage.get_invocation(flow_id, step=0)

    assert result is None


@pytest.mark.asyncio
async def test_sqlite_get_invocations_for_flow_empty():
    """Test that get_invocations_for_flow returns empty list for non-existent flow."""
    storage = await pyergon.SqliteExecutionLog.in_memory()

    flow_id = str(uuid.uuid4())
    result = await storage.get_invocations_for_flow(flow_id)

    assert result == []


@pytest.mark.asyncio
async def test_sqlite_reset():
    """Test that reset() works without errors."""
    storage = await pyergon.SqliteExecutionLog.in_memory()

    # Reset should succeed even on empty storage
    await storage.reset()


@pytest.mark.asyncio
async def test_invalid_uuid():
    """Test that invalid UUID is accepted (stored as string)."""
    storage = pyergon.InMemoryExecutionLog()

    # Python implementation stores flow_id as string, doesn't validate UUID format
    # This is valid behavior - just returns None for non-existent flow
    result = await storage.get_invocation("not-a-uuid", step=0)
    assert result is None


@pytest.mark.asyncio
async def test_concurrent_operations():
    """Test that multiple async operations can run concurrently."""
    storage = pyergon.InMemoryExecutionLog()

    # Create multiple concurrent tasks
    tasks = [storage.get_invocation(str(uuid.uuid4()), step=0) for _ in range(10)]

    # All should complete without errors
    results = await asyncio.gather(*tasks)

    # All should return None (no invocations exist)
    assert all(r is None for r in results)


if __name__ == "__main__":
    # Run tests with pytest
    pytest.main([__file__, "-v", "-s"])
