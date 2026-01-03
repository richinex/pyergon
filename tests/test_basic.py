"""
Basic tests for Ergon Python bindings - Phase 1

Tests core type creation and basic operations.
"""

import pytest

import pyergon


def test_import():
    """Test that the module can be imported."""
    assert hasattr(pyergon, "SqliteExecutionLog")
    assert hasattr(pyergon, "InMemoryExecutionLog")
    assert hasattr(pyergon, "Invocation")
    assert hasattr(pyergon, "InvocationStatus")


def test_version():
    """Test that version is available."""
    assert hasattr(pyergon, "__version__")
    assert isinstance(pyergon.__version__, str)
    print(f"Ergon version: {pyergon.__version__}")


def test_sqlite_log_creation():
    """Test creating a SqliteExecutionLog."""
    import os
    import tempfile

    # Use temporary file
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    try:
        storage = pyergon.SqliteExecutionLog(db_path)
        assert storage is not None
        # Repr should contain the path
        assert db_path in repr(storage)
        print(f"Created SQLite log: {repr(storage)}")
    finally:
        # Cleanup
        if os.path.exists(db_path):
            os.unlink(db_path)


@pytest.mark.asyncio
async def test_sqlite_in_memory():
    """Test creating an in-memory SqliteExecutionLog."""
    storage = await pyergon.SqliteExecutionLog.in_memory()
    assert storage is not None
    assert "in-memory" in repr(storage)
    print(f"Created in-memory SQLite log: {repr(storage)}")
    await storage.close()


def test_in_memory_log_creation():
    """Test creating an InMemoryExecutionLog."""
    storage = pyergon.InMemoryExecutionLog()
    assert storage is not None
    assert str(storage) == "InMemoryExecutionLog"
    print(f"Created in-memory log: {repr(storage)}")


def test_multiple_storages():
    """Test creating multiple storage instances."""
    storage1 = pyergon.InMemoryExecutionLog()
    storage2 = pyergon.InMemoryExecutionLog()

    assert storage1 is not storage2
    print("Created multiple storage instances successfully")


if __name__ == "__main__":
    # Run tests with pytest
    pytest.main([__file__, "-v"])
