"""
Basic tests for Ergon Python bindings - Phase 1

Tests core type creation and basic operations.
"""

import pytest
import ergon


def test_import():
    """Test that the module can be imported."""
    assert hasattr(ergon, "SqliteExecutionLog")
    assert hasattr(ergon, "InMemoryExecutionLog")
    assert hasattr(ergon, "Invocation")
    assert hasattr(ergon, "InvocationStatus")


def test_version():
    """Test that version is available."""
    assert hasattr(ergon, "__version__")
    assert isinstance(ergon.__version__, str)
    print(f"Ergon version: {ergon.__version__}")


def test_sqlite_log_creation():
    """Test creating a SqliteExecutionLog."""
    import tempfile
    import os

    # Use temporary file
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    try:
        storage = ergon.SqliteExecutionLog(db_path)
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
    storage = await ergon.SqliteExecutionLog.in_memory()
    assert storage is not None
    assert "in-memory" in repr(storage)
    print(f"Created in-memory SQLite log: {repr(storage)}")
    await storage.close()


def test_in_memory_log_creation():
    """Test creating an InMemoryExecutionLog."""
    storage = ergon.InMemoryExecutionLog()
    assert storage is not None
    assert str(storage) == "InMemoryExecutionLog"
    print(f"Created in-memory log: {repr(storage)}")


def test_multiple_storages():
    """Test creating multiple storage instances."""
    storage1 = ergon.InMemoryExecutionLog()
    storage2 = ergon.InMemoryExecutionLog()

    assert storage1 is not storage2
    print("Created multiple storage instances successfully")


if __name__ == "__main__":
    # Run tests with pytest
    pytest.main([__file__, "-v"])
