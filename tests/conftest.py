"""
Pytest configuration and fixtures for ergon tests.

Provides reusable fixtures for storage backends, test flows, and utilities.
"""

import asyncio
import shutil
import tempfile
from collections.abc import AsyncGenerator
from dataclasses import dataclass
from pathlib import Path
from uuid import uuid4

import pytest
from hypothesis import strategies as st

from pyergon.core import Invocation, InvocationStatus
from pyergon.decorators import flow, flow_type, step
from pyergon.storage import InMemoryExecutionLog, SqliteExecutionLog


def pytest_sessionfinish(session, exitstatus):
    """Force cleanup after all tests complete to prevent CI hanging."""
    import os
    import signal

    # In CI environments only, force exit to prevent hanging
    if os.getenv("CI") or os.getenv("GITHUB_ACTIONS"):
        # Force exit immediately with the pytest exit status
        os._exit(exitstatus)


@pytest.fixture
async def in_memory_storage() -> AsyncGenerator[InMemoryExecutionLog, None]:
    """Async in-memory storage fixture with automatic cleanup."""
    storage = InMemoryExecutionLog()
    yield storage
    await storage.reset()


@pytest.fixture
async def sqlite_memory_storage() -> AsyncGenerator[SqliteExecutionLog, None]:
    """Async SQLite in-memory storage fixture with automatic cleanup."""
    storage = SqliteExecutionLog(":memory:")
    await storage.connect()
    yield storage
    await storage.close()


@pytest.fixture
def temp_db_path():
    """Temporary database file path with automatic cleanup."""
    tmpdir = Path(tempfile.mkdtemp())
    db_path = tmpdir / "test.db"
    yield db_path
    shutil.rmtree(tmpdir, ignore_errors=True)


@pytest.fixture
async def sqlite_file_storage(temp_db_path: Path) -> AsyncGenerator[SqliteExecutionLog, None]:
    """Async SQLite file-based storage fixture with automatic cleanup."""
    storage = SqliteExecutionLog(str(temp_db_path))
    await storage.connect()
    yield storage
    await storage.close()


@pytest.fixture
def random_flow_id() -> str:
    """Generate random flow ID for testing."""
    return str(uuid4())


# Sample test flows for reuse across tests


@dataclass
@flow_type
class SimpleTestFlow:
    """Simple flow for basic testing."""

    value: int = 0

    @step
    async def compute(self) -> int:
        """Simple computation step."""
        await asyncio.sleep(0.001)  # Simulate work
        return self.value * 2

    @flow
    async def run(self) -> int:
        """Flow entry point."""
        result = await self.compute()
        return result


@dataclass
@flow_type
class MultiStepTestFlow:
    """Multi-step flow for testing step sequencing."""

    initial_value: int = 0

    @step
    async def step_one(self) -> int:
        await asyncio.sleep(0.001)
        return self.initial_value + 1

    @step
    async def step_two(self, value: int) -> int:
        await asyncio.sleep(0.001)
        return value * 2

    @step
    async def step_three(self, value: int) -> int:
        await asyncio.sleep(0.001)
        return value + 10

    @flow
    async def run(self) -> int:
        v1 = await self.step_one()
        v2 = await self.step_two(v1)
        v3 = await self.step_three(v2)
        return v3


@dataclass
@flow_type
class FlakyTestFlow:
    """Flow with controlled failure for retry testing."""

    fail_on_attempt: int = 1
    current_attempt: int = 0

    @step
    async def flaky_operation(self) -> str:
        self.current_attempt += 1
        if self.current_attempt == self.fail_on_attempt:
            raise ValueError(f"Intentional failure on attempt {self.current_attempt}")
        return "success"

    @flow
    async def run(self) -> str:
        return await self.flaky_operation()


@pytest.fixture
def simple_flow() -> SimpleTestFlow:
    """Simple test flow instance."""
    return SimpleTestFlow(value=21)


@pytest.fixture
def multi_step_flow() -> MultiStepTestFlow:
    """Multi-step test flow instance."""
    return MultiStepTestFlow(initial_value=5)


@pytest.fixture
def flaky_flow() -> FlakyTestFlow:
    """Flaky test flow instance."""
    return FlakyTestFlow(fail_on_attempt=2)


# Hypothesis strategies for property-based testing


@st.composite
def invocation_strategy(draw):
    """Strategy for generating valid Invocation objects."""
    inv_id = str(uuid4())
    flow_id = str(uuid4())
    step = draw(st.integers(min_value=0, max_value=1000))
    status = draw(
        st.sampled_from(
            [
                InvocationStatus.PENDING,
                InvocationStatus.COMPLETE,
                InvocationStatus.WAITING_FOR_SIGNAL,
                InvocationStatus.WAITING_FOR_TIMER,
            ]
        )
    )

    return Invocation(
        id=inv_id,
        flow_id=flow_id,
        step=step,
        class_name=draw(
            st.text(
                min_size=1, max_size=50, alphabet=st.characters(whitelist_categories=("Lu", "Ll"))
            )
        ),
        method_name=draw(
            st.text(
                min_size=1, max_size=50, alphabet=st.characters(whitelist_categories=("Lu", "Ll"))
            )
        ),
        parameters=draw(st.binary(min_size=0, max_size=100)),
        params_hash=draw(st.integers()),
        status=status,
        attempts=draw(st.integers(min_value=0, max_value=10)),
    )


# Register strategies for easy import
pytest.invocation_strategy = invocation_strategy
