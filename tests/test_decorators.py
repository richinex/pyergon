"""
Tests for Phase 3 decorator system.

Tests @flow, @flow_type, and @step decorators and Executor execution.
"""

import asyncio
from dataclasses import dataclass

import pytest

import pyergon


@dataclass
@pyergon.flow_type
class SimpleWorkflow:
    """A simple workflow for testing."""

    def __post_init__(self):
        self.execution_log = []

    @pyergon.step
    async def step_one(self, value: int) -> int:
        """First step: multiply by 2."""
        self.execution_log.append(f"step_one({value})")
        return value * 2

    @pyergon.step
    async def step_two(self, value: int) -> int:
        """Second step: add 10."""
        self.execution_log.append(f"step_two({value})")
        return value + 10

    @pyergon.flow
    async def run(self, initial_value: int) -> int:
        """Main flow: chain the steps."""
        result1 = await self.step_one(initial_value)
        result2 = await self.step_two(result1)
        return result2


def test_step_decorator():
    """Test that @step decorator adds metadata."""

    @pyergon.step
    async def test_step():
        pass

    assert hasattr(test_step, "_is_ergon_step")
    assert test_step._is_ergon_step is True
    assert test_step._step_name == "test_step"


def test_flow_type_decorator():
    """Test that @flow_type decorator adds metadata."""

    @dataclass
    @pyergon.flow_type
    class TestFlow:
        @pyergon.step
        async def step_a(self):
            pass

        @pyergon.step
        async def step_b(self):
            pass

        @pyergon.flow
        async def run(self):
            pass

    assert hasattr(TestFlow, "_is_ergon_flow")
    assert TestFlow._is_ergon_flow is True
    assert hasattr(TestFlow, "_ergon_steps")
    assert "step_a" in TestFlow._ergon_steps
    assert "step_b" in TestFlow._ergon_steps
    assert "run" not in TestFlow._ergon_steps  # run is not a step


@pytest.mark.asyncio
async def test_executor_execution():
    """Test basic flow execution with Executor."""
    storage = pyergon.InMemoryExecutionLog()
    workflow = SimpleWorkflow()

    executor = pyergon.Executor(workflow, storage, "test-flow-1")
    outcome = await executor.run(lambda w: w.run(5))

    # 5 * 2 = 10, 10 + 10 = 20
    assert isinstance(outcome, pyergon.Completed)
    assert outcome.result == 20


@pytest.mark.asyncio
async def test_executor_tracks_steps():
    """Test that executor tracks step execution."""
    storage = pyergon.InMemoryExecutionLog()
    workflow = SimpleWorkflow()

    executor = pyergon.Executor(workflow, storage, "test-flow-2")
    outcome = await executor.run(lambda w: w.run(5))

    assert isinstance(outcome, pyergon.Completed)
    assert outcome.result == 20

    # Both steps should have executed
    assert len(workflow.execution_log) == 2
    assert "step_one(5)" in workflow.execution_log
    assert "step_two(10)" in workflow.execution_log


@pytest.mark.asyncio
async def test_executor_with_sqlite():
    """Test flow execution with SQLite storage."""
    storage = await pyergon.SqliteExecutionLog.in_memory()
    workflow = SimpleWorkflow()

    executor = pyergon.Executor(workflow, storage, "test-flow-3")
    outcome = await executor.run(lambda w: w.run(3))

    # 3 * 2 = 6, 6 + 10 = 16
    assert isinstance(outcome, pyergon.Completed)
    assert outcome.result == 16


@pytest.mark.asyncio
async def test_executor_with_custom_id():
    """Test flow execution with a custom flow ID."""
    storage = pyergon.InMemoryExecutionLog()
    workflow = SimpleWorkflow()
    custom_id = "test-flow-12345"

    executor = pyergon.Executor(workflow, storage, custom_id)
    assert executor.flow_id == custom_id

    outcome = await executor.run(lambda w: w.run(7))

    assert isinstance(outcome, pyergon.Completed)
    assert outcome.result == 24  # 7 * 2 = 14, 14 + 10 = 24


@dataclass
@pyergon.flow_type
class MultiStepWorkflow:
    """Workflow with multiple steps for testing."""

    @pyergon.step
    async def fetch_data(self) -> dict:
        """Simulate fetching data."""
        await asyncio.sleep(0.01)  # Simulate async I/O
        return {"status": "ok", "value": 42}

    @pyergon.step
    async def process_data(self, data: dict) -> int:
        """Process the fetched data."""
        return data["value"] * 2

    @pyergon.step
    async def format_result(self, value: int) -> str:
        """Format the result."""
        return f"Result: {value}"

    @pyergon.flow
    async def run(self) -> str:
        """Execute the full workflow."""
        data = await self.fetch_data()
        processed = await self.process_data(data)
        formatted = await self.format_result(processed)
        return formatted


@pytest.mark.asyncio
async def test_multi_step_workflow():
    """Test a workflow with multiple chained steps."""
    storage = pyergon.InMemoryExecutionLog()
    workflow = MultiStepWorkflow()

    executor = pyergon.Executor(workflow, storage, "multi-step-test")
    outcome = await executor.run(lambda w: w.run())

    assert isinstance(outcome, pyergon.Completed)
    assert outcome.result == "Result: 84"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
