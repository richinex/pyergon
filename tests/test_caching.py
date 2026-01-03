"""
Tests for Phase 4 caching and storage integration.

Tests result serialization, caching, and step persistence.
"""

import asyncio
from dataclasses import dataclass

import pytest

import pyergon


@dataclass
@pyergon.flow_type
class CachingTestWorkflow:
    """Workflow for testing caching behavior."""

    def __post_init__(self):
        self.execution_count = {}

    @pyergon.step
    async def expensive_operation(self, value: int) -> int:
        """Simulate an expensive operation that should be cached."""
        # Track how many times this step executes
        if "expensive_operation" not in self.execution_count:
            self.execution_count["expensive_operation"] = 0
        self.execution_count["expensive_operation"] += 1

        # Simulate expensive work
        await asyncio.sleep(0.01)
        return value * 10

    @pyergon.step
    async def another_step(self, value: int) -> int:
        """Another step for testing multiple step tracking."""
        if "another_step" not in self.execution_count:
            self.execution_count["another_step"] = 0
        self.execution_count["another_step"] += 1

        return value + 5

    @pyergon.flow
    async def run(self, initial_value: int) -> int:
        """Execute the workflow."""
        result1 = await self.expensive_operation(initial_value)
        result2 = await self.another_step(result1)
        return result2


@pytest.mark.asyncio
async def test_step_logging_to_storage():
    """Test that steps are logged to storage."""
    storage = pyergon.InMemoryExecutionLog()
    workflow = CachingTestWorkflow()

    executor = pyergon.Executor(workflow, storage, "caching-test-1")
    outcome = await executor.run(lambda w: w.run(5))

    assert isinstance(outcome, pyergon.Completed)
    assert outcome.result == 55  # (5 * 10) + 5

    # Verify steps were logged (flow entry + 2 steps = 3 invocations)
    invocations = await storage.get_invocations_for_flow(executor.flow_id)
    assert len(invocations) == 3

    # Check flow entry
    assert invocations[0].step == 0
    assert invocations[0].method_name == "run"
    assert invocations[0].status == pyergon.InvocationStatus.COMPLETE

    # Check first step (expensive_operation)
    step_methods = {inv.method_name for inv in invocations}
    assert "expensive_operation" in step_methods
    assert "another_step" in step_methods

    # All should be complete
    assert all(inv.status == pyergon.InvocationStatus.COMPLETE for inv in invocations)


@pytest.mark.asyncio
async def test_step_execution_tracking():
    """Test that step execution is properly tracked."""
    storage = await pyergon.SqliteExecutionLog.in_memory()
    workflow = CachingTestWorkflow()

    executor = pyergon.Executor(workflow, storage, "caching-test-2")
    outcome = await executor.run(lambda w: w.run(7))

    # Each step should execute exactly once
    assert isinstance(outcome, pyergon.Completed)
    assert workflow.execution_count["expensive_operation"] == 1
    assert workflow.execution_count["another_step"] == 1


@pytest.mark.asyncio
async def test_multiple_flows_separate_storage():
    """Test that multiple flows maintain separate execution logs."""
    storage = pyergon.InMemoryExecutionLog()

    # Run first workflow
    workflow1 = CachingTestWorkflow()
    executor1 = pyergon.Executor(workflow1, storage, "multi-flow-1")
    outcome1 = await executor1.run(lambda w: w.run(3))

    # Run second workflow
    workflow2 = CachingTestWorkflow()
    executor2 = pyergon.Executor(workflow2, storage, "multi-flow-2")
    outcome2 = await executor2.run(lambda w: w.run(4))

    assert isinstance(outcome1, pyergon.Completed)
    assert isinstance(outcome2, pyergon.Completed)
    assert outcome1.result == 35  # (3 * 10) + 5
    assert outcome2.result == 45  # (4 * 10) + 5

    # Verify separate flows (flow entry + 2 steps each = 3 invocations)
    invocations1 = await storage.get_invocations_for_flow(executor1.flow_id)
    invocations2 = await storage.get_invocations_for_flow(executor2.flow_id)

    assert len(invocations1) == 3
    assert len(invocations2) == 3
    assert executor1.flow_id != executor2.flow_id


@dataclass
@pyergon.flow_type
class ComplexWorkflow:
    """Workflow with more complex data types."""

    @pyergon.step
    async def process_dict(self, data: dict) -> dict:
        """Process a dictionary."""
        return {**data, "processed": True, "count": len(data)}

    @pyergon.step
    async def process_list(self, items: list) -> list:
        """Process a list."""
        return [item * 2 for item in items]

    @pyergon.flow
    async def run(self, data: dict, items: list) -> tuple:
        """Execute with complex types."""
        processed_dict = await self.process_dict(data)
        processed_list = await self.process_list(items)
        return (processed_dict, processed_list)


@pytest.mark.asyncio
async def test_complex_data_types():
    """Test serialization with complex data types."""
    storage = pyergon.InMemoryExecutionLog()
    workflow = ComplexWorkflow()

    executor = pyergon.Executor(workflow, storage, "complex-data-test")
    outcome = await executor.run(lambda w: w.run({"name": "test", "value": 42}, [1, 2, 3]))

    assert isinstance(outcome, pyergon.Completed)
    processed_dict, processed_list = outcome.result

    assert processed_dict == {"name": "test", "value": 42, "processed": True, "count": 2}
    assert processed_list == [2, 4, 6]

    # Verify storage (flow entry + 2 steps = 3 invocations)
    invocations = await storage.get_invocations_for_flow(executor.flow_id)
    assert len(invocations) == 3


@pytest.mark.asyncio
async def test_sqlite_persistence():
    """Test that SQLite storage persists data."""
    storage = await pyergon.SqliteExecutionLog.in_memory()
    workflow = CachingTestWorkflow()

    # Execute workflow
    executor = pyergon.Executor(workflow, storage, "sqlite-persist-test")
    outcome = await executor.run(lambda w: w.run(9))

    assert isinstance(outcome, pyergon.Completed)
    assert outcome.result == 95  # (9 * 10) + 5

    # Verify we can retrieve invocations after execution (flow entry + 2 steps = 3)
    invocations = await storage.get_invocations_for_flow(executor.flow_id)
    assert len(invocations) == 3

    # Verify invocation details - find the expensive_operation step
    expensive_inv = next(
        (inv for inv in invocations if inv.method_name == "expensive_operation"), None
    )
    assert expensive_inv is not None
    assert expensive_inv.class_name == "CachingTestWorkflow"
    assert expensive_inv.attempts == 1


@pytest.mark.asyncio
async def test_executor_state():
    """Test that Executor maintains proper state."""
    storage = pyergon.InMemoryExecutionLog()
    workflow = CachingTestWorkflow()

    executor = pyergon.Executor(workflow, storage, "executor-state-test")

    # Check initial state
    assert executor.flow_id is not None
    assert executor.flow_id == "executor-state-test"

    # Execute
    outcome = await executor.run(lambda w: w.run(2))

    assert isinstance(outcome, pyergon.Completed)
    assert outcome.result == 25  # (2 * 10) + 5

    # Verify invocations were logged (flow entry + 2 steps = 3 invocations)
    invocations = await storage.get_invocations_for_flow(executor.flow_id)
    assert len(invocations) == 3


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
