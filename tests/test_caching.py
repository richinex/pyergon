"""
Tests for Phase 4 caching and storage integration.

Tests result serialization, caching, and step persistence.
"""

import pytest
import ergon
import asyncio


@ergon.flow
class CachingTestWorkflow:
    """Workflow for testing caching behavior."""

    def __init__(self):
        self.execution_count = {}

    @ergon.step
    async def expensive_operation(self, value: int) -> int:
        """Simulate an expensive operation that should be cached."""
        # Track how many times this step executes
        if 'expensive_operation' not in self.execution_count:
            self.execution_count['expensive_operation'] = 0
        self.execution_count['expensive_operation'] += 1

        # Simulate expensive work
        await asyncio.sleep(0.01)
        return value * 10

    @ergon.step
    async def another_step(self, value: int) -> int:
        """Another step for testing multiple step tracking."""
        if 'another_step' not in self.execution_count:
            self.execution_count['another_step'] = 0
        self.execution_count['another_step'] += 1

        return value + 5

    async def run(self, initial_value: int) -> int:
        """Execute the workflow."""
        result1 = await self.expensive_operation(initial_value)
        result2 = await self.another_step(result1)
        return result2


@pytest.mark.asyncio
async def test_step_logging_to_storage():
    """Test that steps are logged to storage."""
    storage = ergon.InMemoryExecutionLog()
    workflow = CachingTestWorkflow()

    ctx = await ergon.execute_with_context(workflow, storage)
    result = await ctx.execute_flow(workflow.run, 5)

    assert result == 55  # (5 * 10) + 5

    # Verify steps were logged
    invocations = await storage.get_invocations_for_flow(ctx.flow_id)
    assert len(invocations) == 2

    # Check first step
    assert invocations[0].step == 0
    assert invocations[0].method_name == "expensive_operation"
    assert invocations[0].status.is_complete

    # Check second step
    assert invocations[1].step == 1
    assert invocations[1].method_name == "another_step"
    assert invocations[1].status.is_complete


@pytest.mark.asyncio
async def test_step_execution_tracking():
    """Test that step execution is properly tracked."""
    storage = ergon.SqliteExecutionLog.in_memory()
    workflow = CachingTestWorkflow()

    ctx = await ergon.execute_with_context(workflow, storage)
    await ctx.execute_flow(workflow.run, 7)

    # Each step should execute exactly once
    assert workflow.execution_count['expensive_operation'] == 1
    assert workflow.execution_count['another_step'] == 1


@pytest.mark.asyncio
async def test_multiple_flows_separate_storage():
    """Test that multiple flows maintain separate execution logs."""
    storage = ergon.InMemoryExecutionLog()

    # Run first workflow
    workflow1 = CachingTestWorkflow()
    ctx1 = await ergon.execute_with_context(workflow1, storage)
    result1 = await ctx1.execute_flow(workflow1.run, 3)

    # Run second workflow
    workflow2 = CachingTestWorkflow()
    ctx2 = await ergon.execute_with_context(workflow2, storage)
    result2 = await ctx2.execute_flow(workflow2.run, 4)

    assert result1 == 35  # (3 * 10) + 5
    assert result2 == 45  # (4 * 10) + 5

    # Verify separate flows
    invocations1 = await storage.get_invocations_for_flow(ctx1.flow_id)
    invocations2 = await storage.get_invocations_for_flow(ctx2.flow_id)

    assert len(invocations1) == 2
    assert len(invocations2) == 2
    assert ctx1.flow_id != ctx2.flow_id


@ergon.flow
class ComplexWorkflow:
    """Workflow with more complex data types."""

    @ergon.step
    async def process_dict(self, data: dict) -> dict:
        """Process a dictionary."""
        return {**data, "processed": True, "count": len(data)}

    @ergon.step
    async def process_list(self, items: list) -> list:
        """Process a list."""
        return [item * 2 for item in items]

    async def run(self, data: dict, items: list) -> tuple:
        """Execute with complex types."""
        processed_dict = await self.process_dict(data)
        processed_list = await self.process_list(items)
        return (processed_dict, processed_list)


@pytest.mark.asyncio
async def test_complex_data_types():
    """Test serialization with complex data types."""
    storage = ergon.InMemoryExecutionLog()
    workflow = ComplexWorkflow()

    ctx = await ergon.execute_with_context(workflow, storage)
    result = await ctx.execute_flow(
        workflow.run,
        {"name": "test", "value": 42},
        [1, 2, 3]
    )

    processed_dict, processed_list = result

    assert processed_dict == {"name": "test", "value": 42, "processed": True, "count": 2}
    assert processed_list == [2, 4, 6]

    # Verify storage
    invocations = await storage.get_invocations_for_flow(ctx.flow_id)
    assert len(invocations) == 2


@pytest.mark.asyncio
async def test_sqlite_persistence():
    """Test that SQLite storage persists data."""
    storage = ergon.SqliteExecutionLog.in_memory()
    workflow = CachingTestWorkflow()

    # Execute workflow
    ctx = await ergon.execute_with_context(workflow, storage)
    flow_id = ctx.flow_id
    result = await ctx.execute_flow(workflow.run, 9)

    assert result == 95  # (9 * 10) + 5

    # Verify we can retrieve invocations after execution
    invocations = await storage.get_invocations_for_flow(flow_id)
    assert len(invocations) == 2

    # Verify invocation details
    first_inv = invocations[0]
    assert first_inv.class_name == "CachingTestWorkflow"
    assert first_inv.method_name == "expensive_operation"
    assert first_inv.attempts == 1


@pytest.mark.asyncio
async def test_flow_context_state():
    """Test that FlowContext maintains proper state."""
    storage = ergon.InMemoryExecutionLog()
    workflow = CachingTestWorkflow()

    ctx = await ergon.execute_with_context(workflow, storage)

    # Check initial state
    assert ctx.current_step == 0
    assert ctx.flow_id is not None

    # Execute
    await ctx.execute_flow(workflow.run, 2)

    # Check final state
    assert ctx.current_step == 2  # Should have incremented for 2 steps


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
