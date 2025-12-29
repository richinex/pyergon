"""
Tests for Phase 3 decorator system.

Tests @flow and @step decorators and FlowContext execution.
"""

import pytest
import ergon
import asyncio


@ergon.flow
class SimpleWorkflow:
    """A simple workflow for testing."""

    def __init__(self):
        self.execution_log = []

    @ergon.step
    async def step_one(self, value: int) -> int:
        """First step: multiply by 2."""
        self.execution_log.append(f"step_one({value})")
        return value * 2

    @ergon.step
    async def step_two(self, value: int) -> int:
        """Second step: add 10."""
        self.execution_log.append(f"step_two({value})")
        return value + 10

    async def run(self, initial_value: int) -> int:
        """Main flow: chain the steps."""
        result1 = await self.step_one(initial_value)
        result2 = await self.step_two(result1)
        return result2


def test_step_decorator():
    """Test that @step decorator adds metadata."""
    @ergon.step
    async def test_step():
        pass

    assert hasattr(test_step, '_is_ergon_step')
    assert test_step._is_ergon_step is True
    assert test_step._step_name == 'test_step'


def test_flow_decorator():
    """Test that @flow decorator adds metadata."""
    @ergon.flow
    class TestFlow:
        @ergon.step
        async def step_a(self):
            pass

        @ergon.step
        async def step_b(self):
            pass

        async def run(self):
            pass

    assert hasattr(TestFlow, '_is_ergon_flow')
    assert TestFlow._is_ergon_flow is True
    assert hasattr(TestFlow, '_ergon_steps')
    assert 'step_a' in TestFlow._ergon_steps
    assert 'step_b' in TestFlow._ergon_steps
    assert 'run' not in TestFlow._ergon_steps  # run is not a step


@pytest.mark.asyncio
async def test_flow_context_execution():
    """Test basic flow execution with FlowContext."""
    storage = ergon.InMemoryExecutionLog()
    workflow = SimpleWorkflow()

    async with ergon.FlowContext(storage) as ctx:
        result = await ctx.execute_flow(workflow.run, 5)

    # 5 * 2 = 10, 10 + 10 = 20
    assert result == 20


@pytest.mark.asyncio
async def test_execute_with_context():
    """Test the execute_with_context helper."""
    storage = ergon.InMemoryExecutionLog()
    workflow = SimpleWorkflow()

    ctx = await ergon.execute_with_context(workflow, storage)
    result = await ctx.execute_flow(workflow.run, 5)

    assert result == 20
    # Both steps should have executed
    assert len(workflow.execution_log) == 2
    assert "step_one(5)" in workflow.execution_log
    assert "step_two(10)" in workflow.execution_log


@pytest.mark.asyncio
async def test_flow_context_with_sqlite():
    """Test flow execution with SQLite storage."""
    storage = ergon.SqliteExecutionLog.in_memory()
    workflow = SimpleWorkflow()

    async with ergon.FlowContext(storage) as ctx:
        result = await ctx.execute_flow(workflow.run, 3)

    # 3 * 2 = 6, 6 + 10 = 16
    assert result == 16


@pytest.mark.asyncio
async def test_flow_with_custom_id():
    """Test flow execution with a custom flow ID."""
    storage = ergon.InMemoryExecutionLog()
    workflow = SimpleWorkflow()
    custom_id = "test-flow-12345"

    async with ergon.FlowContext(storage, flow_id=custom_id) as ctx:
        assert ctx.flow_id == custom_id
        result = await ctx.execute_flow(workflow.run, 7)

    assert result == 24  # 7 * 2 = 14, 14 + 10 = 24


@ergon.flow
class MultiStepWorkflow:
    """Workflow with multiple steps for testing."""

    @ergon.step
    async def fetch_data(self) -> dict:
        """Simulate fetching data."""
        await asyncio.sleep(0.01)  # Simulate async I/O
        return {"status": "ok", "value": 42}

    @ergon.step
    async def process_data(self, data: dict) -> int:
        """Process the fetched data."""
        return data["value"] * 2

    @ergon.step
    async def format_result(self, value: int) -> str:
        """Format the result."""
        return f"Result: {value}"

    async def run(self) -> str:
        """Execute the full workflow."""
        data = await self.fetch_data()
        processed = await self.process_data(data)
        formatted = await self.format_result(processed)
        return formatted


@pytest.mark.asyncio
async def test_multi_step_workflow():
    """Test a workflow with multiple chained steps."""
    storage = ergon.InMemoryExecutionLog()
    workflow = MultiStepWorkflow()

    ctx = await ergon.execute_with_context(workflow, storage)
    result = await ctx.execute_flow(workflow.run)

    assert result == "Result: 84"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
