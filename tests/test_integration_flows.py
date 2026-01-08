"""Integration tests for end-to-end flow execution."""

import asyncio
from dataclasses import dataclass

import pytest

from pyergon.decorators import flow, flow_type, step
from pyergon.executor import Executor
from pyergon.storage.memory import InMemoryExecutionLog
from pyergon.storage.sqlite import SqliteExecutionLog


@dataclass
@flow_type
class MultiStepIntegrationFlow:
    """Flow with multiple steps for integration testing."""

    results: list = None

    def __post_init__(self):
        if self.results is None:
            self.results = []

    @step
    async def initialize(self) -> dict:
        """Initialize with configuration."""
        self.results.append("init")
        return {"config": "loaded", "version": "1.0"}

    @step
    async def process_data(self, config: dict) -> list:
        """Process data based on configuration."""
        self.results.append(f"process:{config['version']}")
        await asyncio.sleep(0.001)  # Simulate async work
        return [1, 2, 3, 4, 5]

    @step
    async def aggregate(self, data: list) -> int:
        """Aggregate results."""
        self.results.append(f"aggregate:{len(data)}")
        return sum(data)

    @step
    async def format_result(self, total: int) -> str:
        """Format final result."""
        self.results.append(f"format:{total}")
        return f"Total: {total}"

    @flow
    async def run(self) -> str:
        config = await self.initialize()
        data = await self.process_data(config)
        total = await self.aggregate(data)
        result = await self.format_result(total)
        return result


@dataclass
@flow_type
class ErrorHandlingFlow:
    """Flow that tests error handling."""

    attempt_count: int = 0

    @step
    async def might_fail(self, fail: bool) -> str:
        """Step that might fail based on input."""
        self.attempt_count += 1
        if fail:
            raise ValueError(f"Failed on attempt {self.attempt_count}")
        return "success"

    @step
    async def always_succeeds(self) -> str:
        """Step that always succeeds."""
        return "done"

    @flow
    async def run_with_failure(self) -> str:
        """Run with expected failure."""
        return await self.might_fail(fail=True)

    @flow
    async def run_with_success(self) -> str:
        """Run with success."""
        result = await self.might_fail(fail=False)
        final = await self.always_succeeds()
        return f"{result}-{final}"


@dataclass
@flow_type
class ConditionalFlow:
    """Flow with conditional execution paths."""

    @step
    async def check_condition(self, value: int) -> bool:
        """Check a condition."""
        return value > 10

    @step
    async def path_a(self) -> str:
        """Execute path A."""
        return "path_a"

    @step
    async def path_b(self) -> str:
        """Execute path B."""
        return "path_b"

    @flow
    async def run(self, value: int) -> str:
        """Run with conditional logic."""
        condition = await self.check_condition(value)
        if condition:
            return await self.path_a()
        else:
            return await self.path_b()


@dataclass
@flow_type
class NestedAsyncFlow:
    """Flow with nested async operations."""

    @step
    async def parallel_fetch(self) -> list:
        """Simulate parallel data fetching."""

        async def fetch_user():
            await asyncio.sleep(0.001)
            return {"id": 1, "name": "Alice"}

        async def fetch_orders():
            await asyncio.sleep(0.001)
            return [{"order_id": 101}, {"order_id": 102}]

        user, orders = await asyncio.gather(fetch_user(), fetch_orders())
        return [user, orders]

    @step
    async def process_results(self, data: list) -> dict:
        """Process fetched results."""
        user, orders = data
        return {"user": user["name"], "order_count": len(orders)}

    @flow
    async def run(self) -> dict:
        data = await self.parallel_fetch()
        result = await self.process_results(data)
        return result


@pytest.mark.asyncio
async def test_multi_step_integration_memory():
    """Test multi-step flow with in-memory storage."""
    storage = InMemoryExecutionLog()
    workflow = MultiStepIntegrationFlow()

    executor = Executor(workflow, storage, "integration-test-1")
    outcome = await executor.run(lambda w: w.run())

    assert outcome.result == "Total: 15"
    assert len(workflow.results) == 4
    assert "init" in workflow.results
    assert "aggregate:5" in workflow.results


@pytest.mark.asyncio
async def test_multi_step_integration_sqlite():
    """Test multi-step flow with SQLite storage."""
    storage = await SqliteExecutionLog.in_memory()
    workflow = MultiStepIntegrationFlow()

    executor = Executor(workflow, storage, "integration-test-2")
    outcome = await executor.run(lambda w: w.run())

    assert outcome.result == "Total: 15"
    assert len(workflow.results) == 4


@pytest.mark.asyncio
async def test_error_handling_flow_with_failure():
    """Test flow that encounters an error."""
    storage = InMemoryExecutionLog()
    workflow = ErrorHandlingFlow()

    executor = Executor(workflow, storage, "error-test-1")
    outcome = await executor.run(lambda w: w.run_with_failure())

    # Error is captured as result
    assert isinstance(outcome.result, Exception)
    assert "Failed on attempt" in str(outcome.result)


@pytest.mark.asyncio
async def test_error_handling_flow_with_success():
    """Test flow that completes successfully."""
    storage = InMemoryExecutionLog()
    workflow = ErrorHandlingFlow()

    executor = Executor(workflow, storage, "error-test-2")
    outcome = await executor.run(lambda w: w.run_with_success())

    assert outcome.result == "success-done"
    assert workflow.attempt_count == 1


@pytest.mark.asyncio
async def test_conditional_flow_path_a():
    """Test conditional flow taking path A."""
    storage = InMemoryExecutionLog()
    workflow = ConditionalFlow()

    executor = Executor(workflow, storage, "conditional-test-1")
    outcome = await executor.run(lambda w: w.run(15))

    assert outcome.result == "path_a"


@pytest.mark.asyncio
async def test_conditional_flow_path_b():
    """Test conditional flow taking path B."""
    storage = InMemoryExecutionLog()
    workflow = ConditionalFlow()

    executor = Executor(workflow, storage, "conditional-test-2")
    outcome = await executor.run(lambda w: w.run(5))

    assert outcome.result == "path_b"


@pytest.mark.asyncio
async def test_nested_async_flow():
    """Test flow with nested async operations."""
    storage = InMemoryExecutionLog()
    workflow = NestedAsyncFlow()

    executor = Executor(workflow, storage, "nested-async-test")
    outcome = await executor.run(lambda w: w.run())

    assert outcome.result["user"] == "Alice"
    assert outcome.result["order_count"] == 2


@pytest.mark.asyncio
async def test_flow_replay_with_cache():
    """Test flow replay uses cached results."""
    storage = InMemoryExecutionLog()

    # First execution
    workflow1 = MultiStepIntegrationFlow()
    executor1 = Executor(workflow1, storage, "replay-test")
    outcome1 = await executor1.run(lambda w: w.run())

    assert outcome1.result == "Total: 15"
    assert len(workflow1.results) == 4

    # Second execution (replay with cache)
    workflow2 = MultiStepIntegrationFlow()
    executor2 = Executor(workflow2, storage, "replay-test")
    outcome2 = await executor2.run(lambda w: w.run())

    assert outcome2.result == "Total: 15"
    # Steps should not execute again (cached)
    assert len(workflow2.results) == 0


@pytest.mark.asyncio
async def test_multiple_flows_concurrent():
    """Test multiple flows executing concurrently."""
    storage = InMemoryExecutionLog()

    async def run_flow(flow_id: str, value: int):
        workflow = ConditionalFlow()
        executor = Executor(workflow, storage, flow_id)
        outcome = await executor.run(lambda w: w.run(value))
        return outcome.result

    # Run multiple flows concurrently
    results = await asyncio.gather(
        run_flow("concurrent-1", 15),
        run_flow("concurrent-2", 5),
        run_flow("concurrent-3", 20),
        run_flow("concurrent-4", 3),
    )

    assert results == ["path_a", "path_b", "path_a", "path_b"]


@pytest.mark.asyncio
async def test_flow_with_complex_data_types():
    """Test flow handling complex nested data structures."""

    @dataclass
    @flow_type
    class ComplexDataFlow:
        @step
        async def create_nested_data(self) -> dict:
            return {
                "users": [
                    {"id": 1, "name": "Alice", "tags": ["admin", "active"]},
                    {"id": 2, "name": "Bob", "tags": ["user"]},
                ],
                "metadata": {"version": 2, "created": "2024-01-01"},
            }

        @step
        async def process_nested(self, data: dict) -> int:
            user_count = len(data["users"])
            tag_count = sum(len(u["tags"]) for u in data["users"])
            return user_count + tag_count

        @flow
        async def run(self) -> int:
            data = await self.create_nested_data()
            return await self.process_nested(data)

    storage = InMemoryExecutionLog()
    workflow = ComplexDataFlow()

    executor = Executor(workflow, storage, "complex-data-test")
    outcome = await executor.run(lambda w: w.run())

    # 2 users + 3 tags = 5
    assert outcome.result == 5


@pytest.mark.asyncio
async def test_flow_with_none_return_values():
    """Test flow handling None return values."""

    @dataclass
    @flow_type
    class NoneReturnFlow:
        @step
        async def returns_none(self) -> None:
            """Step that returns None."""
            return None

        @step
        async def handles_none(self, value: None) -> str:
            """Step that handles None input."""
            return "handled_none" if value is None else "not_none"

        @flow
        async def run(self) -> str:
            none_value = await self.returns_none()
            return await self.handles_none(none_value)

    storage = InMemoryExecutionLog()
    workflow = NoneReturnFlow()

    executor = Executor(workflow, storage, "none-return-test")
    outcome = await executor.run(lambda w: w.run())

    assert outcome.result == "handled_none"


@pytest.mark.asyncio
async def test_long_running_flow():
    """Test flow with multiple async delays."""

    @dataclass
    @flow_type
    class LongRunningFlow:
        @step
        async def step1(self) -> str:
            await asyncio.sleep(0.001)
            return "step1"

        @step
        async def step2(self, prev: str) -> str:
            await asyncio.sleep(0.001)
            return f"{prev}->step2"

        @step
        async def step3(self, prev: str) -> str:
            await asyncio.sleep(0.001)
            return f"{prev}->step3"

        @flow
        async def run(self) -> str:
            r1 = await self.step1()
            r2 = await self.step2(r1)
            r3 = await self.step3(r2)
            return r3

    storage = InMemoryExecutionLog()
    workflow = LongRunningFlow()

    executor = Executor(workflow, storage, "long-running-test")
    outcome = await executor.run(lambda w: w.run())

    assert outcome.result == "step1->step2->step3"
