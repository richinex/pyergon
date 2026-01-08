"""Edge case tests for decorator system."""

import asyncio
from dataclasses import dataclass

import pytest

import pyergon
from pyergon.decorators import flow, flow_type, step


@dataclass
@flow_type
class WorkflowWithDependencies:
    """Workflow testing step dependencies."""

    @step
    async def step_a(self) -> int:
        """Independent step."""
        return 10

    @step
    async def step_b(self) -> int:
        """Another independent step."""
        return 20

    @step(depends_on="step_a")
    async def step_c(self, value: int) -> int:
        """Depends on step_a."""
        return value + 5

    @step(depends_on=["step_a", "step_b"])
    async def step_d(self, a_val: int, b_val: int) -> int:
        """Depends on multiple steps."""
        return a_val + b_val

    @flow
    async def run(self) -> int:
        a = await self.step_a()
        b = await self.step_b()
        c = await self.step_c(a)
        d = await self.step_d(a, b)
        return c + d


@dataclass
@flow_type
class WorkflowWithInputs:
    """Workflow testing step inputs parameter."""

    @step
    async def fetch_user(self) -> dict:
        return {"id": 123, "name": "Alice"}

    @step
    async def fetch_product(self) -> dict:
        return {"id": 456, "name": "Widget"}

    @step(inputs={"user_data": "fetch_user", "product_data": "fetch_product"})
    async def combine_data(self, user_data: dict, product_data: dict) -> str:
        return f"User {user_data['name']} ordered {product_data['name']}"

    @flow
    async def run(self) -> str:
        user = await self.fetch_user()
        product = await self.fetch_product()
        result = await self.combine_data(user, product)
        return result


@dataclass
@flow_type
class WorkflowWithErrors:
    """Workflow that raises errors."""

    @step
    async def failing_step(self) -> int:
        """Step that always fails."""
        raise ValueError("Intentional error")

    @step
    async def successful_step(self) -> int:
        """Step that succeeds."""
        return 42

    @flow
    async def run_with_error(self) -> int:
        """Flow that fails."""
        return await self.failing_step()

    @flow
    async def run_success(self) -> int:
        """Flow that succeeds."""
        return await self.successful_step()


@dataclass
@flow_type
class WorkflowWithEmptyDependsOn:
    """Workflow with explicit empty depends_on."""

    @step(depends_on=[])
    async def independent_step(self) -> int:
        """Step with explicit empty dependencies."""
        return 100

    @flow
    async def run(self) -> int:
        return await self.independent_step()


@pytest.mark.asyncio
async def test_step_with_string_depends_on():
    """Test step with single string depends_on."""
    storage = pyergon.InMemoryExecutionLog()
    workflow = WorkflowWithDependencies()

    executor = pyergon.Executor(workflow, storage, "test-depends-1")
    outcome = await executor.run(lambda w: w.run())

    assert isinstance(outcome, pyergon.Completed)
    # step_a=10, step_c=10+5=15, step_b=20, step_d=10+20=30, total=15+30=45
    assert outcome.result == 45


@pytest.mark.asyncio
async def test_step_with_list_depends_on():
    """Test step with list depends_on."""
    storage = pyergon.InMemoryExecutionLog()
    workflow = WorkflowWithDependencies()

    executor = pyergon.Executor(workflow, storage, "test-depends-2")
    outcome = await executor.run(lambda w: w.run())

    assert isinstance(outcome, pyergon.Completed)
    assert outcome.result == 45


@pytest.mark.asyncio
async def test_step_with_inputs():
    """Test step with inputs parameter."""
    storage = pyergon.InMemoryExecutionLog()
    workflow = WorkflowWithInputs()

    executor = pyergon.Executor(workflow, storage, "test-inputs-1")
    outcome = await executor.run(lambda w: w.run())

    assert isinstance(outcome, pyergon.Completed)
    assert "Alice" in outcome.result
    assert "Widget" in outcome.result


@pytest.mark.asyncio
async def test_step_with_empty_depends_on():
    """Test step with explicit empty depends_on list."""
    storage = pyergon.InMemoryExecutionLog()
    workflow = WorkflowWithEmptyDependsOn()

    executor = pyergon.Executor(workflow, storage, "test-empty-deps")
    outcome = await executor.run(lambda w: w.run())

    assert isinstance(outcome, pyergon.Completed)
    assert outcome.result == 100


@pytest.mark.asyncio
async def test_flow_with_error():
    """Test flow that raises an error."""
    storage = pyergon.InMemoryExecutionLog()
    workflow = WorkflowWithErrors()

    executor = pyergon.Executor(workflow, storage, "test-error-1")
    outcome = await executor.run(lambda w: w.run_with_error())

    assert isinstance(outcome, pyergon.Completed)
    # Error is stored as result
    assert isinstance(outcome.result, Exception)
    assert "Intentional error" in str(outcome.result)


@pytest.mark.asyncio
async def test_flow_success_after_error():
    """Test successful flow execution on same workflow instance."""
    storage = pyergon.InMemoryExecutionLog()
    workflow = WorkflowWithErrors()

    # First run succeeds
    executor1 = pyergon.Executor(workflow, storage, "test-success-1")
    outcome1 = await executor1.run(lambda w: w.run_success())

    assert isinstance(outcome1, pyergon.Completed)
    assert outcome1.result == 42


@pytest.mark.asyncio
async def test_step_caching():
    """Test that steps are cached on replay."""
    storage = pyergon.InMemoryExecutionLog()

    @dataclass
    @flow_type
    class CachedWorkflow:
        execution_count: int = 0

        @step
        async def counted_step(self) -> int:
            self.execution_count += 1
            return 100

        @flow
        async def run(self) -> int:
            return await self.counted_step()

    workflow = CachedWorkflow()

    # First execution
    executor1 = pyergon.Executor(workflow, storage, "cache-test")
    outcome1 = await executor1.run(lambda w: w.run())

    assert outcome1.result == 100
    assert workflow.execution_count == 1

    # Second execution with same storage should use cache
    workflow2 = CachedWorkflow()
    executor2 = pyergon.Executor(workflow2, storage, "cache-test")
    outcome2 = await executor2.run(lambda w: w.run())

    assert outcome2.result == 100
    # Step should not execute again (cached)
    assert workflow2.execution_count == 0


@pytest.mark.asyncio
async def test_step_metadata_preserved():
    """Test that step decorator preserves function metadata."""

    @step
    async def documented_step() -> str:
        """This is a documented step."""
        return "result"

    assert documented_step.__doc__ == "This is a documented step."
    assert documented_step.__name__ == "documented_step"
    assert hasattr(documented_step, "_is_ergon_step")


@pytest.mark.asyncio
async def test_flow_type_collects_all_steps():
    """Test that flow_type decorator collects all @step methods."""

    @dataclass
    @flow_type
    class MultiStepFlow:
        @step
        async def step1(self):
            pass

        @step
        async def step2(self):
            pass

        @step
        async def step3(self):
            pass

        @flow
        async def run(self):
            pass

        async def non_step_method(self):
            """Regular method, not a step."""
            pass

    assert hasattr(MultiStepFlow, "_ergon_steps")
    assert "step1" in MultiStepFlow._ergon_steps
    assert "step2" in MultiStepFlow._ergon_steps
    assert "step3" in MultiStepFlow._ergon_steps
    assert "run" not in MultiStepFlow._ergon_steps
    assert "non_step_method" not in MultiStepFlow._ergon_steps


@pytest.mark.asyncio
async def test_nested_async_calls():
    """Test workflow with nested async calls."""

    @dataclass
    @flow_type
    class NestedWorkflow:
        @step
        async def outer_step(self) -> int:
            """Outer step that calls helper."""
            result = await self._helper_function()
            return result * 2

        async def _helper_function(self) -> int:
            """Helper function (not a step)."""
            await asyncio.sleep(0.001)
            return 21

        @flow
        async def run(self) -> int:
            return await self.outer_step()

    storage = pyergon.InMemoryExecutionLog()
    workflow = NestedWorkflow()

    executor = pyergon.Executor(workflow, storage, "nested-test")
    outcome = await executor.run(lambda w: w.run())

    assert isinstance(outcome, pyergon.Completed)
    assert outcome.result == 42


@pytest.mark.asyncio
async def test_step_with_no_parameters():
    """Test step that takes no parameters besides self."""

    @dataclass
    @flow_type
    class SimpleFlow:
        @step
        async def no_params_step(self) -> str:
            """Step with no parameters."""
            return "success"

        @flow
        async def run(self) -> str:
            return await self.no_params_step()

    storage = pyergon.InMemoryExecutionLog()
    workflow = SimpleFlow()

    executor = pyergon.Executor(workflow, storage, "no-params-test")
    outcome = await executor.run(lambda w: w.run())

    assert isinstance(outcome, pyergon.Completed)
    assert outcome.result == "success"


@pytest.mark.asyncio
async def test_step_with_complex_return_types():
    """Test step with complex return types (dict, list, nested)."""

    @dataclass
    @flow_type
    class ComplexReturnFlow:
        @step
        async def return_dict(self) -> dict:
            return {"key": "value", "number": 42, "nested": {"inner": "data"}}

        @step
        async def return_list(self) -> list:
            return [1, 2, 3, "four", {"five": 5}]

        @step
        async def process_complex(self, data: dict) -> int:
            return data["number"] * 2

        @flow
        async def run(self) -> int:
            data = await self.return_dict()
            await self.return_list()
            return await self.process_complex(data)

    storage = pyergon.InMemoryExecutionLog()
    workflow = ComplexReturnFlow()

    executor = pyergon.Executor(workflow, storage, "complex-test")
    outcome = await executor.run(lambda w: w.run())

    assert isinstance(outcome, pyergon.Completed)
    assert outcome.result == 84
