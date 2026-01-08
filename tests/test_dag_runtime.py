"""Tests for runtime DAG execution using @step decorators."""

import pytest

from pyergon.decorators import flow, flow_type, step
from pyergon.executor.dag_runtime import DagExecutionError, dag


@pytest.mark.asyncio
async def test_dag_runtime_basic():
    """Test basic DAG runtime execution."""

    @flow_type
    class BasicDagFlow:
        @step
        async def step_a(self):
            return 10

        @step
        async def step_b(self):
            return 20

        @flow
        async def run(self):
            return await dag(self)

    flow_instance = BasicDagFlow()
    result = await flow_instance.run()

    # Should return last step's result
    assert result in [10, 20]


@pytest.mark.asyncio
async def test_dag_runtime_with_dependencies():
    """Test DAG runtime with explicit dependencies."""

    @flow_type
    class DependentDagFlow:
        @step
        async def fetch_data(self):
            return 42

        @step(depends_on=["fetch_data"], inputs={"data": "fetch_data"})
        async def process_data(self, data):
            return data * 2

        @flow
        async def run(self):
            return await dag(self)

    flow_instance = DependentDagFlow()
    result = await flow_instance.run()

    assert result == 84


@pytest.mark.asyncio
async def test_dag_runtime_parallel_steps():
    """Test that independent steps run in parallel."""

    @flow_type
    class ParallelDagFlow:
        @step
        async def step_a(self):
            return 1

        @step
        async def step_b(self):
            return 2

        @step
        async def step_c(self):
            return 3

        @step(
            depends_on=["step_a", "step_b", "step_c"],
            inputs={"a": "step_a", "b": "step_b", "c": "step_c"},
        )
        async def merge(self, a, b, c):
            return a + b + c

        @flow
        async def run(self):
            return await dag(self)

    flow_instance = ParallelDagFlow()
    result = await flow_instance.run()

    assert result == 6


@pytest.mark.asyncio
async def test_dag_runtime_diamond_pattern():
    """Test diamond dependency pattern."""

    @flow_type
    class DiamondDagFlow:
        @step
        async def root(self):
            return 10

        @step(depends_on=["root"], inputs={"val": "root"})
        async def branch_a(self, val):
            return val + 1

        @step(depends_on=["root"], inputs={"val": "root"})
        async def branch_b(self, val):
            return val + 2

        @step(depends_on=["branch_a", "branch_b"], inputs={"a": "branch_a", "b": "branch_b"})
        async def merge(self, a, b):
            return a + b

        @flow
        async def run(self):
            return await dag(self)

    flow_instance = DiamondDagFlow()
    result = await flow_instance.run()

    assert result == 23  # (10 + 1) + (10 + 2)


@pytest.mark.asyncio
async def test_dag_runtime_final_step_selection():
    """Test selecting a specific final step."""

    @flow_type
    class SelectiveDagFlow:
        @step
        async def step_a(self):
            return 100

        @step(depends_on=["step_a"], inputs={"val": "step_a"})
        async def step_b(self, val):
            return val + 50

        @step(depends_on=["step_a"], inputs={"val": "step_a"})
        async def step_c(self, val):
            return val + 25

        @flow
        async def run(self):
            # Select step_b as final
            return await dag(self, final_step="step_b")

    flow_instance = SelectiveDagFlow()
    result = await flow_instance.run()

    assert result == 150


@pytest.mark.asyncio
async def test_dag_runtime_no_steps_error():
    """Test error when flow has no @step methods."""

    @flow_type
    class NoStepsFlow:
        @flow
        async def run(self):
            return await dag(self)

    flow_instance = NoStepsFlow()

    with pytest.raises(DagExecutionError, match="No @step methods found"):
        await flow_instance.run()


@pytest.mark.asyncio
async def test_dag_runtime_missing_dependency_error():
    """Test error when step depends on non-existent step."""

    @flow_type
    class MissingDepFlow:
        @step(depends_on=["nonexistent"])
        async def step_a(self):
            return 1

        @flow
        async def run(self):
            return await dag(self)

    flow_instance = MissingDepFlow()

    with pytest.raises(DagExecutionError, match="not a @step method"):
        await flow_instance.run()


@pytest.mark.asyncio
async def test_dag_runtime_missing_input_error():
    """Test error when step requires input from non-existent step."""

    @flow_type
    class MissingInputFlow:
        @step
        async def step_a(self):
            return 10

        @step(depends_on=["step_a"], inputs={"val": "nonexistent"})  # nonexistent step
        async def step_b(self, val):
            return val

        @flow
        async def run(self):
            return await dag(self)

    flow_instance = MissingInputFlow()

    with pytest.raises(DagExecutionError, match="not a @step method"):
        await flow_instance.run()


@pytest.mark.asyncio
async def test_dag_runtime_step_failure():
    """Test that step failures are propagated."""

    @flow_type
    class FailingDagFlow:
        @step
        async def step_a(self):
            return 10

        @step(depends_on=["step_a"])
        async def step_b(self):
            raise ValueError("Step failed")

        @flow
        async def run(self):
            return await dag(self)

    flow_instance = FailingDagFlow()

    with pytest.raises(DagExecutionError, match="Step execution failed"):
        await flow_instance.run()


@pytest.mark.asyncio
async def test_dag_runtime_complex_workflow():
    """Test complex multi-level DAG workflow."""

    @flow_type
    class ComplexDagFlow:
        @step
        async def fetch_user(self):
            return {"id": 1, "name": "Alice"}

        @step
        async def fetch_orders(self):
            return [{"id": 101, "total": 50}, {"id": 102, "total": 75}]

        @step
        async def fetch_inventory(self):
            return {"in_stock": True}

        @step(
            depends_on=["fetch_user", "fetch_orders", "fetch_inventory"],
            inputs={"user": "fetch_user", "orders": "fetch_orders", "inventory": "fetch_inventory"},
        )
        async def validate(self, user, orders, inventory):
            return {"valid": inventory["in_stock"], "total": sum(o["total"] for o in orders)}

        @step(depends_on=["validate"], inputs={"validation": "validate"})
        async def process(self, validation):
            return validation["total"] if validation["valid"] else 0

        @flow
        async def run(self):
            return await dag(self)

    flow_instance = ComplexDagFlow()
    result = await flow_instance.run()

    assert result == 125  # 50 + 75


@pytest.mark.asyncio
async def test_dag_runtime_nonexistent_final_step():
    """Test error when final_step doesn't exist."""

    @flow_type
    class FlowWithSteps:
        @step
        async def step_a(self):
            return 1

        @flow
        async def run(self):
            return await dag(self, final_step="nonexistent")

    flow_instance = FlowWithSteps()

    with pytest.raises(DagExecutionError, match="was not executed"):
        await flow_instance.run()


@pytest.mark.asyncio
async def test_dag_runtime_private_methods_ignored():
    """Test that private methods are not treated as steps."""

    @flow_type
    class PrivateMethodFlow:
        def _helper(self):
            """Private helper method."""
            return 42

        @step
        async def step_a(self):
            return self._helper()

        @flow
        async def run(self):
            return await dag(self)

    flow_instance = PrivateMethodFlow()
    result = await flow_instance.run()

    assert result == 42
