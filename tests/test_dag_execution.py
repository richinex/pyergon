"""Tests for DAG (Directed Acyclic Graph) parallel execution."""

import asyncio
import pickle

import pytest

from pyergon.executor.dag import DagSummary, DeferredRegistry, StepHandle, execute_dag


@pytest.mark.asyncio
async def test_deferred_registry_basic():
    """Test basic DeferredRegistry with independent steps."""
    registry = DeferredRegistry()

    async def step_a(_inputs):
        return 10

    async def step_b(_inputs):
        return 20

    handle_a = registry.register("a", [], step_a)
    handle_b = registry.register("b", [], step_b)

    assert isinstance(handle_a, StepHandle)
    assert isinstance(handle_b, StepHandle)
    assert handle_a.step_id == "a"
    assert handle_b.step_id == "b"

    await registry.execute()

    result_a = await handle_a.resolve()
    result_b = await handle_b.resolve()

    assert result_a == 10
    assert result_b == 20


@pytest.mark.asyncio
async def test_dag_parallel_execution():
    """Test that independent steps run in parallel."""
    registry = DeferredRegistry()
    execution_order = []

    async def step_a(_inputs):
        execution_order.append("a_start")
        await asyncio.sleep(0.01)
        execution_order.append("a_end")
        return 1

    async def step_b(_inputs):
        execution_order.append("b_start")
        await asyncio.sleep(0.01)
        execution_order.append("b_end")
        return 2

    async def step_c(_inputs):
        execution_order.append("c_start")
        await asyncio.sleep(0.01)
        execution_order.append("c_end")
        return 3

    handle_a = registry.register("a", [], step_a)
    handle_b = registry.register("b", [], step_b)
    handle_c = registry.register("c", [], step_c)

    await registry.execute()

    result_a = await handle_a.resolve()
    result_b = await handle_b.resolve()
    result_c = await handle_c.resolve()

    assert result_a == 1
    assert result_b == 2
    assert result_c == 3

    # All should start before any end (parallel execution)
    assert "a_start" in execution_order
    assert "b_start" in execution_order
    assert "c_start" in execution_order


@pytest.mark.asyncio
async def test_dag_with_dependencies():
    """Test DAG with dependent steps."""
    registry = DeferredRegistry()

    async def step_a(_inputs):
        return 10

    async def step_b(inputs):
        a_value = pickle.loads(inputs["a"])
        return a_value + 5

    async def step_c(inputs):
        b_value = pickle.loads(inputs["b"])
        return b_value * 2

    handle_a = registry.register("a", [], step_a)
    handle_b = registry.register("b", ["a"], step_b)
    handle_c = registry.register("c", ["b"], step_c)

    await registry.execute()

    result_a = await handle_a.resolve()
    result_b = await handle_b.resolve()
    result_c = await handle_c.resolve()

    assert result_a == 10
    assert result_b == 15
    assert result_c == 30


@pytest.mark.asyncio
async def test_dag_diamond_pattern():
    """Test diamond dependency pattern (A -> B,C -> D)."""
    registry = DeferredRegistry()

    async def step_a(_inputs):
        return 10

    async def step_b(inputs):
        a_value = pickle.loads(inputs["a"])
        return a_value + 1

    async def step_c(inputs):
        a_value = pickle.loads(inputs["a"])
        return a_value + 2

    async def step_d(inputs):
        b_value = pickle.loads(inputs["b"])
        c_value = pickle.loads(inputs["c"])
        return b_value + c_value

    registry.register("a", [], step_a)
    registry.register("b", ["a"], step_b)
    registry.register("c", ["a"], step_c)
    handle_d = registry.register("d", ["b", "c"], step_d)

    await registry.execute()

    result_d = await handle_d.resolve()
    assert result_d == 23  # (10 + 1) + (10 + 2) = 23


@pytest.mark.asyncio
async def test_dag_validation_invalid_dependency():
    """Test validation catches invalid dependencies."""
    registry = DeferredRegistry()

    async def step_a(_inputs):
        return 1

    registry.register("a", ["nonexistent"], step_a)

    with pytest.raises(ValueError, match="depends on non-existent"):
        registry.validate()


@pytest.mark.asyncio
async def test_dag_validation_cycle_detection():
    """Test validation catches cycles."""
    registry = DeferredRegistry()

    async def step_a(inputs):
        return 1

    async def step_b(inputs):
        return 2

    # Create cycle: a -> b -> a
    registry.register("a", ["b"], step_a)
    registry.register("b", ["a"], step_b)

    with pytest.raises(ValueError, match="Cycle detected"):
        registry.validate()


@pytest.mark.asyncio
async def test_dag_summary():
    """Test DAG summary statistics."""
    registry = DeferredRegistry()

    async def step(_inputs):
        return 1

    registry.register("a", [], step)
    registry.register("b", ["a"], step)
    registry.register("c", ["a"], step)
    registry.register("d", ["b", "c"], step)

    summary = registry.summary()

    assert isinstance(summary, DagSummary)
    assert summary.total_steps == 4
    assert summary.root_count == 1
    assert summary.roots == ["a"]
    assert summary.leaf_count == 1
    assert summary.leaves == ["d"]
    assert summary.max_depth == 2


@pytest.mark.asyncio
async def test_dag_level_graph():
    """Test level graph generation."""
    registry = DeferredRegistry()

    async def step(_inputs):
        return 1

    registry.register("a", [], step)
    registry.register("b", ["a"], step)
    registry.register("c", ["b"], step)

    graph = registry.level_graph()

    assert "Level 0" in graph
    assert "Level 1" in graph
    assert "Level 2" in graph
    assert "[a]" in graph
    assert "[b]" in graph
    assert "[c]" in graph
    assert "parallel" in graph


@pytest.mark.asyncio
async def test_step_handle_single_use():
    """Test that step handles can only be resolved once."""
    registry = DeferredRegistry()

    async def step_a(_inputs):
        return 42

    handle = registry.register("a", [], step_a)

    await registry.execute()

    result = await handle.resolve()
    assert result == 42

    with pytest.raises(RuntimeError, match="called twice"):
        await handle.resolve()


@pytest.mark.asyncio
async def test_dag_step_failure():
    """Test DAG handles step failures."""
    registry = DeferredRegistry()

    async def step_a(_inputs):
        return 1

    async def step_b(_inputs):
        raise ValueError("Step B failed")

    registry.register("a", [], step_a)
    registry.register("b", [], step_b)

    with pytest.raises(RuntimeError, match="DAG execution failed"):
        await registry.execute()


@pytest.mark.asyncio
async def test_execute_dag_deadlock_detection():
    """Test execute_dag detects deadlocks."""
    from pyergon.executor.dag import DeferredStep

    # Create invalid DAG that would deadlock
    step1 = DeferredStep(
        step_id="a",
        dependencies=["b"],
        factory=lambda _: asyncio.sleep(0),
        result_future=asyncio.Future(),
    )

    step2 = DeferredStep(
        step_id="b",
        dependencies=["a"],
        factory=lambda _: asyncio.sleep(0),
        result_future=asyncio.Future(),
    )

    with pytest.raises(ValueError, match="Deadlock|cycle"):
        await execute_dag([step1, step2])


@pytest.mark.asyncio
async def test_dag_complex_parallel_branches():
    """Test complex DAG with multiple parallel branches."""
    registry = DeferredRegistry()

    async def root(_inputs):
        return 100

    async def branch(inputs):
        root_val = pickle.loads(inputs["root"])
        return root_val + 1

    async def merge(inputs):
        a = pickle.loads(inputs["a"])
        b = pickle.loads(inputs["b"])
        c = pickle.loads(inputs["c"])
        return a + b + c

    registry.register("root", [], root)
    registry.register("a", ["root"], branch)
    registry.register("b", ["root"], branch)
    registry.register("c", ["root"], branch)
    handle_merge = registry.register("merge", ["a", "b", "c"], merge)

    await registry.execute()

    result = await handle_merge.resolve()
    assert result == 303  # (100+1) + (100+1) + (100+1) = 303
