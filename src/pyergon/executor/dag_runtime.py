"""DAG runtime execution for automatic parallel step execution.

Provides runtime DAG execution using method introspection to discover
steps decorated with @step and build the dependency graph automatically.

Usage:
    ```python
    @flow
    class MyFlow:
        @step
        async def step_a(self) -> int:
            return 10

        @step(depends_on=["step_a"], inputs={'a': 'step_a'})
        async def step_b(self, a: int) -> int:
            return a * 2

        async def run(self) -> int:
            return await dag(self)
    ```

The dag() function:
1. Discovers all @step methods
2. Builds dependency graph from depends_on metadata
3. Executes in topological order
4. Auto-wires inputs based on step return values
5. Returns the result of the last step

Concurrency vs Parallelism:
DAG uses asyncio.gather() for concurrent execution. This provides
concurrency (interleaved execution), not true parallelism. Steps
must be non-blocking:

- Good: await asyncio.sleep(), await http_client.get(), await db.query()
- Bad: time.sleep(), CPU-intensive loops, blocking I/O

For blocking operations, explicitly offload to threads:

    ```python
    @step
    async def cpu_intensive(self) -> int:
        result = await asyncio.to_thread(
            self._blocking_computation,
            arg1, arg2
        )
        return result

    def _blocking_computation(self, arg1, arg2):
        # Blocking work is safe here (runs in thread pool)
        time.sleep(1.0)
        return heavy_computation()
    ```
"""

import asyncio
from collections import deque
from typing import Any


class DagExecutionError(Exception):
    """Error during DAG execution."""

    pass


async def dag(flow: Any, final_step: str | None = None) -> Any:
    """Execute a flow's steps as a DAG based on dependency metadata.

    Independent steps at the same dependency level are executed
    concurrently using asyncio.gather().

    Args:
        flow: Flow instance with @step decorated methods
        final_step: Optional name of final step to execute and return.
                   If None, executes all steps and returns the last one.

    Returns:
        The return value of the final step

    Raises:
        DagExecutionError: If dependency graph has cycles or missing dependencies

    Example:
        ```python
        @flow
        class OrderFlow:
            @step
            async def fetch_user(self) -> User:
                ...

            @step
            async def fetch_inventory(self) -> Inventory:
                ...

            @step(depends_on=["fetch_user", "fetch_inventory"],
                  inputs={'user': 'fetch_user', 'inv': 'fetch_inventory'})
            async def process_order(self, user: User, inv: Inventory) -> Order:
                ...

            async def run(self) -> Order:
                return await dag(self)
        ```
    """
    # Step 1: Discover all @step methods
    steps = _discover_steps(flow)

    if not steps:
        raise DagExecutionError(f"No @step methods found in {flow.__class__.__name__}")

    # Step 2: Build dependency graph
    graph = _build_dependency_graph(flow, steps)

    # Step 3: Group steps by dependency level for parallel execution
    levels = _group_by_level(graph)

    # Step 4: Execute steps level by level, with parallel execution within each level
    results: dict[str, Any] = {}

    for level_steps in levels:
        # Create tasks for all steps at this level
        tasks = []
        task_names = []

        for step_name in level_steps:
            step_method = getattr(flow, step_name)

            # Get input wiring metadata
            step_inputs = getattr(step_method, "_step_inputs", {})

            # Build kwargs from wired inputs
            kwargs = {}
            for param_name, source_step in step_inputs.items():
                if source_step not in results:
                    raise DagExecutionError(
                        f"Step '{step_name}' requires input '{param_name}' from '{source_step}', "
                        f"but '{source_step}' has not executed yet or failed"
                    )
                kwargs[param_name] = results[source_step]

            # Add task to execute
            tasks.append(step_method(**kwargs))
            task_names.append(step_name)

        # Execute all steps at this level in parallel
        try:
            level_results = await asyncio.gather(*tasks)
        except Exception as e:
            # Find which step failed
            raise DagExecutionError(
                f"Step execution failed at level {levels.index(level_steps)}: {e}"
            ) from e

        # Store results
        for step_name, result in zip(task_names, level_results):
            results[step_name] = result

    # Step 5: Return result of final step
    if final_step:
        if final_step not in results:
            raise DagExecutionError(f"Final step '{final_step}' was not executed")
        return results[final_step]
    else:
        # Return result of last level's last step
        if levels:
            return results[levels[-1][-1]]
        return None


def _discover_steps(flow: Any) -> list[str]:
    """Discover all @step decorated methods in a flow.

    Args:
        flow: Flow instance

    Returns:
        List of step method names
    """
    steps = []

    for name in dir(flow):
        # Skip private methods
        if name.startswith("_"):
            continue

        attr = getattr(flow, name)

        # Check if it's an ergon step
        if callable(attr) and hasattr(attr, "_is_ergon_step"):
            steps.append(name)

    return steps


def _build_dependency_graph(flow: Any, steps: list[str]) -> dict[str, list[str]]:
    """Build dependency graph from @step metadata.

    Args:
        flow: Flow instance
        steps: List of step names

    Returns:
        Dict mapping step_name to list of steps it depends on

    Example:
        {'step_a': [], 'step_b': ['step_a'], 'step_c': ['step_a', 'step_b']}
    """
    graph: dict[str, list[str]] = {}

    for step_name in steps:
        step_method = getattr(flow, step_name)
        depends_on = getattr(step_method, "_step_depends_on", [])

        # Filter out special markers like __NO_AUTO_CHAIN__
        dependencies = [dep for dep in depends_on if not dep.startswith("__")]

        graph[step_name] = dependencies

    # Validate dependencies exist
    all_steps = set(steps)
    for step_name, deps in graph.items():
        for dep in deps:
            if dep not in all_steps:
                raise DagExecutionError(
                    f"Step '{step_name}' depends on '{dep}', but '{dep}' is not a @step method"
                )

    return graph


def _topological_sort(graph: dict[str, list[str]]) -> list[str]:
    """Topological sort of dependency graph using Kahn's algorithm.

    Args:
        graph: Dependency graph mapping step to dependencies

    Returns:
        List of steps in execution order

    Raises:
        DagExecutionError: If graph has cycles

    Algorithm (Kahn's):
    1. Find nodes with no incoming edges (no dependencies)
    2. Remove them and their outgoing edges
    3. Repeat until graph is empty
    4. If graph still has edges, there's a cycle
    """
    # Build in-degree map (how many steps depend on each step)
    in_degree: dict[str, int] = {step: 0 for step in graph}

    for step, deps in graph.items():
        for dep in deps:
            in_degree[step] += 1

    # Queue of steps with no dependencies
    queue = deque([step for step, degree in in_degree.items() if degree == 0])

    result = []

    while queue:
        # Process step with no remaining dependencies
        step = queue.popleft()
        result.append(step)

        # Reduce in-degree for steps that depend on this one
        for dependent_step, deps in graph.items():
            if step in deps:
                in_degree[dependent_step] -= 1

                # If dependent has no more dependencies, add to queue
                if in_degree[dependent_step] == 0:
                    queue.append(dependent_step)

    # Check for cycles
    if len(result) != len(graph):
        remaining = [step for step in graph if step not in result]
        raise DagExecutionError(f"Dependency graph has cycles. Remaining steps: {remaining}")

    return result


def _group_by_level(graph: dict[str, list[str]]) -> list[list[str]]:
    """Group steps by dependency level for parallel execution.

    Steps at the same level have no dependencies on each other
    and can execute in parallel.

    Args:
        graph: Dependency graph mapping step to dependencies

    Returns:
        List of levels, each containing steps that can execute in parallel

    Example:
        graph = {'a': [], 'b': [], 'c': ['a'], 'd': ['a', 'b'], 'e': ['c', 'd']}
        Returns: [['a', 'b'], ['c', 'd'], ['e']]
        - Level 0: a, b (no dependencies)
        - Level 1: c (depends on a), d (depends on a, b)
        - Level 2: e (depends on c, d)
    """
    # Calculate the level (max dependency depth) for each step
    levels_map: dict[str, int] = {}

    def calculate_level(step: str) -> int:
        """Calculate the level of a step - max depth from root."""
        if step in levels_map:
            return levels_map[step]

        deps = graph.get(step, [])
        if not deps:
            # No dependencies - level 0
            level = 0
        else:
            # Level is 1 + max level of dependencies
            level = 1 + max(calculate_level(dep) for dep in deps)

        levels_map[step] = level
        return level

    # Calculate level for all steps
    for step in graph:
        calculate_level(step)

    # Group steps by level
    max_level = max(levels_map.values()) if levels_map else 0
    levels: list[list[str]] = [[] for _ in range(max_level + 1)]

    for step, level in levels_map.items():
        levels[level].append(step)

    return levels


# Export public API
__all__ = ["dag", "DagExecutionError"]
