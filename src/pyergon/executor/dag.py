"""DAG (Directed Acyclic Graph) parallel execution.

Enables automatic parallel execution of independent workflow steps by
building a dependency graph at runtime and executing in topological order.

Design: Information Hiding (Parnas)
Implementation uses deferred handles and dependency tracking, but users
only see the high-level DAG flow abstraction. The complexity of parallel
execution coordination is hidden.

How it works:
1. Steps return StepHandle[T] instead of T directly
2. Handles contain step metadata and factory functions
3. When handles are used as inputs, dependencies are tracked
4. DeferredRegistry builds the dependency graph
5. Independent steps execute in parallel via asyncio.gather
6. Dependent steps wait for their dependencies first

Example:
    ```python
    @flow
    class OrderProcessor:
        async def process_order(self) -> OrderResult:
            # These run in parallel (independent)
            user = self.fetch_user()
            inventory = self.check_inventory()
            pricing = self.get_pricing()

            # Waits for all three above
            validated = self.validate_order(user, inventory, pricing)

            # Waits for validation
            return self.charge_card(validated)
    ```
"""

import asyncio
import pickle
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, Generic, TypeVar

# Type variables
T = TypeVar("T")
R = TypeVar("R")

# Type aliases for complex types
StepInputs = dict[str, bytes]  # step_id -> serialized value
StepOutput = bytes
StepFactory = Callable[[StepInputs], Awaitable[StepOutput]]


class StepHandle(Generic[T]):
    """Handle representing a deferred step execution.

    The step is not executed immediately. Instead, it's registered with
    the executor and resolved later based on the dependency graph.

    Important: Handles are single-use. The resolve() method can only
    be called once. Store the result if you need it multiple times.

    Attributes:
        step_id: Unique step identifier
        dependencies: List of step IDs this step depends on
        _future: asyncio.Future for the result
    """

    def __init__(self, step_id: str, dependencies: list[str], future: "asyncio.Future[T]"):
        self.step_id = step_id
        self.dependencies = dependencies
        self._future = future
        self._resolved = False

    async def resolve(self) -> T:
        """Await the step result.

        Blocks until the step is resolved. Can only be called once.
        If you need the result in multiple places, store it:

        Example:
            ```python
            result = await handle.resolve()
            # Use result multiple times
            ```

        Returns:
            The step result of type T

        Raises:
            RuntimeError: If called twice
            Exception: If step execution failed
        """
        if self._resolved:
            raise RuntimeError(
                f"StepHandle.resolve() called twice for step '{self.step_id}'. "
                "Handles are single-use - store the result in a variable."
            )
        self._resolved = True

        return await self._future


@dataclass
class DeferredStep:
    """Internal representation of a deferred step for execution.

    Attributes:
        step_id: Step identifier
        dependencies: List of dependency step IDs
        factory: Factory function taking serialized inputs, returning serialized output
        result_future: asyncio.Future to set the result
    """

    step_id: str
    dependencies: list[str]
    factory: StepFactory
    result_future: "asyncio.Future[Any]"


class DeferredRegistry:
    """Registry for collecting deferred steps during flow execution.

    Example:
        ```python
        registry = DeferredRegistry()

        # Register independent steps (run in parallel)
        user = registry.register("fetch_user", [], fetch_user_impl)
        inventory = registry.register("fetch_inventory", [], fetch_inventory_impl)
        pricing = registry.register("fetch_pricing", [], fetch_pricing_impl)

        # Register dependent step (waits for all three)
        validated = registry.register(
            "validate_order",
            ["fetch_user", "fetch_inventory", "fetch_pricing"],
            validate_impl
        )

        # Execute all steps with automatic parallelization
        await registry.execute()

        # Resolve results
        user_data = await user.resolve()
        validated_data = await validated.resolve()
        ```
    """

    def __init__(self):
        self.steps: list[DeferredStep] = []

    def register(
        self, step_name: str, dependencies: list[str], factory: Callable[[StepInputs], Awaitable[T]]
    ) -> StepHandle[T]:
        """Register a deferred step with explicit dependencies.

        Dependencies must be explicitly declared to make the DAG
        structure clear from the code.

        Args:
            step_name: Unique step identifier
            dependencies: List of step names this step depends on
            factory: Async function taking StepInputs and returning T

        Returns:
            StepHandle[T] that can be awaited for the result

        Example:
            ```python
            registry = DeferredRegistry()

            # Independent parallel steps
            a = registry.register("fetch_user", [], async_fetch_user)
            b = registry.register("fetch_inventory", [], async_fetch_inventory)
            c = registry.register("fetch_pricing", [], async_fetch_pricing)

            # Root step
            root = registry.register("root", [], lambda _: async_root())

            # These depend on root - run in parallel after root
            branch1 = registry.register("branch1", ["root"], async_branch1)
            branch2 = registry.register("branch2", ["root"], async_branch2)
            ```
        """
        step_id = step_name
        deps = list(dependencies)  # Copy to avoid mutation

        # Create future for result
        result_future: asyncio.Future[T] = asyncio.Future()

        # Wrap factory to serialize output
        async def factory_wrapped(inputs: StepInputs) -> bytes:
            # Call original factory
            result = await factory(inputs)
            # Serialize result
            return pickle.dumps(result)

        # Store deferred step
        self.steps.append(
            DeferredStep(
                step_id=step_id,
                dependencies=deps,
                factory=factory_wrapped,
                result_future=result_future,
            )
        )

        # Return handle
        return StepHandle(step_id, deps, result_future)

    def validate(self) -> None:
        """Validate the DAG structure without executing.

        Checks for cycles and invalid dependencies. Call before
        execute() to catch structural issues early.

        Raises:
            ValueError: If cycles or invalid dependencies found

        Example:
            ```python
            registry = DeferredRegistry()
            # ... register steps ...

            # Validate before execution
            registry.validate()

            # If validation passes, safe to execute
            await registry.execute()
            ```
        """
        # Build step ID set
        step_ids: set[str] = {s.step_id for s in self.steps}

        # Check for invalid dependencies
        for step in self.steps:
            for dep in step.dependencies:
                if dep not in step_ids:
                    raise ValueError(f"Step '{step.step_id}' depends on non-existent step '{dep}'")

        # Check for cycles using DFS
        visited: set[str] = set()
        rec_stack: set[str] = set()

        def has_cycle(step_id: str) -> bool:
            visited.add(step_id)
            rec_stack.add(step_id)

            # Find step
            step = next((s for s in self.steps if s.step_id == step_id), None)
            if step:
                for dep in step.dependencies:
                    if dep not in visited:
                        if has_cycle(dep):
                            return True
                    elif dep in rec_stack:
                        return True

            rec_stack.remove(step_id)
            return False

        for step in self.steps:
            if step.step_id not in visited:
                if has_cycle(step.step_id):
                    raise ValueError(
                        f"Cycle detected in dependency graph involving step '{step.step_id}'"
                    )

    async def execute(self) -> None:
        """Execute all registered steps with automatic parallelization.

        Automatically validates the DAG before execution. Call validate()
        beforehand if you want to check for issues separately.

        Raises:
            ValueError: If DAG is invalid (cycles or invalid dependencies)
            Exception: If any step execution fails
        """
        await execute_dag(self.steps)

    def summary(self) -> "DagSummary":
        """Return a summary of the DAG structure.

        Returns:
            DagSummary with graph statistics including total steps,
            root nodes, leaf nodes, and maximum depth
        """
        total_steps = len(self.steps)

        # Find roots (steps with no dependencies)
        roots = [s.step_id for s in self.steps if not s.dependencies]

        # Find leaves (steps that no one depends on)
        all_deps: set[str] = set()
        for step in self.steps:
            all_deps.update(step.dependencies)

        leaves = [s.step_id for s in self.steps if s.step_id not in all_deps]

        # Calculate max depth
        max_depth = self._calculate_max_depth()

        return DagSummary(
            total_steps=total_steps,
            root_count=len(roots),
            leaf_count=len(leaves),
            max_depth=max_depth,
            roots=roots,
            leaves=leaves,
        )

    def level_graph(self) -> str:
        """Return a level-based graph showing parallel execution levels.

        Shows steps grouped by execution level, making it clear
        which steps run in parallel.

        Returns:
            Multi-line string representation

        Example output:
            ```
            Level 0: [get_customer]
                     ↓
            Level 1: [validate_payment] [check_inventory] (2 parallel steps)
                     ↓
            Level 2: [process_order]
                     ↓
            Level 3: [send_confirmation]
            ```
        """
        output = f"DAG Execution Levels ({len(self.steps)} steps):\n\n"

        # Calculate depths for all steps
        depths = self._calculate_depths()

        # Group steps by level
        max_level = max(depths.values()) if depths else 0
        levels: list[list[str]] = [[] for _ in range(max_level + 1)]

        for step in self.steps:
            depth = depths.get(step.step_id, 0)
            levels[depth].append(step.step_id)

        # Format each level
        for level, steps in enumerate(levels):
            if not steps:
                continue

            parallel_note = f" ({len(steps)} parallel steps)" if len(steps) > 1 else ""

            output += f"Level {level}: [{'] ['.join(steps)}]{parallel_note}\n"

            if level < max_level:
                output += "         ↓\n"

        output += "\nSteps at the same level run in parallel!\n"
        return output

    def _calculate_depths(self) -> dict[str, int]:
        """Calculate the depth of each step in the DAG.

        Returns:
            Dict mapping step_id to depth. Root nodes have depth 0,
            their children have depth 1, etc.
        """
        depths: dict[str, int] = {}

        # Find roots
        roots = [s for s in self.steps if not s.dependencies]

        # Initialize roots with depth 0
        for root in roots:
            depths[root.step_id] = 0

        # Iteratively calculate depths using dynamic programming
        changed = True
        while changed:
            changed = False
            for step in self.steps:
                if step.step_id in depths:
                    continue

                # Check if all dependencies have depths
                dep_depths = [depths.get(dep) for dep in step.dependencies]
                if all(d is not None for d in dep_depths):
                    max_dep_depth = max(dep_depths) if dep_depths else -1
                    new_depth = max_dep_depth + 1
                    if depths.get(step.step_id) != new_depth:
                        depths[step.step_id] = new_depth
                        changed = True

        return depths

    def _calculate_max_depth(self) -> int:
        """Calculates the maximum depth of the DAG"""
        depths = self._calculate_depths()
        return max(depths.values()) if depths else 0


@dataclass
class DagSummary:
    """Summary information about a DAG structure.

    Attributes:
        total_steps: Total number of steps in the DAG
        root_count: Number of root nodes (steps with no dependencies)
        leaf_count: Number of leaf nodes (steps with no dependents)
        max_depth: Maximum depth of the DAG
        roots: List of root step IDs
        leaves: List of leaf step IDs
    """

    total_steps: int
    root_count: int
    leaf_count: int
    max_depth: int
    roots: list[str]
    leaves: list[str]


async def execute_dag(steps: list[DeferredStep]) -> None:
    """Execute deferred steps in parallel based on dependencies.

    Core DAG execution engine that builds and validates the dependency
    graph, executes steps in topological order, runs independent steps
    in parallel, and preserves task-local context.

    Args:
        steps: List of DeferredStep objects to execute

    Raises:
        ValueError: If DAG has cycles or invalid dependencies
        Exception: If any step execution fails

    Example:
        ```python
        registry = DeferredRegistry()
        # ... register steps ...
        await registry.execute()  # Internally calls execute_dag
        ```
    """
    total = len(steps)
    completed: set[str] = set()
    results: dict[str, bytes] = {}

    # Build step lookup
    step_map: dict[str, DeferredStep] = {s.step_id: s for s in steps}

    # Validate: check for invalid dependencies
    step_ids = set(step_map.keys())
    for step in steps:
        for dep in step.dependencies:
            if dep not in step_ids:
                raise ValueError(f"Step '{step.step_id}' depends on non-existent step '{dep}'")

    while len(completed) < total:
        # Find ready steps (all dependencies completed)
        ready = [
            step_id
            for step_id, step in step_map.items()
            if step_id not in completed and all(dep in completed for dep in step.dependencies)
        ]

        if not ready and len(completed) < total:
            raise ValueError(
                "Deadlock: no steps ready but not all completed. "
                "Possible cycle in dependency graph."
            )

        # Execute ready steps in parallel using asyncio.gather
        # This runs futures concurrently in the SAME task, preserving task-local context
        async def execute_step(step_id: str) -> tuple[str, bytes]:
            step = step_map[step_id]

            # Gather inputs from completed dependencies
            inputs: StepInputs = {dep: results[dep] for dep in step.dependencies}

            # Execute factory
            try:
                output = await step.factory(inputs)
                return (step_id, output)
            except Exception as e:
                # Set exception on future
                step.result_future.set_exception(e)
                raise

        # Run all ready steps in parallel
        try:
            step_results = await asyncio.gather(*[execute_step(step_id) for step_id in ready])
        except Exception as e:
            # One step failed - abort
            raise RuntimeError(f"DAG execution failed: {e}") from e

        # Process results
        for step_id, output_bytes in step_results:
            step = step_map[step_id]

            # Store serialized result
            results[step_id] = output_bytes

            # Deserialize and set future
            try:
                result = pickle.loads(output_bytes)
                step.result_future.set_result(result)
            except Exception as e:
                step.result_future.set_exception(e)
                raise

            # Mark as completed
            completed.add(step_id)


# Public exports
__all__ = [
    "StepHandle",
    "DeferredRegistry",
    "DagSummary",
    "execute_dag",
]
