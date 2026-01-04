"""Direct flow execution without a work queue.

Provides the Executor class for running flows immediately in the
current process. For distributed execution with multiple workers,
use Scheduler and Worker instead.

Design: Simplicity
Single type combining state and execution methods. Package name
provides context (pyergon.Executor), so no redundant prefix needed.
"""

import pickle
from collections.abc import Awaitable, Callable
from typing import Generic, TypeVar
from uuid import uuid4

from pyergon.core import (
    _CACHE_MISS,
    CALL_TYPE,
    EXECUTION_CONTEXT,
    CallType,
    Context,
)
from pyergon.executor.outcome import (
    Completed,
    FlowOutcome,
    Suspended,
    _SuspendExecution,
)
from pyergon.storage.base import ExecutionLog

T = TypeVar("T")  # Flow type
R = TypeVar("R")  # Result type


class Executor(Generic[T]):
    """Execute flows directly without a work queue.

    Provides immediate flow execution in the current process.
    For distributed execution with multiple workers, use
    Scheduler and Worker instead.

    Example:
        ```python
        storage = SqliteExecutionLog("db.db")
        await storage.connect()

        flow = OrderProcessor(order_id="123", amount=100.0)
        executor = Executor(flow, storage)
        result = await executor.run(lambda f: f.process_order())

        # With explicit flow_id
        executor = Executor(flow, storage, flow_id="my-flow-123")
        result = await executor.run(lambda f: f.process_order())
        ```
    """

    def __init__(self, flow: T, storage: ExecutionLog, flow_id: str = None, class_name: str = None):
        """Initialize executor with flow and storage.

        After construction, executor is ready to use immediately.

        Args:
            flow: Flow instance to execute
            storage: Storage backend for persistence
            flow_id: Flow identifier (generated if not provided)
            class_name: Class name (extracted from flow if not provided)
        """
        self.flow = flow
        self.storage = storage
        self.flow_id = flow_id or str(uuid4())

        # Extract class name from flow
        if class_name is not None:
            self.class_name = class_name
        else:
            self.class_name = flow.__class__.__name__

    async def run(self, entry_point: Callable[[T], Awaitable[R]]) -> FlowOutcome[R]:
        """Execute flow with replay and suspension support.

        Creates execution context, runs the flow, and detects completion
        or suspension. Suspension is detected via context state, not timing.

        Args:
            entry_point: Async function taking the flow and returning result

        Returns:
            FlowOutcome - either Completed(result) or Suspended(reason)

        Example:
            ```python
            executor = Executor(order, storage)
            outcome = await executor.run(lambda f: f.process_order())

            match outcome:
                case Completed(result):
                    print(f"Flow completed: {result}")
                case Suspended(reason):
                    print(f"Flow suspended: {reason}")
            ```
        """
        # Create execution context
        ctx = Context(flow_id=self.flow_id, storage=self.storage, class_name=self.class_name)

        # Set task-local context
        token_ctx = EXECUTION_CONTEXT.set(ctx)
        token_call_type = CALL_TYPE.set(CallType.RUN)

        try:
            # Allocate step 0 for flow entry
            step_0 = ctx.next_step()  # Should be 0
            retry_policy = getattr(self.flow, "_ergon_retry_policy", None)

            # Check cache before logging (critical for replay)
            cached = await ctx.get_cached_result(
                step=step_0, class_name=self.class_name, method_name="<flow_entry>", params_hash=0
            )

            if cached is not _CACHE_MISS:
                # Flow already completed - return cached result
                return Completed(result=cached)

            # Not cached - log flow entry invocation
            await ctx.log_step_start(
                step=step_0,
                class_name=self.class_name,
                method_name="<flow_entry>",
                parameters=b"",
                params_hash=0,
                delay=None,
                retry_policy=retry_policy,
            )

            # Execute flow with context available
            result = await entry_point(self.flow)

            # Check for suspension
            suspend_reason = ctx.take_suspend_reason()

            if suspend_reason is not None:
                # Flow suspended - don't log completion
                return Suspended(reason=suspend_reason)
            else:
                # Flow completed - log completion
                result_bytes = pickle.dumps(result)
                await ctx.log_step_completion(
                    step=step_0,
                    return_value=result_bytes,
                    is_retryable=None,
                )
                return Completed(result=result)

        except _SuspendExecution:
            # Flow suspended via control flow exception
            # Raised by pending_child.result() or pending_timer.wait()
            suspend_reason = ctx.take_suspend_reason()
            if suspend_reason is None:
                raise RuntimeError("_SuspendExecution raised but no suspend_reason set in context")
            return Suspended(reason=suspend_reason)

        except Exception as e:
            # Flow raised an exception - return as failed completion
            # Don't log step 0 completion for errors (flow will be retried)
            # Worker will handle retry logic
            return Completed(result=e)

        finally:
            # Clean up task-local context
            EXECUTION_CONTEXT.reset(token_ctx)
            CALL_TYPE.reset(token_call_type)

    async def execute(self, entry_point: Callable[[T], Awaitable[R]]) -> FlowOutcome[R]:
        """Alias for run().

        Args:
            entry_point: Async function taking the flow

        Returns:
            FlowOutcome - either Completed(result) or Suspended(reason)

        Example:
            ```python
            outcome = await executor.execute(lambda f: f.run())
            ```
        """
        return await self.run(entry_point)

    def __repr__(self) -> str:
        """Readable representation for debugging."""
        return f"Executor(flow_id={self.flow_id!r}, class_name={self.class_name!r})"


# =============================================================================
# Helper Functions
# =============================================================================


async def execute_flow(
    flow: T, storage: ExecutionLog, entry_point: Callable[[T], Awaitable[R]], flow_id: str = None
) -> FlowOutcome[R]:
    """Convenience function for one-off flow execution.

    Args:
        flow: Flow instance to execute
        storage: Storage backend
        entry_point: Flow entry point function
        flow_id: Flow identifier (generated if not provided)

    Returns:
        FlowOutcome - either Completed(result) or Suspended(reason)

    Example:
        ```python
        outcome = await execute_flow(
            order,
            storage,
            lambda f: f.process_order()
        )

        if isinstance(outcome, Completed):
            print(f"Result: {outcome.result}")
        ```
    """
    executor = Executor(flow, storage, flow_id)
    return await executor.run(entry_point)
