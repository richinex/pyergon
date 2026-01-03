"""
Executor - Direct flow execution.

This module provides Executor for running flows directly without a queue.
For distributed execution, use Scheduler + Worker instead.

**Rust Reference**:
`/home/richinex/Documents/devs/rust_projects/ergon/ergon/src/executor/instance.rs`

RUST COMPLIANCE: Matches Rust ergon src/executor/instance.rs + executor.rs
Simplified from FlowInstance + FlowExecutor into single Executor type.

From Dave Cheney:
"The name of an identifier includes its package name"
ergon::Executor is clear - no need for FlowExecutor or ExecutionExecutor.

Design: Single type that holds state AND provides execution methods.
No separation of FlowInstance/FlowExecutor - simpler is better.
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
    """
    Execute flows directly (synchronous or async).

    Executor provides direct flow execution without going through a queue.
    Use this when you want to run a flow immediately in the current process.

    For distributed execution, use Scheduler + Worker instead.

    From Dave Cheney: "Design APIs for their default use case"
    Most common case is async execution, so run() is async by default.

    RUST COMPLIANCE: Combines Rust FlowInstance + FlowExecutor into one type.
    Simplified following Dave's advice: "prefer fewer, simpler things".

    Usage:
        # Direct async execution
        storage = SqliteExecutionLog("db.db")
        await storage.connect()

        flow = OrderProcessor(order_id="123", amount=100.0)
        executor = Executor(flow, storage)
        result = await executor.run(lambda f: f.process_order())

        # Or with explicit flow_id
        executor = Executor(flow, storage, flow_id="my-flow-123")
        result = await executor.run(lambda f: f.process_order())
    """

    def __init__(self, flow: T, storage: ExecutionLog, flow_id: str = None, class_name: str = None):
        """
        Initialize executor with flow and storage.

        From Dave Cheney: "Make the zero value useful"
        After construction, executor is ready to use (no separate connect() needed).

        Args:
            flow: Flow instance to execute
            storage: Storage backend for persistence
            flow_id: Optional flow ID (generated if not provided)
            class_name: Optional class name (extracted if not provided)
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
        """
        Execute flow asynchronously with instant suspension detection.

        **Rust Reference**: `src/executor/instance.rs` lines 91-136

        This is the main execution method. Creates a Context,
        sets it as task-local, executes the flow, and detects suspension
        INSTANTLY (no timeout needed).

        **From Rust**:
        ```rust
        pub async fn execute<F, R>(&self, f: F) -> FlowOutcome<R>
        where F: FnOnce(&T) -> BoxFuture<'static, R>
        ```

        **Instant Suspension Detection**:
        After flow execution, checks if Context has a suspend_reason.
        If yes, returns Suspended(reason). If no, returns Completed(result).

        No timeout needed! Suspension is detected via context state, not timing.

        **Python Best Practice**: Using async/await for concurrency
        Reference: https://docs.python.org/3/library/asyncio-task.html

        Args:
            entry_point: Async function that takes the flow and returns result
                        Example: lambda f: f.run()

        Returns:
            FlowOutcome: Either Completed(result) or Suspended(reason)

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
        # From Rust: let context = Arc::new(ExecutionContext::new(self.id, self.storage.clone()))
        ctx = Context(flow_id=self.flow_id, storage=self.storage, class_name=self.class_name)

        # Set task-local context
        # From Rust: EXECUTION_CONTEXT.scope(..., CALL_TYPE.scope(..., flow_future))
        token_ctx = EXECUTION_CONTEXT.set(ctx)
        token_call_type = CALL_TYPE.set(CallType.RUN)

        try:
            # Allocate step 0 for flow entry
            # From Rust #[flow] macro: step 0 represents the entire flow
            step_0 = ctx.next_step()  # Should be 0
            retry_policy = getattr(self.flow, "_ergon_retry_policy", None)

            # Check cache BEFORE logging (critical for replay)
            # From Rust #[flow] macro lines 174-205: checks cache first
            cached = await ctx.get_cached_result(
                step=step_0, class_name=self.class_name, method_name="<flow_entry>", params_hash=0
            )

            if cached is not _CACHE_MISS:
                # Flow already completed - return cached result (can be None)
                # This is a replay, skip re-execution
                return Completed(result=cached)

            # Not cached - log flow entry invocation
            await ctx.log_step_start(
                step=step_0,
                class_name=self.class_name,
                method_name="<flow_entry>",
                parameters=b"",  # Flow entry has no parameters
                params_hash=0,
                delay=None,
                retry_policy=retry_policy,
            )

            # Execute flow with context available
            # Python note: We can't manually poll futures like Rust's poll_fn,
            # but we can check suspend_reason immediately after await completes
            result = await entry_point(self.flow)

            # Check for instant suspension detection
            # From Rust: context.take_suspend_reason()
            suspend_reason = ctx.take_suspend_reason()

            if suspend_reason is not None:
                # Flow suspended (timer or signal) - don't log completion
                return Suspended(reason=suspend_reason)
            else:
                # Flow completed normally - log step 0 completion
                result_bytes = pickle.dumps(result)
                await ctx.log_step_completion(
                    step=step_0,
                    return_value=result_bytes,
                    is_retryable=None,  # Success, not an error
                )
                return Completed(result=result)

        except _SuspendExecution:
            # Flow suspended via exception (Python-specific mechanism)
            # This is raised by pending_child.result() or pending_timer.wait()
            # when the flow needs to suspend.
            #
            # In Rust, futures can return Poll::Pending without completing.
            # In Python, we use this exception as a control flow mechanism.
            suspend_reason = ctx.take_suspend_reason()
            if suspend_reason is None:
                raise RuntimeError("_SuspendExecution raised but no suspend_reason set in context")
            return Suspended(reason=suspend_reason)

        except Exception as e:
            # Flow raised an exception
            # From Rust #[flow] macro lines 148-153: ONLY cache if result.is_ok()
            # Do NOT log step 0 completion for errors - the flow will be retried
            # and should execute from the beginning

            # Return the exception as the result (failed completion)
            # Worker will handle retry logic
            return Completed(result=e)

        finally:
            # Clean up task-local context
            EXECUTION_CONTEXT.reset(token_ctx)
            CALL_TYPE.reset(token_call_type)

    async def execute(self, entry_point: Callable[[T], Awaitable[R]]) -> FlowOutcome[R]:
        """
        Alias for run() - matches Rust naming.

        **Rust Reference**: `src/executor/instance.rs` line 91

        From Rust: `executor.execute(|f| f.run()).await`

        Args:
            entry_point: Async function that takes the flow

        Returns:
            FlowOutcome: Either Completed(result) or Suspended(reason)

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
    """
    Convenience function for one-off flow execution.

    From Dave Cheney: "Design APIs for their default use case"
    Most users just want to run a flow once, so provide a simple function.

    Args:
        flow: Flow instance to execute
        storage: Storage backend
        entry_point: Flow entry point function
        flow_id: Optional flow ID

    Returns:
        FlowOutcome: Either Completed(result) or Suspended(reason)

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
