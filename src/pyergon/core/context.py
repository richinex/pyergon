"""Task-local execution context for workflow state management.

Provides Context for tracking flow execution state without threading
it through every function call. Uses contextvars for task-local storage,
allowing multiple flows to execute concurrently without interference.

Design: Task-Local State (contextvars)
    Avoids explicit parameter passing while remaining safe for concurrent
    async execution. Each async task has its own isolated Context.
"""

import pickle
from contextvars import ContextVar
from threading import Lock
from typing import TYPE_CHECKING, Any, Optional, TypeVar

from pyergon.core.call_type import CallType
from pyergon.models import Invocation
from pyergon.storage.base import ExecutionLog

# Avoid circular import for type checking
if TYPE_CHECKING:
    from pyergon.executor.outcome import SuspendReason

T = TypeVar("T")

# Sentinel value for cache miss detection
#
# Problem: Optional[T] cannot distinguish between:
#   - Cache miss (no result found) → should return sentinel
#   - Cache hit with None result (step returned None) → should return None
#
# Solution: Sentinel Object Pattern (PEP 661)
#   Use unique object() instance tested with identity (is), not equality (==)
#
# Usage:
#   cached = await get_cached_result(...)
#   if cached is not _CACHE_MISS:
#       return cached  # Cache hit (may be None)
#   else:
#       # Cache miss - execute step
#
_CACHE_MISS = object()


# =============================================================================
# Task-Local Context Variables
# =============================================================================

EXECUTION_CONTEXT: ContextVar[Optional["Context"]] = ContextVar("execution_context", default=None)
"""Task-local Context for flow execution state.

Each async task has its own Context, allowing multiple flows to execute
concurrently without interference.

Usage:
    ```python
    ctx = Context(flow_id, storage)
    token = EXECUTION_CONTEXT.set(ctx)
    try:
        # Context available to all code in this task
        current_ctx = EXECUTION_CONTEXT.get()
        step = current_ctx.next_step()
    finally:
        EXECUTION_CONTEXT.reset(token)
    ```
"""

CALL_TYPE: ContextVar[CallType] = ContextVar("call_type", default=CallType.RUN)
"""Task-local execution mode (RUN, AWAIT, or RESUME).

Tracks whether the current step is first execution, waiting, or resuming.

Usage:
    ```python
    token = CALL_TYPE.set(CallType.RUN)
    try:
        if CALL_TYPE.get() == CallType.AWAIT:
            # Handle waiting case
            pass
    finally:
        CALL_TYPE.reset(token)
    ```
"""


# =============================================================================
# Context - Flow Execution State
# =============================================================================


class Context:
    """Manages execution state for a single flow instance.

    Provides atomic step counter, caching, logging operations, and
    integration with task-local context variables.

    Design: Single Responsibility
        Only manages execution state. Does not contain flow business
        logic or worker coordination logic.

    Usage:
        ```python
        ctx = Context(flow_id, storage)
        token = EXECUTION_CONTEXT.set(ctx)
        try:
            step = ctx.next_step()
            await ctx.log_step_start(...)
        finally:
            EXECUTION_CONTEXT.reset(token)
        ```
    """

    def __init__(self, flow_id: str, storage: ExecutionLog, class_name: str = "Flow"):
        """Initialize Context for flow execution.

        Args:
            flow_id: Unique identifier for this flow execution
            storage: Storage backend for persistence
            class_name: Name of the flow class (for logging)
        """
        self.flow_id = flow_id
        self.storage = storage
        self.class_name = class_name

        # Step counter - starts at -1, first call to next_step() returns 0
        # Python GIL makes simple int increment atomic (BEST_PRACTICES.md)
        self._step_counter = -1

        # Enclosing step - tracks parent step for invoke() coordination
        self._enclosing_step = -1  # -1 means no enclosing step

        # Suspension tracking (in-memory only, not persisted)
        # Uses threading.Lock to protect Optional[SuspendReason]
        # Reference: https://docs.python.org/3/library/threading.html#lock-objects
        self._suspend_reason: SuspendReason | None = None
        self._suspend_lock = Lock()

    def next_step(self) -> int:
        """Get next step number (atomic increment).

        This method increments the step counter atomically and returns
        the new value. Thread-safe for concurrent access.

        Returns:
            Next step number (0-indexed)

        Note: This is NOT async because it needs to be callable
        synchronously from decorator code. We use a simple increment
        with lock-free access for performance.
        """
        # Simple increment - Python GIL makes this atomic for single operations
        self._step_counter += 1
        return self._step_counter

    def current_step(self) -> int:
        """Get current step number without incrementing.

        Returns:
            Current step number
        """
        return self._step_counter

    def last_allocated_step(self) -> int:
        """
        Get the last allocated step number.

        This is the same as current_step() - returns the most recent
        step number allocated by next_step().

        Returns:
            Last allocated step number
        """
        return self._step_counter

    def set_enclosing_step(self, step: int) -> None:
        """Set the current enclosing step.

        Called by @step decorator to tell invoke().result() which parent step is executing.

        No lock needed - GIL protects simple int assignment in async context.
        """
        self._enclosing_step = step

    def get_enclosing_step(self) -> int | None:
        """Get the current enclosing step, if any.

        Returns:
            Step number if a @step wrapper is active, None if at top level

        No lock needed - GIL protects simple int read in async context.
        """
        step = self._enclosing_step
        return step if step >= 0 else None

    async def log_step_start(
        self,
        step: int,
        class_name: str,
        method_name: str,
        parameters: bytes,
        params_hash: int,
        delay: int | None = None,
        retry_policy: Any | None = None,
    ) -> None:
        """Log step execution start to storage.

        Creates an Invocation record with status=PENDING.

        Args:
            step: Step number
            class_name: Name of the flow class
            method_name: Name of the step method
            parameters: Serialized parameters (pickle)
            params_hash: Hash of parameters for cache key
            delay: Optional delay in milliseconds before execution
            retry_policy: Optional retry policy for this step
        """
        await self.storage.log_invocation_start(
            flow_id=self.flow_id,
            step=step,
            class_name=class_name,
            method_name=method_name,
            parameters=parameters,
            params_hash=params_hash,
            delay=delay,
            retry_policy=retry_policy,
        )

    async def log_step_completion(
        self, step: int, return_value: bytes, is_retryable: bool | None = None
    ) -> Invocation:
        """Log step execution completion to storage.

        Updates Invocation record with status=COMPLETE and return_value.

        Args:
            step: Step number
            return_value: Serialized result (pickle)
            is_retryable: If this is an error, whether it's retryable

        Returns:
            Updated Invocation record
        """
        invocation = await self.storage.log_invocation_completion(
            flow_id=self.flow_id, step=step, return_value=return_value
        )

        # Update is_retryable if specified (for error handling)
        if is_retryable is not None:
            await self.storage.update_is_retryable(
                flow_id=self.flow_id, step=step, is_retryable=is_retryable
            )

        return invocation

    async def get_cached_result(
        self, step: int, class_name: str, method_name: str, params_hash: int
    ) -> Any:
        """Get cached result for a step if it exists.

        Checks if a step with the same parameters has already been executed.
        Returns _CACHE_MISS sentinel if not cached, otherwise returns deserialized result
        (which may be None).

        Args:
            step: Step number
            class_name: Name of the flow class
            method_name: Name of the step method
            params_hash: Hash of parameters

        Returns:
            Cached result if exists and parameters match (can be None),
            _CACHE_MISS sentinel if no cache

        Note: If result is a cached exception, it will be raised, not returned.
        """
        from pyergon.executor.suspension_payload import SuspensionPayload
        from pyergon.models import InvocationStatus

        invocation = await self.storage.get_invocation(flow_id=self.flow_id, step=step)

        if invocation is None:
            return _CACHE_MISS

        # Validate invocation matches current execution (non-determinism detection)
        if invocation.params_hash != params_hash:
            # Parameters changed - this is non-deterministic behavior
            raise RuntimeError(
                f"Non-deterministic behavior detected: "
                f"Step {step} ({method_name}) called with different parameters. "
                f"Expected hash {invocation.params_hash}, got {params_hash}. "
                f"This indicates the workflow is non-deterministic."
            )

        # Return cached result if step is complete
        if invocation.status == InvocationStatus.COMPLETE:
            if invocation.return_value is None:
                return _CACHE_MISS

            # Defensive: Check if return_value is empty
            if len(invocation.return_value) == 0:
                return _CACHE_MISS

            try:
                result = pickle.loads(invocation.return_value)

                # If result is an exception, raise it (replay error)
                if isinstance(result, Exception):
                    raise result

                return result
            except Exception:
                # If deserialization fails, treat as no cache
                return _CACHE_MISS

        # Check if step is waiting for a signal (child flow completion)
        elif invocation.status == InvocationStatus.WAITING_FOR_SIGNAL:
            # Check if signal result is available (child flow completed)
            # This prevents re-executing step bodies when replaying flows with child invocations
            signal_name = invocation.timer_name or ""

            suspension_result = await self.storage.get_suspension_result(
                self.flow_id, step, signal_name
            )

            if suspension_result is not None:
                # Deserialize the suspension payload to get the result
                try:
                    payload = pickle.loads(suspension_result)
                    if isinstance(payload, dict):
                        # Handle dict-based SuspensionPayload
                        if payload.get("success", False):
                            # Return the successful result from the child
                            result = pickle.loads(payload["data"])
                            return result
                    elif isinstance(payload, SuspensionPayload):
                        # Handle dataclass-based SuspensionPayload
                        if payload.success:
                            result = pickle.loads(payload.data)
                            return result
                except Exception:
                    # Failed to deserialize signal payload
                    pass

        # Check if step is waiting for a timer
        elif invocation.status == InvocationStatus.WAITING_FOR_TIMER:
            # Timer suspension handling (similar to signals, but with empty data)
            # Check if timer has fired (SuspensionPayload with empty data)
            # Unlike signals (which carry the actual step result in payload.data),
            # timers only mark that the delay has passed (payload.data is empty)
            # The step needs to execute to produce its actual return value

            timer_key = invocation.timer_name or ""
            timer_result = await self.storage.get_suspension_result(self.flow_id, step, timer_key)

            if timer_result is not None:
                # Timer has fired - verify it's a valid SuspensionPayload
                try:
                    payload = pickle.loads(timer_result)
                    if isinstance(payload, dict):
                        # Handle dict-based payload
                        if payload.get("success", False):
                            # Timer fired successfully, but don't return cached result
                            # Let the step execute to produce its actual return value
                            # The schedule_timer() call will handle cleanup
                            pass
                    elif isinstance(payload, SuspensionPayload):
                        # Handle dataclass-based payload
                        if payload.success:
                            # Timer fired successfully
                            pass
                except Exception:
                    # Failed to deserialize timer payload
                    pass

            # Don't return cached result - let step execute to produce actual value
            # The schedule_timer() call will see the timer has fired and return
            # Then the step continues and produces its real return value

        return _CACHE_MISS

    async def update_step_retryability(self, step: int, is_retryable: bool) -> None:
        """Update whether a step error is retryable.

        Called by the @step decorator when a step returns an error
        to mark whether the error is retryable (transient) or not (permanent).

        Args:
            step: Step number
            is_retryable: Whether the error is retryable
        """
        await self.storage.update_is_retryable(
            flow_id=self.flow_id, step=step, is_retryable=is_retryable
        )

    # =========================================================================
    # SUSPENSION REASON MANAGEMENT
    # =========================================================================

    def set_suspend_reason(self, reason: "SuspendReason") -> None:
        """Set the suspension reason for this flow.

        Called by schedule_timer() or await_external_signal() to indicate
        the flow is suspending. The worker checks this after flow execution
        to determine if the flow suspended instead of completing.

        Thread Safety: Uses threading.Lock for atomic access.
        Reference: https://docs.python.org/3/library/threading.html#lock-objects

        Args:
            reason: Why the flow is suspending (timer or signal)

        Example:
            ```python
            from pyergon.executor.outcome import SuspendReason

            reason = SuspendReason(
                flow_id=ctx.flow_id,
                step=5,
                signal_name="payment_confirmed"
            )
            ctx.set_suspend_reason(reason)
            ```
        """
        with self._suspend_lock:
            self._suspend_reason = reason

    def take_suspend_reason(self) -> Optional["SuspendReason"]:
        """Take and clear the suspension reason (consuming it atomically).

        Called by the worker after flow execution to check if the flow
        suspended. Returns the reason if suspended, None if completed.

        Taking clears the value, ensuring each suspension is handled only once.

        Thread Safety: Lock ensures atomic read-and-clear operation.
        Reference: BEST_PRACTICES.md section on thread-safe state

        Returns:
            SuspendReason if flow suspended, None if completed normally

        Example:
            ```python
            result = await flow.run()
            suspend_reason = ctx.take_suspend_reason()

            if suspend_reason is not None:
                # Flow suspended
                handle_suspension(suspend_reason)
            else:
                # Flow completed
                handle_completion(result)
            ```
        """
        with self._suspend_lock:
            reason = self._suspend_reason
            self._suspend_reason = None
            return reason

    def has_suspend_reason(self) -> bool:
        """Check if a suspension reason is pending without consuming it.

        Used by @flow decorator to avoid caching "completion" when the flow
        is actually suspending. Unlike take_suspend_reason(), this does
        not consume the suspend reason.

        Returns:
            True if suspension reason is set, False otherwise

        Example:
            ```python
            if ctx.has_suspend_reason():
                # Don't cache completion, flow is suspending
                pass
            ```
        """
        with self._suspend_lock:
            return self._suspend_reason is not None

    def get_suspend_reason(self) -> Optional["SuspendReason"]:
        """Get a copy of the suspension reason without clearing it.

        Returns:
            SuspendReason if set, None otherwise

        Example:
            ```python
            reason = ctx.get_suspend_reason()
            if reason and reason.is_timer():
                print(f"Waiting for timer at step {reason.step}")
            ```
        """
        with self._suspend_lock:
            return self._suspend_reason

    def __repr__(self) -> str:
        """Readable representation for debugging."""
        return f"Context(flow_id={self.flow_id!r}, step_counter={self._step_counter})"


# =============================================================================
# Helper Functions
# =============================================================================


def get_current_context() -> Context | None:
    """Get the current Context from task-local storage.

    Returns:
        Current Context if set, None otherwise

    Usage:
        ctx = get_current_context()
        if ctx is not None:
            step = ctx.next_step()
    """
    return EXECUTION_CONTEXT.get()


def get_current_call_type() -> CallType:
    """Get the current CallType from task-local storage.

    Returns:
        Current CallType (defaults to RUN if not set)

    Usage:
        call_type = get_current_call_type()
        if call_type == CallType.AWAIT:
            # Handle waiting case
            pass
    """
    return CALL_TYPE.get()
