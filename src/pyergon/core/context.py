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

        # Enclosing step - tracks parent step for invoke() coordination (Rust line 64)
        self._enclosing_step = -1  # -1 means no enclosing step

        # Suspension tracking (in-memory only, not persisted)
        # From Rust: suspend_reason: Mutex<Option<SuspendReason>>
        # Python equivalent: threading.Lock protecting Optional[SuspendReason]
        # Reference: https://docs.python.org/3/library/threading.html#lock-objects
        self._suspend_reason: SuspendReason | None = None
        self._suspend_lock = Lock()

    def next_step(self) -> int:
        """
        Get next step number (atomic increment).

        This method increments the step counter atomically and returns
        the new value. Thread-safe for concurrent access.

        From Rust:
            pub fn next_step(&self) -> i32 {
                self.step_counter.fetch_add(1, Ordering::SeqCst) + 1
            }

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
        """
        Get current step number without incrementing.

        From Rust:
            pub fn current_step(&self) -> i32 {
                self.step_counter.load(Ordering::SeqCst)
            }

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
        """
        Set the current enclosing step.

        **Rust Reference**: set_enclosing_step (context.rs:477-480)

        Called by #[step] macro to tell invoke().result() which parent step is executing.

        No lock needed - GIL protects simple int assignment in async context.
        """
        self._enclosing_step = step

    def get_enclosing_step(self) -> int | None:
        """
        Get the current enclosing step, if any.

        **Rust Reference**: get_enclosing_step (context.rs:486-495)

        Returns:
            Step number if a #[step] wrapper is active, None if at top level

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
        """
        Log step execution start to storage.

        Creates an Invocation record with status=PENDING.

        From Rust:
            pub async fn log_step_start(&self, params: LogStepStartParams) -> Result<()>

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
        """
        Log step execution completion to storage.

        Updates Invocation record with status=COMPLETE and return_value.

        From Rust:
            pub async fn log_step_completion(
                &self, step: i32, return_value: &[u8]
            ) -> Result<Invocation>

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
        """
        Get cached result for a step if it exists.

        Checks if a step with the same parameters has already been executed.
        Returns _CACHE_MISS sentinel if not cached, otherwise returns deserialized result
        (which may be None).

        From Rust:
            pub async fn get_cached_result<R>(...) -> Result<Option<R>>
            - Ok(Some(value)) -> cache hit, return value (can be None)
            - Ok(None) -> cache miss, return _CACHE_MISS sentinel

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
        invocation = await self.storage.get_invocation(flow_id=self.flow_id, step=step)

        if invocation is None:
            return _CACHE_MISS

        if not invocation.is_complete:
            return _CACHE_MISS

        # Check if parameters match (non-determinism detection)
        if invocation.params_hash != params_hash:
            # Parameters changed - this is non-deterministic behavior
            # Rust would panic here, Python raises exception
            raise RuntimeError(
                f"Non-deterministic behavior detected: "
                f"Step {step} ({method_name}) called with different parameters. "
                f"Expected hash {invocation.params_hash}, got {params_hash}. "
                f"This indicates the workflow is non-deterministic."
            )

        # Deserialize and return cached result
        if invocation.return_value is None:
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

    async def update_step_retryability(self, step: int, is_retryable: bool) -> None:
        """
        Update whether a step error is retryable.

        **Rust Reference**: `src/executor/context.rs` line 222

        This is called by the step decorator when a step returns an error
        to mark whether the error is retryable (transient) or not (permanent).

        Args:
            step: Step number
            is_retryable: Whether the error is retryable
        """
        await self.storage.update_is_retryable(
            flow_id=self.flow_id, step=step, is_retryable=is_retryable
        )

    async def await_timer(self, step: int) -> None:
        """
        Wait for a timer to fire.

        Marks the step as WAITING_FOR_TIMER and waits for timer notification.

        From Rust:
            pub async fn await_timer(&self, step: i32) -> Result<()>

        Args:
            step: Step number that is waiting for timer
        """
        # Implementation will be completed when we add timer support
        # For now, this is a placeholder
        pass

    async def complete_timer(self, step: int) -> None:
        """
        Mark a timer as complete.

        Updates step status from WAITING_FOR_TIMER to PENDING for re-execution.

        From Rust:
            pub async fn complete_timer(&self, step: i32) -> Result<()>

        Args:
            step: Step number whose timer has fired
        """
        # Implementation will be completed when we add timer support
        pass

    # =========================================================================
    # SUSPENSION REASON MANAGEMENT
    # =========================================================================

    def set_suspend_reason(self, reason: "SuspendReason") -> None:
        """
        Set the suspension reason for this flow.

        **Rust Reference**: `src/executor/context.rs` lines 462-471

        Called by schedule_timer() or await_external_signal() to indicate
        the flow is suspending. The worker checks this after flow execution
        to determine if the flow suspended instead of completing.

        **Thread Safety**: Uses threading.Lock for atomic access
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
        """
        Take and clear the suspension reason (consuming it atomically).

        **Rust Reference**: `src/executor/context.rs` lines 534-539

        Called by the worker after flow execution to check if the flow
        suspended. Returns Some(reason) if suspended, None if completed.

        Taking clears the value, ensuring each suspension is handled only once.

        **Thread Safety**: Lock ensures atomic read-and-clear operation
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
        """
        Check if a suspension reason is pending without consuming it.

        **Rust Reference**: `src/executor/context.rs` lines 507-513

        Used by flow macro to avoid caching "completion" when the flow
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
        """
        Get a copy of the suspension reason without clearing it.

        **Rust Reference**: `src/executor/context.rs` lines 519-524

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
    """
    Get the current Context from task-local storage.

    From Rust:
        EXECUTION_CONTEXT.try_with(|ctx| ctx.clone())

    Returns:
        Current Context if set, None otherwise

    Usage:
        ctx = get_current_context()
        if ctx is not None:
            step = ctx.next_step()
    """
    return EXECUTION_CONTEXT.get()


def get_current_call_type() -> CallType:
    """
    Get the current CallType from task-local storage.

    From Rust:
        CALL_TYPE.try_with(|ct| *ct)

    Returns:
        Current CallType (defaults to RUN if not set)

    Usage:
        call_type = get_current_call_type()
        if call_type == CallType.AWAIT:
            # Handle waiting case
            pass
    """
    return CALL_TYPE.get()
