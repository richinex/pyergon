"""
ExecutionLog protocol - Abstract interface for storage backends.

Design Pattern: Adapter Pattern (Chapter 10)
ExecutionLog defines the target interface that all storage adapters implement.
Different storage backends (SQLite, Redis, Memory) adapt to this common interface.

Design Principle: Interface Segregation (SOLID)
Interface is focused - only essential persistence methods, no bloat.

Design Principle: Dependency Inversion (SOLID)
High-level modules (Worker, Scheduler) depend on this abstraction,
not on concrete storage implementations.

From Dave Cheney's Practical Go:
"Let functions define the behavior they require" - workers only need ExecutionLog,
not SqliteExecutionLog. This allows easy testing with InMemoryExecutionLog.

From Software Design Patterns:
Like the GameData interface in attendance_report_v2.py, this defines
what operations clients need without specifying implementation details.
"""

from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Protocol, runtime_checkable

from pyergon.models import (
    Invocation,
    RetryPolicy,
    ScheduledFlow,
    TaskStatus,
    TimerInfo,
)


@dataclass
class SignalInfo:
    """
    Information about a flow waiting for a signal.

    **Rust Reference**: `src/storage/mod.rs` lines 75-79

    Attributes:
        flow_id: Flow identifier waiting for signal
        step: Step number waiting for signal
        signal_name: Optional signal name (stored in timer_name field)
    """

    flow_id: str
    step: int
    signal_name: str | None = None


class StorageError(Exception):
    """
    Storage operation failed.

    From Dave Cheney: "Errors are values"
    Custom exception with context, not generic Exception.
    """

    pass


class ExecutionLog(ABC):
    """
    Abstract storage interface for durable execution.

    This interface defines the contract that all storage backends must fulfill.
    Clients program to this interface, not to concrete implementations.

    Pattern Benefits:
    - Open-Closed Principle: Add new storage backends without modifying clients
    - Testability: Easy to mock/stub with InMemoryExecutionLog
    - Flexibility: Switch storage at runtime (SQLite <-> Redis <-> Memory)

    From Dave Cheney:
    "Design APIs that are hard to misuse" - each method has one clear purpose,
    parameters are self-documenting, no ambiguous nil parameters.
    """

    # ========================================================================
    # Invocation Operations - Track individual step executions
    # ========================================================================

    @abstractmethod
    async def log_invocation_start(
        self,
        flow_id: str,
        step: int,
        class_name: str,
        method_name: str,
        parameters: bytes,
        params_hash: int,
        delay: int | None = None,
        retry_policy: RetryPolicy | None = None,
    ) -> Invocation:
        """
        Record the start of a step execution.

        RUST COMPLIANCE: Matches Rust ExecutionLog::log_invocation_start
        Updated to include all Invocation fields (id, params_hash, delay, retry_policy).

        From Dave Cheney: "Be wary of functions which take several parameters
        of the same type" - here different types (str, int, bytes) prevent
        accidental parameter swapping.

        Args:
            flow_id: Unique flow identifier
            step: Step number (0-indexed)
            class_name: Name of flow class
            method_name: Name of step method (was step_name)
            parameters: Serialized parameters (was params)
            params_hash: Hash of parameters for non-determinism detection
            delay: Optional delay before execution in milliseconds
            retry_policy: Optional retry policy for this invocation

        Returns:
            Created Invocation record

        Raises:
            StorageError: If database operation fails
        """
        pass

    @abstractmethod
    async def log_invocation_completion(
        self, flow_id: str, step: int, return_value: bytes, is_retryable: bool | None = None
    ) -> Invocation:
        """
        Record the completion of a step execution.

        RUST COMPLIANCE: Matches Rust ExecutionLog::log_invocation_completion
        Updated field names and added is_retryable for error handling.

        From Dave Cheney: "Only handle an error once" - this method either
        succeeds (returns Invocation) or raises (StorageError). Caller handles
        the error, this method doesn't log AND raise.

        Args:
            flow_id: Unique flow identifier
            step: Step number (0-indexed)
            return_value: Serialized result (was result)
            is_retryable: Whether cached error is retryable (3-state: None/True/False)

        Returns:
            Updated Invocation record

        Raises:
            StorageError: If invocation not found or update fails
        """
        pass

    @abstractmethod
    async def get_invocation(self, flow_id: str, step: int) -> Invocation | None:
        """
        Retrieve a specific step invocation.

        From Dave Cheney: "Make the zero value useful" - returns None
        when invocation doesn't exist (not raising exception), which is
        a valid state (step hasn't executed yet).

        Args:
            flow_id: Unique flow identifier
            step: Step number (0-indexed)

        Returns:
            Invocation if found, None otherwise
        """
        pass

    @abstractmethod
    async def get_incomplete_flows(self) -> list[Invocation]:
        """
        Get all flows with incomplete invocations.

        Used for recovery - find flows that were interrupted mid-execution.

        From Dave Cheney: "Return early rather than nesting deeply" -
        implementations should use guard clauses to filter results early.

        Returns:
            List of invocations for incomplete flows (may be empty)
        """
        pass

    # ========================================================================
    # Queue Operations - Distribute work across workers
    # ========================================================================

    @abstractmethod
    async def enqueue_flow(self, flow: ScheduledFlow) -> str:
        """
        Add a flow to the distributed work queue.

        **Rust Reference**: `src/storage/mod.rs` line 167
        ```rust
        async fn enqueue_flow(&self, flow: ScheduledFlow) -> Result<Uuid>
        ```

        This method accepts a complete ScheduledFlow object which includes:
        - flow_id, flow_type, flow_data (basic flow info)
        - status, retry_count, error_message (execution state)
        - parent_metadata (parent_flow_id, signal_token for child flows)
        - scheduled_for (for delayed execution/retry)
        - retry_policy (for retry behavior)

        The ScheduledFlow object contains all necessary metadata for execution,
        including parent-child relationships for Level 3 API.

        From Dave Cheney: "Design APIs that are hard to misuse" - by accepting
        a complete object instead of individual parameters, we ensure all required
        fields are present and prevent parameter order mistakes.

        Args:
            flow: Complete ScheduledFlow object to enqueue

        Returns:
            task_id: Unique task identifier for tracking (flow.task_id)

        Raises:
            StorageError: If enqueue fails
        """
        pass

    @abstractmethod
    async def dequeue_flow(self, worker_id: str) -> ScheduledFlow | None:
        """
        Claim and retrieve a pending flow from the queue.

        From Dave Cheney: "Design APIs for their default use case" -
        most common use is worker polling queue, so this single method
        does both claim AND retrieve (no separate claim() then get()).

        Implementation Note: Use IMMEDIATE transaction for pessimistic locking
        to prevent double-execution (from Rust ergon SqliteExecutionLog).

        Args:
            worker_id: Worker identifier (for tracking who claimed this)

        Returns:
            ScheduledFlow if work available, None if queue empty
        """
        pass

    @abstractmethod
    async def complete_flow(
        self, task_id: str, status: TaskStatus, error_message: str | None = None
    ) -> None:
        """
        Mark a flow task as complete or failed.

        Args:
            task_id: The task identifier
            status: The final status (COMPLETE, FAILED, or SUSPENDED)
            error_message: Optional error message if status is FAILED

        From Dave Cheney: "Only handle an error once" - this method either
        succeeds (returns None) or raises (StorageError). No return value
        needed since it's fire-and-forget update.

        Args:
            task_id: Task identifier from enqueue_flow()
            status: Final status (COMPLETE or FAILED)

        Raises:
            ValueError: If status is not terminal (COMPLETE/FAILED)
            StorageError: If update fails
        """
        pass

    @abstractmethod
    async def retry_flow(self, task_id: str, error_message: str, delay: timedelta) -> None:
        """
        Reschedule a failed flow for retry after a delay.

        **Rust Reference**: `src/storage/mod.rs` lines 245-260

        This method:
        1. Increments retry_count
        2. Sets error_message
        3. Sets status back to PENDING
        4. Clears locked_by
        5. Sets scheduled_for (current time + delay)
        6. Re-enqueues the flow

        Args:
            task_id: Task identifier from enqueue_flow()
            error_message: Error message from the failed attempt
            delay: How long to wait before retrying

        Raises:
            StorageError: If flow not found or update fails
        """
        pass

    @abstractmethod
    async def get_scheduled_flow(self, task_id: str) -> ScheduledFlow | None:
        """
        Retrieve a scheduled flow by task ID.

        From Dave Cheney: "Design APIs for their default use case"
        Returns Optional to make the "not found" case explicit without exceptions.

        Args:
            task_id: Task identifier from enqueue_flow()

        Returns:
            ScheduledFlow if found, None otherwise
        """
        pass

    # ========================================================================
    # Timer Operations - Durable timer coordination
    # ========================================================================

    @abstractmethod
    async def log_timer(
        self, flow_id: str, step: int, timer_fire_at: datetime, timer_name: str | None = None
    ) -> None:
        """
        Schedule a durable timer.

        RUST COMPLIANCE: Matches Rust ExecutionLog::log_timer
        Updated field names: fire_at → timer_fire_at, name → timer_name.
        Changed timer_name to Optional[str] to match 3-state semantic.

        Args:
            flow_id: Unique flow identifier
            step: Step number waiting for timer
            timer_fire_at: When timer should fire (was fire_at)
            timer_name: Optional timer name for debugging (was name)

        Raises:
            StorageError: If timer logging fails
        """
        pass

    @abstractmethod
    async def get_expired_timers(self, now: datetime) -> list[TimerInfo]:
        """
        Get all timers that have expired.

        **Rust Reference**: storage/mod.rs lines 66-71 (TimerInfo struct)

        Returns TimerInfo objects with flow_id, step, fire_at, and timer_name.
        This matches Rust's implementation and provides better debugging information.

        Args:
            now: Current time (explicit parameter for testability)

        Returns:
            List of TimerInfo objects for expired timers
        """
        pass

    @abstractmethod
    async def claim_timer(self, flow_id: str, step: int, worker_id: str) -> bool:
        """
        Claim an expired timer (optimistic concurrency).

        From Dave Cheney: "Design APIs that are hard to misuse" - returns
        bool instead of raising exception. False means another worker claimed
        it first, which is normal in distributed system.

        Implementation Note: Use UPDATE with WHERE status check for
        optimistic concurrency (from Rust ergon timer.rs).

        Args:
            flow_id: Unique flow identifier
            step: Step number waiting for timer
            worker_id: Worker identifier (for tracking)

        Returns:
            True if successfully claimed, False if already claimed
        """
        pass

    # ========================================================================
    # Signal Operations - External event coordination
    # ========================================================================

    @abstractmethod
    async def store_suspension_result(
        self, flow_id: str, step: int, suspension_key: str, result: bytes
    ) -> None:
        """
        Store suspension result data (timer or signal completion).

        **Rust Reference**: `src/storage/mod.rs` lines 403-410

        This is the underlying method for storing results that resume suspended flows.
        - For timers: empty bytes (timer just marks completion)
        - For signals: SuspensionPayload with child result

        Args:
            flow_id: Unique flow identifier
            step: Step number waiting for suspension
            suspension_key: Unique key (timer_name or signal_name)
            result: Serialized result data (SuspensionPayload)

        Raises:
            StorageError: If storage operation fails
        """
        pass

    @abstractmethod
    async def get_suspension_result(
        self, flow_id: str, step: int, suspension_key: str
    ) -> bytes | None:
        """
        Get suspension result data if available.

        **Rust Reference**: `src/storage/mod.rs` lines 424-433

        Returns None if the suspension hasn't completed yet.

        Args:
            flow_id: Unique flow identifier
            step: Step number waiting for suspension
            suspension_key: Unique key (timer_name or signal_name)

        Returns:
            Serialized result if available, None otherwise
        """
        pass

    @abstractmethod
    async def remove_suspension_result(self, flow_id: str, step: int, suspension_key: str) -> None:
        """
        Remove suspension result data after consuming it.

        **Rust Reference**: `src/storage/mod.rs` lines 442-448

        Clean up after the flow has resumed and consumed the result.

        Args:
            flow_id: Unique flow identifier
            step: Step number waiting for suspension
            suspension_key: Unique key (timer_name or signal_name)

        Raises:
            StorageError: If removal fails
        """
        pass

    @abstractmethod
    async def log_signal(self, flow_id: str, step: int, signal_name: str) -> None:
        """
        Mark an invocation as waiting for an external signal.

        **Rust Reference**: `src/storage/mod.rs` lines 500-505

        Updates the invocation status to WAITING_FOR_SIGNAL and stores the signal name.
        Called by child flow invocation before scheduling the child.

        CRITICAL: This must be called BEFORE scheduling the child flow to prevent
        race conditions where the child completes before the parent is marked as waiting.

        Args:
            flow_id: Parent flow identifier
            step: Step number waiting for signal
            signal_name: Signal to wait for (typically child_flow_id)

        Raises:
            StorageError: If update fails
        """
        pass

    async def get_waiting_signals(self) -> list[SignalInfo]:
        """
        Get all flows waiting for external signals.

        **Rust Reference**: `src/storage/mod.rs` lines 496-498

        Returns flows where:
        - execution_log.status = 'WAITING_FOR_SIGNAL'
        - flow_queue.status = 'SUSPENDED'

        This is used by Worker signal processing to poll for signals
        and deliver them to waiting flows.

        Returns:
            List of SignalInfo containing flow_id, step, and signal_name

        Default Implementation:
            Returns empty list. Backends supporting signals should override.
        """
        return []

    @abstractmethod
    async def update_is_retryable(self, flow_id: str, step: int, is_retryable: bool) -> None:
        """
        Update the is_retryable flag for a specific step invocation.

        **Rust Reference**: `src/storage/mod.rs` line 150

        This method is called when a step returns an error to mark whether
        the error is retryable (transient) or not (permanent).

        Args:
            flow_id: Flow identifier
            step: Step number
            is_retryable: True if error is retryable, False if permanent

        Example:
            ```python
            # Mark step 5's error as non-retryable (permanent failure)
            await storage.update_is_retryable(flow_id, 5, False)
            ```
        """
        pass

    @abstractmethod
    async def has_non_retryable_error(self, flow_id: str) -> bool:
        """
        Check if any step in the flow has a non-retryable error.

        **Rust Reference**: `src/storage/sqlite.rs` lines 531-545

        Used by retry logic to determine if a flow should stop retrying.
        If any step has is_retryable=False, the entire flow should not retry.

        Args:
            flow_id: Flow identifier

        Returns:
            True if any step has is_retryable=False, False otherwise

        Example:
            ```python
            # Check if flow should stop retrying
            if await storage.has_non_retryable_error(flow_id):
                # Don't retry - permanent failure
                await storage.complete_flow(task_id, TaskStatus.FAILED)
            ```
        """
        pass

    # ========================================================================
    # Flow Resume Operations
    # ========================================================================

    @abstractmethod
    async def resume_flow(self, flow_id: str) -> bool:
        """
        Resume a suspended flow by changing status SUSPENDED → PENDING.

        **Rust Reference**: `src/storage/mod.rs` lines 352-357
        **Rust Reference**: `src/storage/memory.rs` lines 626-662

        This method atomically:
        1. Checks if flow is SUSPENDED
        2. Changes status to PENDING
        3. Re-enqueues to work queue

        Called by:
        - Timer processing when timer fires
        - Signal processing when child completes
        - handle_suspended_flow() to check race conditions

        Args:
            flow_id: Flow identifier to resume

        Returns:
            True if flow was resumed (was SUSPENDED), False otherwise

        Example:
            ```python
            resumed = await storage.resume_flow(flow_id)
            if resumed:
                logger.info(f"Resumed flow {flow_id}")
            ```
        """
        pass

    @abstractmethod
    async def get_invocations_for_flow(self, flow_id: str) -> list[Invocation]:
        """
        Get all invocations (steps) for a specific flow.

        **Rust Reference**: `src/storage/mod.rs` lines 164-172

        Used by handle_suspended_flow() to check if signals/timers arrived
        during suspension (race condition handling).

        Args:
            flow_id: Flow identifier

        Returns:
            List of all invocations for this flow

        Example:
            ```python
            invocations = await storage.get_invocations_for_flow(flow_id)
            for inv in invocations:
                if inv.status == InvocationStatus.WAITING_FOR_SIGNAL:
                    # Check if signal arrived
                    pass
            ```
        """
        pass

    @abstractmethod
    async def get_next_timer_fire_time(self) -> datetime | None:
        """
        Get the earliest timer fire time across all flows.

        **Rust Reference**: `src/storage/mod.rs` lines 306-308
        **Rust Reference**: `src/storage/memory.rs` lines 456-476

        Used by Worker to calculate sleep duration for event-driven timer
        processing. Instead of polling every N seconds, Worker sleeps until
        the next timer fires (or new work arrives).

        Returns:
            datetime of next timer, or None if no timers pending

        Implementation:
            Query all invocations with status=WAITING_FOR_TIMER,
            return MIN(timer_fire_at)

        Example:
            ```python
            next_fire = await storage.get_next_timer_fire_time()
            if next_fire:
                sleep_duration = (next_fire - datetime.now()).total_seconds()
                await asyncio.sleep(max(0, sleep_duration))
            else:
                # No timers, wait indefinitely
                await asyncio.Event().wait()
            ```
        """
        pass

    # ========================================================================
    # Utility Operations
    # ========================================================================

    @abstractmethod
    async def reset(self) -> None:
        """
        Clear all data (for testing/demos).

        From Dave Cheney: "Make the zero value useful" - after reset(),
        storage is in initial state (empty but functional).

        Warning: Destructive operation - only use in testing!
        """
        pass

    @abstractmethod
    async def close(self) -> None:
        """
        Close storage connections and clean up resources.

        From Dave Cheney: "Never start a goroutine without knowing when it
        will stop" - storage connections must be explicitly closed, not
        left to garbage collection.

        Use this in a context manager or try/finally block.
        """
        pass


# =============================================================================
# Notification Source Protocols - Event-Driven Storage
# =============================================================================


@runtime_checkable
class WorkNotificationSource(Protocol):
    """
    Protocol for storage backends that support event-driven work notifications.

    **Rust Reference**: `src/storage/mod.rs` lines 633-638

    This protocol enables workers to wait for work instead of polling,
    improving latency and reducing CPU usage.

    **Pattern**: Interface Segregation Principle (SOLID)
    Not all storage backends need to implement this. Storage backends
    that can't provide efficient notifications (e.g., remote REST API)
    can skip this protocol and workers will fall back to polling.

    **Contract**:
    Implementations must:
    1. Maintain an asyncio.Event for work notifications
    2. Call `event.set()` when work becomes available (enqueue_flow, resume_flow)
    3. Workers will `await event.wait()` and then `event.clear()`

    **From Rust**:
    ```rust
    pub trait WorkNotificationSource: ExecutionLog {
        fn work_notify(&self) -> &Arc<tokio::sync::Notify>;
    }
    ```

    **Python Equivalent**:
    Unlike Rust's compile-time trait bounds, Python uses runtime
    protocol checking with isinstance(). This provides flexibility
    at the cost of later error detection.

    **Usage**:
    ```python
    from pyergon.storage import WorkNotificationSource

    class MyStorage(ExecutionLog):
        def __init__(self):
            self._work_notify = asyncio.Event()

        def work_notify(self) -> asyncio.Event:
            return self._work_notify

        async def enqueue_flow(self, flow):
            # ... store in database ...
            self._work_notify.set()  # Wake one worker
            return task_id
    ```

    **Worker Usage**:
    ```python
    if isinstance(storage, WorkNotificationSource):
        # Fast path: event-driven
        await asyncio.wait_for(
            storage.work_notify().wait(),
            timeout=poll_interval
        )
    else:
        # Fallback: polling
        await asyncio.sleep(poll_interval)
    ```
    """

    def work_notify(self) -> asyncio.Event:
        """
        Return event that signals when work becomes available.

        Workers use this to wait for work instead of sleeping.
        Storage backend signals this event when new work is enqueued.

        **From Rust**: `fn work_notify(&self) -> &Arc<Notify>`

        Returns:
            asyncio.Event that workers wait on
        """
        ...


@runtime_checkable
class TimerNotificationSource(Protocol):
    """
    Protocol for storage backends that support event-driven timer notifications.

    **Rust Reference**: `src/storage/mod.rs` lines 676-681

    This protocol enables timer processing to be event-driven rather than
    polling-based, improving latency and reducing unnecessary database queries.

    **Pattern**: Interface Segregation Principle (SOLID)
    Not all storage backends need timer notifications. This protocol
    separates timer notification capability from base ExecutionLog.

    **Contract**:
    Implementations must call `event.set()` when:
    - A new timer is scheduled (log_timer)
    - A timer's fire time is updated
    - A timer is claimed (may need to recalculate next wake time)

    **From Rust**:
    ```rust
    pub trait TimerNotificationSource: ExecutionLog {
        fn timer_notify(&self) -> &Arc<tokio::sync::Notify>;
    }
    ```

    **Python Equivalent**:
    ```python
    class MyStorage(ExecutionLog):
        def __init__(self):
            self._timer_notify = asyncio.Event()

        def timer_notify(self) -> asyncio.Event:
            return self._timer_notify

        async def log_timer(self, flow_id, step, fire_at, name):
            # ... store timer ...
            self._timer_notify.set()  # Wake timer processor
    ```

    **Timer Processor Usage**:
    ```python
    if isinstance(storage, TimerNotificationSource):
        # Event-driven: wake on timer changes
        next_fire = await storage.get_next_timer_fire_time()
        if next_fire:
            sleep_until = (next_fire - datetime.now()).total_seconds()
            await asyncio.wait_for(
                storage.timer_notify().wait(),
                timeout=sleep_until
            )
    ```
    """

    def timer_notify(self) -> asyncio.Event:
        """
        Return event that signals when timer state changes.

        Timer processors use this to wait for timer events instead of polling.
        Storage backend signals this when timers are scheduled or fired.

        **From Rust**: `fn timer_notify(&self) -> &Arc<Notify>`

        Returns:
            asyncio.Event that timer processors wait on
        """
        ...
