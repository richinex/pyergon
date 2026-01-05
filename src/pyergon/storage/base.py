"""Abstract storage interface for durable execution persistence.

Defines the contract that all storage backends must fulfill, enabling
workers and schedulers to operate independently of storage implementation.

Design: Adapter Pattern
    ExecutionLog is the target interface. Different storage backends
    (SQLite, Redis, Memory) adapt to this common interface.

Design: Interface Segregation (SOLID)
    Focused interface with only essential persistence methods.

Design: Dependency Inversion (SOLID)
    High-level modules (Worker, Scheduler) depend on this abstraction,
    not on concrete implementations. This enables easy testing with
    InMemoryExecutionLog and flexible deployment with production backends.
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
    """Information about a flow waiting for an external signal.

    Returned by get_waiting_signals() to identify flows suspended
    while waiting for signals to arrive.

    Attributes:
        flow_id: Flow identifier waiting for signal
        step: Step number waiting for signal
        signal_name: Optional signal name for identification
    """

    flow_id: str
    step: int
    signal_name: str | None = None


class StorageError(Exception):
    """Storage operation failed.

    Custom exception with context for storage-related failures.
    """

    pass


class ExecutionLog(ABC):
    """Abstract interface for durable execution storage.

    Defines the contract that all storage backends must fulfill.
    Clients program to this interface, not to concrete implementations.

    Benefits:
        - Open-Closed: Add storage backends without modifying clients
        - Testability: Easy mocking with InMemoryExecutionLog
        - Flexibility: Switch storage at runtime (SQLite/Redis/Memory)
        - API Safety: Each method has one clear purpose with
          self-documenting parameters
    """

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
        """Record the start of a step execution.

        Args:
            flow_id: Unique flow identifier
            step: Step number (0-indexed)
            class_name: Name of flow class
            method_name: Name of step method
            parameters: Serialized parameters (pickle format)
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
        """Record the completion of a step execution.

        Updates the invocation with the result and marks it complete.
        Either succeeds (returns Invocation) or raises (StorageError).

        Args:
            flow_id: Unique flow identifier
            step: Step number (0-indexed)
            return_value: Serialized result (pickle format)
            is_retryable: Whether cached error is retryable (3-state: None/True/False)

        Returns:
            Updated Invocation record

        Raises:
            StorageError: If invocation not found or update fails
        """
        pass

    @abstractmethod
    async def get_invocation(self, flow_id: str, step: int) -> Invocation | None:
        """Retrieve a specific step invocation.

        Returns None when invocation doesn't exist (valid state when
        step hasn't executed yet).

        Args:
            flow_id: Unique flow identifier
            step: Step number (0-indexed)

        Returns:
            Invocation if found, None otherwise
        """
        pass

    @abstractmethod
    async def get_incomplete_flows(self) -> list[Invocation]:
        """Get all flows with incomplete invocations.

        Used for recovery - find flows that were interrupted mid-execution.

        Returns:
            List of invocations for incomplete flows (may be empty)
        """
        pass

    @abstractmethod
    async def enqueue_flow(self, flow: ScheduledFlow) -> str:
        """Add a flow to the distributed work queue.

        This method accepts a complete ScheduledFlow object which includes:
        - flow_id, flow_type, flow_data (basic flow info)
        - status, retry_count, error_message (execution state)
        - parent_metadata (parent_flow_id, signal_token for child flows)
        - scheduled_for (for delayed execution/retry)
        - retry_policy (for retry behavior)

        The ScheduledFlow object contains all necessary metadata for execution,
        including parent-child relationships for child flow invocation.

        Accepting a complete object ensures all required fields are present
        and prevents parameter order mistakes.

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
        """Claim and retrieve a pending flow from the queue.

        Single method does both claim AND retrieve (no separate operations).

        Implementation Note: Use pessimistic locking (e.g., SQLite IMMEDIATE
        transaction) to prevent double-execution.

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
        """Mark a flow task as complete or failed.

        Either succeeds (returns None) or raises (StorageError).

        Args:
            task_id: Task identifier from enqueue_flow()
            status: Final status (COMPLETE, FAILED, or SUSPENDED)
            error_message: Optional error message if status is FAILED

        Raises:
            ValueError: If status is not terminal (COMPLETE/FAILED)
            StorageError: If update fails
        """
        pass

    @abstractmethod
    async def retry_flow(self, task_id: str, error_message: str, delay: timedelta) -> None:
        """Reschedule a failed flow for retry after a delay.

        This method atomically:
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
        """Retrieve a scheduled flow by task ID.

        Returns None to make the "not found" case explicit without exceptions.

        Args:
            task_id: Task identifier from enqueue_flow()

        Returns:
            ScheduledFlow if found, None otherwise
        """
        pass

    @abstractmethod
    async def log_timer(
        self, flow_id: str, step: int, timer_fire_at: datetime, timer_name: str | None = None
    ) -> None:
        """Schedule a durable timer.

        Creates a timer that will trigger workflow resumption at the specified time.

        Args:
            flow_id: Unique flow identifier
            step: Step number waiting for timer
            timer_fire_at: When timer should fire
            timer_name: Optional timer name for debugging

        Raises:
            StorageError: If timer logging fails
        """
        pass

    @abstractmethod
    async def get_expired_timers(self, now: datetime) -> list[TimerInfo]:
        """Get all timers that have expired.

        Returns TimerInfo objects with flow_id, step, fire_at, and timer_name
        for better debugging information.

        Args:
            now: Current time (explicit parameter for testability)

        Returns:
            List of TimerInfo objects for expired timers
        """
        pass

    async def move_ready_delayed_tasks(self) -> int:
        """Move delayed tasks that are ready to execute to the ready queue.

        This is a maintenance operation for distributed backends (e.g., Redis)
        that use separate queues for delayed tasks. For local backends (in-memory,
        SQLite), this is a no-op since delayed tasks are already in the queue
        and filtered at dequeue time based on scheduled_for timestamp.

        Returns:
            Number of tasks moved to ready queue
        """
        return 0  # No-op by default

    @abstractmethod
    async def claim_timer(self, flow_id: str, step: int) -> bool:
        """Claim an expired timer (optimistic concurrency).

        Returns bool instead of raising exception. False means another
        worker claimed it first, which is normal in distributed system.

        Implementation Note: Use UPDATE with WHERE status check for
        optimistic concurrency control.

        Args:
            flow_id: Unique flow identifier
            step: Step number waiting for timer
            worker_id: Worker identifier (for tracking)

        Returns:
            True if successfully claimed, False if already claimed
        """
        pass

    @abstractmethod
    async def store_suspension_result(
        self, flow_id: str, step: int, suspension_key: str, result: bytes
    ) -> None:
        """Store suspension result data (timer or signal completion).

        This is the underlying method for storing results that resume suspended flows:
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
        """Get suspension result data if available.

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
        """Remove suspension result data after consuming it.

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
        """Mark an invocation as waiting for an external signal.

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
        """Get all flows waiting for external signals.

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
        """Update the is_retryable flag for a specific step invocation.

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
        """Check if any step in the flow has a non-retryable error.

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

    @abstractmethod
    async def resume_flow(self, flow_id: str) -> bool:
        """Resume a suspended flow by changing status SUSPENDED → PENDING.

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
        """Get all invocations (steps) for a specific flow.

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
        """Get the earliest timer fire time across all flows.

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

    @abstractmethod
    async def reset(self) -> None:
        """Clear all data (for testing/demos).

        After reset(), storage is in initial state (empty but functional).

        Warning: Destructive operation - only use in testing!
        """
        pass

    @abstractmethod
    async def close(self) -> None:
        """Close storage connections and clean up resources.

        Storage connections must be explicitly closed.
        Use this in a context manager or try/finally block.
        """
        pass


@runtime_checkable
class WorkNotificationSource(Protocol):
    """Protocol for storage backends that support event-driven work notifications.

    This protocol enables workers to wait for work instead of polling,
    improving latency and reducing CPU usage.

    Pattern: Interface Segregation Principle (SOLID)
    Not all storage backends need to implement this. Storage backends
    that can't provide efficient notifications (e.g., remote REST API)
    can skip this protocol and workers will fall back to polling.

    Contract:
    Implementations must:
    1. Maintain an asyncio.Event for work notifications
    2. Call `event.set()` when work becomes available (enqueue_flow, resume_flow)
    3. Workers will `await event.wait()` and then `event.clear()`

    Python vs Other Languages:
    Unlike compile-time trait bounds, Python uses runtime protocol checking
    with isinstance(). This provides flexibility at the cost of later error
    detection.

    Usage:
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

    Worker Usage:
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
        """Return event that signals when work becomes available.

        Workers use this to wait for work instead of sleeping.
        Storage backend signals this event when new work is enqueued.

        Returns:
            asyncio.Event that workers wait on
        """
        ...


@runtime_checkable
class TimerNotificationSource(Protocol):
    """Protocol for storage backends that support event-driven timer notifications.

    This protocol enables timer processing to be event-driven rather than
    polling-based, improving latency and reducing unnecessary database queries.

    Pattern: Interface Segregation Principle (SOLID)
    Not all storage backends need timer notifications. This protocol
    separates timer notification capability from base ExecutionLog.

    Contract:
    Implementations must call `event.set()` when:
    - A new timer is scheduled (log_timer)
    - A timer's fire time is updated
    - A timer is claimed (may need to recalculate next wake time)

    Usage:
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

    Timer Processor Usage:
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
        """Return event that signals when timer state changes.

        Timer processors use this to wait for timer events instead of polling.
        Storage backend signals this when timers are scheduled or fired.

        Returns:
            asyncio.Event that timer processors wait on
        """
        ...
