"""In-memory storage implementation for pyergon.

Design Pattern: Adapter Pattern
InMemoryExecutionLog adapts in-memory dictionaries to ExecutionLog interface.

Instance is immediately usable after __init__.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta

from uuid_extensions import uuid7

from pyergon.models import (
    Invocation,
    InvocationStatus,
    RetryPolicy,
    ScheduledFlow,
    TaskStatus,
    TimerInfo,
)
from pyergon.storage.base import ExecutionLog, StorageError


class InMemoryExecutionLog(ExecutionLog):
    """In-memory storage for testing.

    Can be substituted for SqliteExecutionLog without changing client code.

    Usage:
        storage = InMemoryExecutionLog()
        await storage.log_invocation_start(...)
    """

    def __init__(self):
        """Initialize in-memory storage with notification support.

        Creates notification events for:
        - work_notify: Wake workers when work becomes available
        - timer_notify: Wake timer processors when timer state changes
        - status_notify: Notify flow status changes (for future use)
        """
        # Storage: {(flow_id, step): Invocation}
        self._invocations: dict[tuple[str, int], Invocation] = {}

        # Storage: {task_id: ScheduledFlow}
        self._scheduled_flows: dict[str, ScheduledFlow] = {}

        # Storage: {(flow_id, step, suspension_key): bytes}
        # For storing suspension results (timer/signal completions)
        self._suspension_results: dict[tuple[str, int, str], bytes] = {}

        # Lock for thread-safety
        self._lock = asyncio.Lock()

        # Notification events (implements WorkNotificationSource and TimerNotificationSource)
        self._work_notify = asyncio.Event()
        self._timer_notify = asyncio.Event()
        self._status_notify = asyncio.Event()

    def __repr__(self) -> str:
        """Return string representation of storage instance."""
        return "InMemoryExecutionLog"

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

        This method checks if an invocation already exists:
        - If COMPLETE: skip, don't overwrite cached result
        - If WAITING_FOR_SIGNAL: skip, don't overwrite signal state
        - Otherwise: overwrite with new invocation
        """
        async with self._lock:
            key = (flow_id, step)

            # Check if invocation already exists and skip if Complete,
            # WaitingForSignal, or WaitingForTimer
            attempts = 1
            inv_id = str(uuid7())

            if key in self._invocations:
                existing = self._invocations[key]
                if existing.status == InvocationStatus.COMPLETE:
                    # Don't overwrite cached results during replay
                    return existing
                if existing.status == InvocationStatus.WAITING_FOR_SIGNAL:
                    # Don't overwrite signal state during replay
                    return existing
                if existing.status == InvocationStatus.WAITING_FOR_TIMER:
                    # Don't overwrite timer state during replay
                    return existing

                # Retrying a failed/pending invocation - increment attempts
                attempts = existing.attempts + 1
                inv_id = existing.id

            # Create new invocation (or overwrite non-Complete/non-WaitingForSignal)
            now = datetime.now()

            invocation = Invocation(
                id=inv_id,
                flow_id=flow_id,
                step=step,
                timestamp=now,
                class_name=class_name,
                method_name=method_name,
                status=InvocationStatus.PENDING,
                attempts=attempts,
                parameters=parameters,
                params_hash=params_hash,
                return_value=None,
                delay=delay,
                retry_policy=retry_policy,
                is_retryable=None,
                timer_fire_at=None,
                timer_name=None,
                updated_at=now,
            )

            self._invocations[key] = invocation
            return invocation

    async def log_invocation_completion(
        self, flow_id: str, step: int, return_value: bytes, is_retryable: bool | None = None
    ) -> Invocation:
        """Record the completion of a step execution."""
        async with self._lock:
            key = (flow_id, step)

            if key not in self._invocations:
                raise StorageError(f"Invocation not found: flow_id={flow_id}, step={step}")

            old = self._invocations[key]

            # Create updated invocation
            updated = Invocation(
                id=old.id,
                flow_id=old.flow_id,
                step=old.step,
                timestamp=old.timestamp,
                class_name=old.class_name,
                method_name=old.method_name,
                status=InvocationStatus.COMPLETE,
                attempts=old.attempts + 1,
                parameters=old.parameters,
                params_hash=old.params_hash,
                return_value=return_value,
                delay=old.delay,
                retry_policy=old.retry_policy,
                is_retryable=is_retryable,
                timer_fire_at=old.timer_fire_at,
                timer_name=old.timer_name,
                updated_at=datetime.now(),
            )

            self._invocations[key] = updated
            return updated

    async def get_invocation(self, flow_id: str, step: int) -> Invocation | None:
        """Retrieve a specific step invocation."""
        async with self._lock:
            return self._invocations.get((flow_id, step))

    async def get_invocations_for_flow(self, flow_id: str) -> list[Invocation]:
        """Get all invocations for a flow."""
        async with self._lock:
            return [inv for (fid, _), inv in self._invocations.items() if fid == flow_id]

    async def get_incomplete_flows(self) -> list[Invocation]:
        """Get all flows with incomplete invocations."""
        async with self._lock:
            # Return invocations that are not complete
            return [inv for inv in self._invocations.values() if not inv.is_complete]

    async def has_non_retryable_error(self, flow_id: str) -> bool:
        """Check if flow has non-retryable error."""
        async with self._lock:
            for (fid, _), inv in self._invocations.items():
                if fid == flow_id and inv.is_retryable is False:
                    return True
            return False

    async def update_is_retryable(self, flow_id: str, step: int, is_retryable: bool) -> None:
        """Update is_retryable field."""
        async with self._lock:
            key = (flow_id, step)
            if key in self._invocations:
                old = self._invocations[key]
                updated = Invocation(
                    id=old.id,
                    flow_id=old.flow_id,
                    step=old.step,
                    timestamp=old.timestamp,
                    class_name=old.class_name,
                    method_name=old.method_name,
                    status=old.status,
                    attempts=old.attempts,
                    parameters=old.parameters,
                    params_hash=old.params_hash,
                    return_value=old.return_value,
                    delay=old.delay,
                    retry_policy=old.retry_policy,
                    is_retryable=is_retryable,
                    timer_fire_at=old.timer_fire_at,
                    timer_name=old.timer_name,
                    updated_at=datetime.now(),
                )
                self._invocations[key] = updated

    async def enqueue_flow(self, flow: ScheduledFlow) -> str:
        """Add a flow to the distributed work queue.

        Accepts a complete ScheduledFlow object which includes parent_metadata
        for child flow invocation.
        """
        async with self._lock:
            # Store the flow object directly
            # The flow object already contains all necessary fields including parent_metadata
            self._scheduled_flows[flow.task_id] = flow

            # Wake up one waiting worker if this is an immediate (non-delayed) flow
            if flow.scheduled_for is None:
                self._work_notify.set()
                self._work_notify.clear()  # Reset for next notification

            return flow.task_id

    async def dequeue_flow(self, worker_id: str) -> ScheduledFlow | None:
        """Claim and retrieve a pending flow from queue.

        Only dequeues flows that are:
        1. Status == PENDING
        2. scheduled_for is None OR scheduled_for <= now (ready to execute)
        """
        async with self._lock:
            now = datetime.now()

            # Find first pending flow that's ready to execute
            for task_id, flow in self._scheduled_flows.items():
                if flow.status == TaskStatus.PENDING:
                    # Check if flow is scheduled for the future
                    if flow.scheduled_for is not None and flow.scheduled_for > now:
                        # Not ready yet - skip this flow
                        continue

                    # Flow is ready - claim it
                    updated = ScheduledFlow(
                        task_id=flow.task_id,
                        flow_id=flow.flow_id,
                        flow_type=flow.flow_type,
                        flow_data=flow.flow_data,
                        status=TaskStatus.RUNNING,
                        locked_by=worker_id,
                        retry_count=flow.retry_count,
                        created_at=flow.created_at,
                        updated_at=now,
                        error_message=flow.error_message,
                        claimed_at=now,
                        completed_at=flow.completed_at,
                        scheduled_for=flow.scheduled_for,
                        parent_metadata=flow.parent_metadata,
                        retry_policy=flow.retry_policy,
                    )
                    self._scheduled_flows[task_id] = updated
                    return updated

            return None

    async def complete_flow(
        self, task_id: str, status: TaskStatus, error_message: str | None = None
    ) -> None:
        """Update flow task status.

        Despite the name "complete_flow", this method updates the status to ANY value,
        including SUSPENDED (not just terminal states). The name comes from the most
        common use case (marking flows COMPLETE/FAILED), but it's actually a general
        status update method.

        Args:
            task_id: The task identifier
            status: The final status (COMPLETE, FAILED, or SUSPENDED)
            error_message: Optional error message if status is FAILED
        """
        async with self._lock:
            if task_id not in self._scheduled_flows:
                raise StorageError(f"Flow not found: task_id={task_id}")

            flow = self._scheduled_flows[task_id]
            now = datetime.now()

            # Set completed_at only for terminal statuses
            completed_at = now if status.is_terminal else flow.completed_at

            updated = ScheduledFlow(
                task_id=flow.task_id,
                flow_id=flow.flow_id,
                flow_type=flow.flow_type,
                flow_data=flow.flow_data,
                status=status,  # Accept ANY status (including SUSPENDED)
                locked_by=flow.locked_by,
                retry_count=flow.retry_count,
                created_at=flow.created_at,
                updated_at=now,
                error_message=error_message if error_message is not None else flow.error_message,
                claimed_at=flow.claimed_at,
                completed_at=completed_at,
                scheduled_for=flow.scheduled_for,
                parent_metadata=flow.parent_metadata,
                retry_policy=flow.retry_policy,
            )

            self._scheduled_flows[task_id] = updated

            # Notify any waiters that a flow status changed
            self._status_notify.set()
            self._status_notify.clear()  # Reset for next notification

    async def retry_flow(self, task_id: str, error_message: str, delay: timedelta) -> None:
        """Reschedule a failed flow for retry after a delay.

        This method:
        1. Increments retry_count
        2. Sets error_message
        3. Sets status back to PENDING
        4. Clears locked_by
        5. Sets scheduled_for (current time + delay)
        6. Re-enqueues the flow
        """
        async with self._lock:
            if task_id not in self._scheduled_flows:
                raise StorageError(f"Flow not found: task_id={task_id}")

            flow = self._scheduled_flows[task_id]
            now = datetime.now()
            scheduled_for = now + delay

            # Update flow for retry
            updated = ScheduledFlow(
                task_id=flow.task_id,
                flow_id=flow.flow_id,
                flow_type=flow.flow_type,
                flow_data=flow.flow_data,
                status=TaskStatus.PENDING,  # Back to pending
                locked_by=None,  # Clear lock
                retry_count=flow.retry_count + 1,  # Increment
                created_at=flow.created_at,
                updated_at=now,
                error_message=error_message,  # Set error
                claimed_at=None,
                completed_at=None,
                scheduled_for=scheduled_for,  # Schedule for future
                parent_metadata=flow.parent_metadata,
                retry_policy=flow.retry_policy,
            )

            self._scheduled_flows[task_id] = updated

            # Notify workers that new work is available (or will be available soon)
            self._work_notify.set()
            self._work_notify.clear()  # Reset for next notification

    async def get_scheduled_flow(self, task_id: str) -> ScheduledFlow | None:
        """Retrieve a scheduled flow by task ID."""
        async with self._lock:
            return self._scheduled_flows.get(task_id)

    async def log_timer(
        self, flow_id: str, step: int, timer_fire_at: datetime, timer_name: str | None = None
    ) -> None:
        """Schedule a durable timer."""
        async with self._lock:
            key = (flow_id, step)
            if key in self._invocations:
                old = self._invocations[key]
                updated = Invocation(
                    id=old.id,
                    flow_id=old.flow_id,
                    step=old.step,
                    timestamp=old.timestamp,
                    class_name=old.class_name,
                    method_name=old.method_name,
                    status=InvocationStatus.WAITING_FOR_TIMER,
                    attempts=old.attempts,
                    parameters=old.parameters,
                    params_hash=old.params_hash,
                    return_value=old.return_value,
                    delay=old.delay,
                    retry_policy=old.retry_policy,
                    is_retryable=old.is_retryable,
                    timer_fire_at=timer_fire_at,
                    timer_name=timer_name,
                    updated_at=datetime.now(),
                )
                self._invocations[key] = updated

                # Notify timer processor that a new timer was scheduled
                self._timer_notify.set()
                self._timer_notify.clear()  # Reset for next notification

    async def get_expired_timers(self, now: datetime) -> list[TimerInfo]:
        """Get all timers that have expired."""
        async with self._lock:
            expired = []
            for (flow_id, step), inv in self._invocations.items():
                if (
                    inv.status == InvocationStatus.WAITING_FOR_TIMER
                    and inv.timer_fire_at is not None
                    and inv.timer_fire_at <= now
                ):
                    expired.append(
                        TimerInfo(
                            flow_id=flow_id,
                            step=step,
                            fire_at=inv.timer_fire_at,
                            timer_name=inv.timer_name,
                        )
                    )
            return expired

    async def claim_timer(self, flow_id: str, step: int) -> bool:
        """Claim an expired timer (optimistic concurrency)."""
        async with self._lock:
            key = (flow_id, step)
            if key not in self._invocations:
                return False

            inv = self._invocations[key]
            if inv.status != InvocationStatus.WAITING_FOR_TIMER:
                return False

            # Claim it by changing status to PENDING
            updated = Invocation(
                id=inv.id,
                flow_id=inv.flow_id,
                step=inv.step,
                timestamp=inv.timestamp,
                class_name=inv.class_name,
                method_name=inv.method_name,
                status=InvocationStatus.PENDING,
                attempts=inv.attempts,
                parameters=inv.parameters,
                params_hash=inv.params_hash,
                return_value=inv.return_value,
                delay=inv.delay,
                retry_policy=inv.retry_policy,
                is_retryable=inv.is_retryable,
                timer_fire_at=inv.timer_fire_at,
                timer_name=inv.timer_name,
                updated_at=datetime.now(),
            )
            self._invocations[key] = updated

            # Notify timer processor that timer was claimed (may need to recalculate next wake time)
            self._timer_notify.set()
            self._timer_notify.clear()  # Reset for next notification

            return True

    async def store_suspension_result(
        self, flow_id: str, step: int, suspension_key: str, result: bytes
    ) -> None:
        """Store suspension result data (timer or signal completion)."""
        async with self._lock:
            key = (flow_id, step, suspension_key)
            self._suspension_results[key] = result

    async def get_suspension_result(
        self, flow_id: str, step: int, suspension_key: str
    ) -> bytes | None:
        """Get suspension result data if available."""
        async with self._lock:
            key = (flow_id, step, suspension_key)
            return self._suspension_results.get(key)

    async def remove_suspension_result(self, flow_id: str, step: int, suspension_key: str) -> None:
        """Remove suspension result data after consuming it."""
        async with self._lock:
            key = (flow_id, step, suspension_key)
            self._suspension_results.pop(key, None)

    async def log_signal(self, flow_id: str, step: int, signal_name: str) -> None:
        """
        Mark an invocation as waiting for an external signal.

        CRITICAL: This must be called BEFORE scheduling the child flow to prevent
        race conditions where the child completes before the parent is marked as waiting.
        """
        async with self._lock:
            key = (flow_id, step)
            if key not in self._invocations:
                raise StorageError(f"Invocation not found: flow_id={flow_id}, step={step}")

            inv = self._invocations[key]

            # Update invocation status to WAITING_FOR_SIGNAL
            updated = Invocation(
                id=inv.id,
                flow_id=inv.flow_id,
                step=inv.step,
                timestamp=inv.timestamp,
                class_name=inv.class_name,
                method_name=inv.method_name,
                status=InvocationStatus.WAITING_FOR_SIGNAL,
                attempts=inv.attempts,
                parameters=inv.parameters,
                params_hash=inv.params_hash,
                return_value=inv.return_value,
                delay=inv.delay,
                retry_policy=inv.retry_policy,
                is_retryable=inv.is_retryable,
                timer_fire_at=inv.timer_fire_at,
                timer_name=signal_name,  # Store signal name in timer_name field
                updated_at=datetime.now(),
            )
            self._invocations[key] = updated

    async def resume_flow(self, flow_id: str) -> bool:
        """Resume a suspended flow by changing status SUSPENDED → PENDING.

        This method atomically:
        1. Checks if flow is SUSPENDED
        2. Changes status to PENDING
        3. Re-enqueues to work queue (adds back to scheduled_flows list)

        Returns:
            True if flow was resumed, False if not suspended
        """
        async with self._lock:
            # Find the flow's task by flow_id
            task_to_resume = None
            task_id_to_resume = None
            for tid, task in self._scheduled_flows.items():
                if task.flow_id == flow_id and task.status == TaskStatus.SUSPENDED:
                    task_to_resume = task
                    task_id_to_resume = tid
                    break

            if task_to_resume is None:
                # Flow not found or not suspended
                return False

            # Update status to PENDING
            updated_task = ScheduledFlow(
                task_id=task_to_resume.task_id,
                flow_id=task_to_resume.flow_id,
                flow_type=task_to_resume.flow_type,
                flow_data=task_to_resume.flow_data,
                status=TaskStatus.PENDING,  # Change from SUSPENDED to PENDING
                locked_by=None,  # Clear lock
                scheduled_for=task_to_resume.scheduled_for,
                created_at=task_to_resume.created_at,
                updated_at=datetime.now(),
                retry_count=task_to_resume.retry_count,
                retry_policy=task_to_resume.retry_policy,
                parent_metadata=task_to_resume.parent_metadata,
            )

            # Replace in scheduled_flows dict
            self._scheduled_flows[task_id_to_resume] = updated_task

            # Wake up one waiting worker since we just made a flow available
            self._work_notify.set()
            self._work_notify.clear()  # Reset for next notification

            return True

    async def get_next_timer_fire_time(self) -> datetime | None:
        """Get the earliest timer fire time across all flows.

        Returns:
            datetime of next timer, or None if no timers pending
        """
        async with self._lock:
            next_fire_time = None

            # Iterate all invocations looking for timers
            for inv in self._invocations.values():
                if inv.status == InvocationStatus.WAITING_FOR_TIMER:
                    if inv.timer_fire_at is not None:
                        if next_fire_time is None:
                            next_fire_time = inv.timer_fire_at
                        elif inv.timer_fire_at < next_fire_time:
                            next_fire_time = inv.timer_fire_at

            return next_fire_time

    async def reset(self) -> None:
        """Clear all data (for testing)."""
        async with self._lock:
            self._invocations.clear()
            self._scheduled_flows.clear()
            self._suspension_results.clear()

    async def close(self) -> None:
        """Close storage (no-op for in-memory)."""
        pass

    def work_notify(self) -> asyncio.Event:
        """Return event for work notifications (WorkNotificationSource protocol).

        Workers wait on this event to be notified when work becomes available,
        instead of polling with sleep().

        Returns:
            asyncio.Event that is set when work is enqueued or resumed
        """
        return self._work_notify

    def timer_notify(self) -> asyncio.Event:
        """Return event for timer notifications (TimerNotificationSource protocol).

        Timer processors wait on this event to be notified when timer state changes,
        instead of polling every N seconds.

        Returns:
            asyncio.Event that is set when timers are scheduled or claimed
        """
        return self._timer_notify

    def status_notify(self) -> asyncio.Event:
        """Return event for flow status change notifications.

        Callers can use this to wait for flow status changes (completion, failure, etc.)
        instead of polling. The notification is triggered whenever any flow status changes.

        Returns:
            asyncio.Event that is set when any flow status changes
        """
        return self._status_notify
