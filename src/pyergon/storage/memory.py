"""In-memory storage implementation for pyergon.

Design Pattern: Adapter Pattern
InMemoryExecutionLog adapts in-memory dictionaries to ExecutionLog interface.

Instance is immediately usable after __init__.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta
from typing import Any

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

        # Index: {flow_id: task_id} for fast O(1) lookup during resume
        self._flow_task_map: dict[str, str] = {}

        # Queue: task_id (FIFO queue for pending tasks, matches Rust mpsc channel)
        self._pending_queue: asyncio.Queue[str] = asyncio.Queue()

        # Storage: {(flow_id, step, suspension_key): bytes}
        # For storing suspension results (timer/signal completions)
        self._suspension_results: dict[tuple[str, int, str], bytes] = {}

        # Lock for thread-safety
        self._lock = asyncio.Lock()

        # Notification events (implements WorkNotificationSource and TimerNotificationSource)
        self._work_notify = asyncio.Event()
        self._timer_notify = asyncio.Event()

        # Status notification uses Condition for race-free wait_for(predicate) pattern
        self._status_notify = asyncio.Condition(self._lock)

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
        retry_policy: RetryPolicy | None = None,
    ) -> Invocation:
        """Record the start of a step execution."""
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
                    retry_policy=old.retry_policy,
                    is_retryable=is_retryable,
                    timer_fire_at=old.timer_fire_at,
                    timer_name=old.timer_name,
                    updated_at=datetime.now(),
                )
                self._invocations[key] = updated

    async def enqueue_flow(self, flow: ScheduledFlow) -> str:
        """Add a flow to the distributed work queue.

        Accepts a complete ScheduledFlow object.
        """
        async with self._lock:
            self._scheduled_flows[flow.task_id] = flow
            self._flow_task_map[flow.flow_id] = flow.task_id

            # Add to FIFO queue (matches Rust channel)
            self._pending_queue.put_nowait(flow.task_id)

            # Notify workers
            # NOTE: We do NOT clear the event here. The worker clears it upon waking.
            # If we clear it here, a race condition exists where the signal is lost.
            self._work_notify.set()

            return flow.task_id

    async def dequeue_flow(self, worker_id: str) -> ScheduledFlow | None:
        """Claim and retrieve a pending flow from queue.

        Uses FIFO queue for O(1) access and fairness.
        Implements daisy-chain notification: if queue is not empty after dequeue,
        wake up other workers.
        """
        async with self._lock:
            try:
                # Try to get task_id from queue
                # We consume items until we find a valid one or queue is empty
                # In a real scenario with proper dequeuing, we shouldn't have many invalid items
                task_id = self._pending_queue.get_nowait()
            except asyncio.QueueEmpty:
                return None

            # Daisy-chain: If there are more items, ensure other workers wake up
            if not self._pending_queue.empty():
                self._work_notify.set()

            if task_id not in self._scheduled_flows:
                # Task might have been completed/deleted - skip and recurse
                # (or return None to let worker loop)
                # For simplicity, we return None and let worker poll again
                return None

            flow = self._scheduled_flows[task_id]
            now = datetime.now()

            # Check if flow is valid for execution
            if flow.status == TaskStatus.PENDING:
                # Check schedule
                if flow.scheduled_for is not None and flow.scheduled_for > now:
                    # Delayed - put back in queue for later
                    # Note: Simple FIFO queue puts it at the back.
                    # For strict ordering, a PriorityQueue is better, but this
                    # matches Rust mpsc behavior.
                    self._pending_queue.put_nowait(task_id)
                    return None

                # Claim it
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
        """Update flow task status."""
        async with self._lock:
            if task_id not in self._scheduled_flows:
                raise StorageError(f"Flow not found: task_id={task_id}")

            flow = self._scheduled_flows[task_id]
            now = datetime.now()

            completed_at = now if status.is_terminal else flow.completed_at

            updated = ScheduledFlow(
                task_id=flow.task_id,
                flow_id=flow.flow_id,
                flow_type=flow.flow_type,
                flow_data=flow.flow_data,
                status=status,
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

            # Notify status listeners (must be called while holding the lock)
            self._status_notify.notify_all()

    async def retry_flow(self, task_id: str, error_message: str, delay: timedelta) -> None:
        """Reschedule a failed flow for retry after a delay."""
        async with self._lock:
            if task_id not in self._scheduled_flows:
                raise StorageError(f"Flow not found: task_id={task_id}")

            flow = self._scheduled_flows[task_id]
            now = datetime.now()
            scheduled_for = now + delay

            updated = ScheduledFlow(
                task_id=flow.task_id,
                flow_id=flow.flow_id,
                flow_type=flow.flow_type,
                flow_data=flow.flow_data,
                status=TaskStatus.PENDING,
                locked_by=None,
                retry_count=flow.retry_count + 1,
                created_at=flow.created_at,
                updated_at=now,
                error_message=error_message,
                claimed_at=None,
                completed_at=None,
                scheduled_for=scheduled_for,
                parent_metadata=flow.parent_metadata,
                retry_policy=flow.retry_policy,
            )

            self._scheduled_flows[task_id] = updated

            # Re-enqueue
            self._pending_queue.put_nowait(task_id)
            self._work_notify.set()

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
                    retry_policy=old.retry_policy,
                    is_retryable=old.is_retryable,
                    timer_fire_at=timer_fire_at,
                    timer_name=timer_name,
                    updated_at=datetime.now(),
                )
                self._invocations[key] = updated
                self._timer_notify.set()
                self._timer_notify.clear()

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
            # Sort by fire_at to handle oldest timers first
            expired.sort(key=lambda t: t.fire_at)
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
                retry_policy=inv.retry_policy,
                is_retryable=inv.is_retryable,
                timer_fire_at=inv.timer_fire_at,
                timer_name=inv.timer_name,
                updated_at=datetime.now(),
            )
            self._invocations[key] = updated
            self._timer_notify.set()
            self._timer_notify.clear()

            return True

    async def store_suspension_result(
        self, flow_id: str, step: int, suspension_key: str, result: bytes
    ) -> None:
        """Store suspension result data."""
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
        """Mark an invocation as waiting for an external signal."""
        async with self._lock:
            key = (flow_id, step)
            if key not in self._invocations:
                raise StorageError(f"Invocation not found: flow_id={flow_id}, step={step}")

            inv = self._invocations[key]

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
                retry_policy=inv.retry_policy,
                is_retryable=inv.is_retryable,
                timer_fire_at=inv.timer_fire_at,
                timer_name=signal_name,
                updated_at=datetime.now(),
            )
            self._invocations[key] = updated

    async def get_waiting_signals(self) -> list[Any]:
        """Get all flows waiting for signals.

        Returns:
            List of SignalInfo-like objects (mapped from invocations)
        """
        async with self._lock:
            # We construct ad-hoc objects or dictionaries to match SignalInfo
            # Since SignalInfo is in models, let's use a simple object or import it if needed.
            # Assuming callers use .signal_name etc.
            from pyergon.models import SignalInfo

            signals = []
            for (flow_id, step), inv in self._invocations.items():
                if inv.status == InvocationStatus.WAITING_FOR_SIGNAL:
                    signals.append(
                        SignalInfo(flow_id=flow_id, step=step, signal_name=inv.timer_name)
                    )
            return signals

    async def resume_flow(self, flow_id: str) -> bool:
        """Resume a suspended flow by changing status SUSPENDED → PENDING.

        1. O(1) lookup via flow_task_map
        2. Check if SUSPENDED
        3. Update to PENDING
        4. Re-enqueue to pending_queue (Critical for waking worker!)
        5. Notify
        """
        async with self._lock:
            # O(1) lookup
            task_id = self._flow_task_map.get(flow_id)
            if not task_id or task_id not in self._scheduled_flows:
                return False

            task_to_resume = self._scheduled_flows[task_id]

            # Only resume if actually suspended (matches Rust)
            if task_to_resume.status != TaskStatus.SUSPENDED:
                return False

            updated_task = ScheduledFlow(
                task_id=task_to_resume.task_id,
                flow_id=task_to_resume.flow_id,
                flow_type=task_to_resume.flow_type,
                flow_data=task_to_resume.flow_data,
                status=TaskStatus.PENDING,
                locked_by=None,
                scheduled_for=task_to_resume.scheduled_for,
                created_at=task_to_resume.created_at,
                updated_at=datetime.now(),
                retry_count=task_to_resume.retry_count,
                retry_policy=task_to_resume.retry_policy,
                parent_metadata=task_to_resume.parent_metadata,
            )

            self._scheduled_flows[task_id] = updated_task

            # Re-enqueue task to queue (Matches Rust pending_tx.send)
            self._pending_queue.put_nowait(task_id)

            # Notify workers
            self._work_notify.set()

            return True

    async def get_next_timer_fire_time(self) -> datetime | None:
        """Get the earliest timer fire time."""
        async with self._lock:
            next_fire_time = None
            for inv in self._invocations.values():
                if inv.status == InvocationStatus.WAITING_FOR_TIMER:
                    if inv.timer_fire_at is not None:
                        if next_fire_time is None or inv.timer_fire_at < next_fire_time:
                            next_fire_time = inv.timer_fire_at
            return next_fire_time

    async def cleanup_completed(self, older_than: timedelta) -> int:
        """Clean up old completed flows and invocations (Matches Rust)."""
        async with self._lock:
            cutoff = datetime.now() - older_than
            count = 0

            # Cleanup invocations
            keys_to_remove = []
            for key, inv in self._invocations.items():
                if inv.status == InvocationStatus.COMPLETE and inv.timestamp < cutoff:
                    keys_to_remove.append(key)

            for key in keys_to_remove:
                del self._invocations[key]
                count += 1

            # Cleanup suspension params
            self._suspension_results.clear()

            # Cleanup flows
            tasks_to_remove = []
            for task_id, flow in self._scheduled_flows.items():
                if (
                    flow.status == TaskStatus.COMPLETE or flow.status == TaskStatus.FAILED
                ) and flow.updated_at < cutoff:
                    tasks_to_remove.append(task_id)

            for task_id in tasks_to_remove:
                flow = self._scheduled_flows[task_id]
                del self._scheduled_flows[task_id]
                self._flow_task_map.pop(flow.flow_id, None)

            return count

    async def reset(self) -> None:
        """Clear all data."""
        async with self._lock:
            self._invocations.clear()
            self._scheduled_flows.clear()
            self._flow_task_map.clear()
            self._suspension_results.clear()
            # Clear queue
            while not self._pending_queue.empty():
                try:
                    self._pending_queue.get_nowait()
                except asyncio.QueueEmpty:
                    break

    async def close(self) -> None:
        pass

    async def wait_for_completion(self, task_id: str) -> TaskStatus:
        """Wait for a single task to complete (race-free).

        Uses asyncio.Condition with manual check-wait loop pattern.
        The condition's lock ensures no race between status check and wait.
        """
        async with self._status_notify:
            while True:
                # Check current status (inside lock)
                flow = self._scheduled_flows.get(task_id)
                if flow is None:
                    raise StorageError(f"Task not found: task_id={task_id}")

                if flow.status in (TaskStatus.COMPLETE, TaskStatus.FAILED):
                    return flow.status

                # Wait for next status change (releases lock while waiting)
                await self._status_notify.wait()

    async def wait_for_all(self, task_ids: list[str]) -> list[tuple[str, TaskStatus]]:
        """Wait for all tasks to complete (race-free).

        Uses asyncio.Condition with manual check-wait loop pattern.
        The condition's lock ensures no race between status check and wait.
        """
        async with self._status_notify:
            while True:
                # Check which tasks have completed (inside lock)
                all_done = True
                for task_id in task_ids:
                    flow = self._scheduled_flows.get(task_id)
                    if flow is None:
                        raise StorageError(f"Task not found: task_id={task_id}")

                    if flow.status not in (TaskStatus.COMPLETE, TaskStatus.FAILED):
                        all_done = False
                        break

                if all_done:
                    # Collect final results
                    results = []
                    for task_id in task_ids:
                        flow = self._scheduled_flows.get(task_id)
                        if flow is None:
                            raise StorageError(f"Task not found: task_id={task_id}")
                        results.append((task_id, flow.status))
                    return results

                # Wait for next status change (releases lock while waiting)
                await self._status_notify.wait()

    def work_notify(self) -> asyncio.Event:
        return self._work_notify

    def timer_notify(self) -> asyncio.Event:
        return self._timer_notify

    def status_notify(self) -> asyncio.Condition:
        """Return condition for flow status change notifications.

        Callers can use this to wait for flow status changes. The condition
        uses the internal lock, so callers must acquire it before waiting:

        ```python
        async with storage.status_notify():
            await storage.status_notify().wait()
        ```

        Recommended: Use wait_for_completion() or wait_for_all() instead,
        which handle the condition properly and avoid race conditions.
        """
        return self._status_notify
