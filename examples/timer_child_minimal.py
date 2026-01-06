"""Minimal Timer + Child Flow + Error Test.

Tests if timers in child flows correctly propagate errors to parent.

## Pattern Shown: Timer + Child Flow Invocation + Error Handling

This example demonstrates:
- Timer suspension in child flows
- Error propagation from child to parent
- Custom retryable error handling
- Minimal step design (only suspension points)
- Logging after suspension (no duplicate logs on replay)

## Run with:
```bash
PYTHONPATH=src python examples/timer_child_minimal.py
```

## Expected Output:
```
=== Scheduling Test1 (child should succeed) ===
  [CHILD] Timer completed, checking if should fail...
  [CHILD] Task succeeded!
[PARENT] Test1 - Child succeeded: Success
=== Test1 final status: Complete ===

=== Scheduling Test2 (child should fail) ===
  [CHILD] Timer completed, checking if should fail...
  [CHILD] Returning error!
[PARENT] Test2 - Child failed: Task failed: Simulated failure
=== Test2 final status: Failed ===

=== Summary ===
Test1 (should succeed): Complete - PASS
Test2 (should fail): Failed - PASS
```
"""

import asyncio
from dataclasses import dataclass

from pyergon import Scheduler, Worker, flow, flow_type, step
from pyergon.core import RetryableError, TaskStatus
from pyergon.executor.timer import schedule_timer_named
from pyergon.storage.sqlite import SqliteExecutionLog

# =============================================================================
# Custom Error
# =============================================================================


class TaskError(Exception):
    """Custom error for task failures."""

    def __init__(self, message: str, is_retryable: bool = False):
        super().__init__(message)
        self.message = message
        self._is_retryable = is_retryable

    def __str__(self) -> str:
        return self.message

    def is_retryable(self) -> bool:
        """Check if error is retryable."""
        return self._is_retryable


class TaskTimeoutError(RetryableError, TaskError):
    """Retryable timeout error."""

    def __init__(self):
        TaskError.__init__(self, "Task timed out", is_retryable=True)
        RetryableError.__init__(self)


class TaskFailedError(TaskError):
    """Non-retryable task failure."""

    def __init__(self, message: str):
        super().__init__(f"Task failed: {message}", is_retryable=False)


# =============================================================================
# Child Flow with Timer
# =============================================================================


@dataclass
@flow_type(invokable=str)
class ChildTask:
    """Child flow that waits on a timer then either succeeds or fails."""

    should_fail: bool

    @step
    async def wait_step(self) -> None:
        """Minimal step: only the suspension point."""
        await schedule_timer_named(2.0, "child-wait")

    @flow
    async def execute(self) -> str:
        """Execute child task with timer.

        Waits for timer, then checks if should fail.
        All logging happens AFTER the timer completes.
        """
        # Step only handles suspension - NO logging before this
        await self.wait_step()

        # All logging and business logic AFTER the step
        print("  [CHILD] Timer completed, checking if should fail...")

        if self.should_fail:
            print("  [CHILD] Returning error!")
            raise TaskFailedError("Simulated failure")

        print("  [CHILD] Task succeeded!")
        return "Success"


# =============================================================================
# Parent Flow
# =============================================================================


@dataclass
@flow_type
class ParentTask:
    """Parent flow that invokes child and handles result."""

    test_name: str
    child_should_fail: bool

    @flow
    async def run(self) -> str:
        """Run parent task.

        Invokes child and waits for result.
        All logging happens AFTER the await.
        """
        # Invoke child and wait for result
        try:
            child_result = await self.invoke(ChildTask(should_fail=self.child_should_fail)).result()

            # All logging AFTER the await
            print(f"[PARENT] {self.test_name} - Child succeeded: {child_result}")
            return f"Parent: child succeeded with {child_result}"

        except Exception as e:
            # Handle child failure
            print(f"[PARENT] {self.test_name} - Child failed: {e}")
            raise TaskFailedError(str(e))


# =============================================================================
# Helper Functions
# =============================================================================


async def wait_for_completion(
    storage: SqliteExecutionLog, task_id: str, timeout_seconds: float = 10.0
) -> TaskStatus | None:
    """Wait for a task to complete or fail.

    Args:
        storage: Storage backend
        task_id: Task ID to monitor
        timeout_seconds: Max time to wait

    Returns:
        Final TaskStatus or None if timeout
    """
    start = asyncio.get_event_loop().time()

    while True:
        elapsed = asyncio.get_event_loop().time() - start
        if elapsed > timeout_seconds:
            return None

        scheduled = await storage.get_scheduled_flow(task_id)

        if scheduled and scheduled.status in (TaskStatus.COMPLETE, TaskStatus.FAILED):
            return scheduled.status

        await asyncio.sleep(0.1)


# =============================================================================
# Main
# =============================================================================


async def main():
    """Run the timer + child flow + error test."""
    import sys

    print("[MAIN] Starting timer_child_minimal test...", file=sys.stderr, flush=True)
    storage = SqliteExecutionLog("data/timer_child_minimal.db")
    print("[MAIN] Created storage")
    await storage.connect()
    print("[MAIN] Connected to storage")
    await storage.reset()
    print("[MAIN] Reset storage")

    # Create worker with timers enabled
    worker = Worker(storage, "test-worker").with_timers().with_poll_interval(0.05)
    print("[MAIN] Created worker")
    await worker.register(ParentTask)
    print("[MAIN] Registered ParentTask")
    await worker.register(ChildTask)
    print("[MAIN] Registered ChildTask")

    worker_handle = await worker.start()
    print("[MAIN] Worker started")
    await asyncio.sleep(0.1)  # Let worker start

    scheduler = Scheduler(storage).with_version("v1.0")
    print("[MAIN] Created scheduler")

    # Test 1: Child should succeed
    print("\n=== Scheduling Test1 (child should succeed) ===")
    task1 = ParentTask(test_name="Test1", child_should_fail=False)
    task_id_1 = await scheduler.schedule(task1)

    status1 = await wait_for_completion(storage, task_id_1, timeout_seconds=10.0)
    print(f"=== Test1 final status: {status1} ===\n")

    # Test 2: Child should fail
    print("=== Scheduling Test2 (child should fail) ===")
    task2 = ParentTask(test_name="Test2", child_should_fail=True)
    task_id_2 = await scheduler.schedule(task2)

    status2 = await wait_for_completion(storage, task_id_2, timeout_seconds=10.0)
    print(f"=== Test2 final status: {status2} ===\n")

    # Verify results
    print("=== Summary ===")
    print(
        f"Test1 (should succeed): {status1} - "
        f"{'PASS' if status1 == TaskStatus.COMPLETE else 'FAIL'}"
    )
    print(f"Test2 (should fail): {status2} - {'PASS' if status2 == TaskStatus.FAILED else 'FAIL'}")

    await worker_handle.shutdown()
    await storage.close()


if __name__ == "__main__":
    asyncio.run(main())
