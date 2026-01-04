"""
Child Flow Retryable Error Test

**Rust Reference**: ergon_rust/ergon/src/executor/child_flow.rs lines 42-62
Demonstrates ChildFlowError preserving retryability from child flows.

## How It Works

When a child flow fails:
1. Child flow raises non-retryable error (is_retryable() = False)
2. Error is serialized into SuspensionPayload with is_retryable=False
3. Parent flow deserializes the payload
4. ChildFlowError is raised with retryability preserved
5. Parent flow respects the non-retryable flag and fails immediately

## Behavior

- Child fails with non-retryable error -> Parent FAILS immediately (no retry)
- Child fails with retryable error -> Parent RETRIES

## Verification

Execution counters verify correct behavior:
- CHILD_EXECUTIONS = 1 (child fails once with non-retryable error)
- PARENT_EXECUTIONS = 2 (initial suspend + resume, no retries)
- retry_count = 0 (parent did not retry)

## Run with
```bash
PYTHONPATH=src python3 examples/child_flow_retryable.py
```
"""

import asyncio
import logging
from dataclasses import dataclass

from pyergon import Scheduler, Worker, flow, flow_type, step
from pyergon.core import RetryableError, RetryPolicy, TaskStatus
from pyergon.storage.sqlite import SqliteExecutionLog

logging.basicConfig(level=logging.CRITICAL)

# Global execution counters - this is our EVIDENCE
CHILD_EXECUTIONS = 0
PARENT_EXECUTIONS = 0


# ============================================================================
# Custom Non-Retryable Error
# ============================================================================


class PaymentError(RetryableError):
    """Base class for payment errors."""

    pass


class InvalidCardError(PaymentError):
    """PERMANENT error - invalid card, no point retrying."""

    def __init__(self, card_number: str):
        self.card_number = card_number
        super().__init__(f"Invalid card number: {card_number}")

    def is_retryable(self) -> bool:
        return False


# ============================================================================
# Child Flow - Raises Non-Retryable Error
# ============================================================================


@dataclass
@flow_type
class PaymentChildFlow:
    """
    Child flow that always fails with non-retryable error.

    Expected: Execute ONCE, fail with is_retryable=False
    """

    card_number: str

    @step
    async def validate_card(self) -> str:
        """Validate card - always fails with non-retryable error."""
        global CHILD_EXECUTIONS
        CHILD_EXECUTIONS += 1

        # ALWAYS fail with NON-RETRYABLE error
        raise InvalidCardError(self.card_number)

    @flow(retry_policy=RetryPolicy.STANDARD)
    async def process(self) -> str:
        """Child flow entry point."""
        result = await self.validate_card()
        return result


# ============================================================================
# Parent Flow - Invokes Child
# ============================================================================


@dataclass
@flow_type
class OrderParentFlow:
    """
    Parent flow that invokes child flow.

    When child fails with is_retryable=False, parent fails immediately
    WITHOUT retry. ChildFlowError preserves retryability information.

    Behavior: Parent executes twice (initial suspend + resume), with zero retries.
    """

    order_id: str
    card_number: str

    @step
    async def process_payment(self) -> str:
        """Invoke child flow to process payment."""
        global PARENT_EXECUTIONS
        PARENT_EXECUTIONS += 1

        # Invoke child flow
        child = PaymentChildFlow(card_number=self.card_number)
        result = await self.invoke(child).result()
        return result

    @flow(retry_policy=RetryPolicy.STANDARD)
    async def run(self) -> str:
        """Parent flow entry point."""
        result = await self.process_payment()
        return result


# ============================================================================
# Main Execution
# ============================================================================


async def main():
    """
    Run test and verify ChildFlowError preserves retryability.
    """

    # Setup storage
    storage = SqliteExecutionLog("data/child_flow_retryable_test.db")
    await storage.connect()
    await storage.reset()

    # Setup scheduler
    scheduler = Scheduler(storage).with_version("v1.0")

    # Schedule parent flow
    order = OrderParentFlow(order_id="ORD-001", card_number="**** **** **** 9999")
    task_id = await scheduler.schedule(order)

    # Start worker
    worker = Worker(storage, "Worker-1", poll_interval=0.1)
    await worker.register(PaymentChildFlow)
    await worker.register(OrderParentFlow)
    worker_handle = await worker.start()

    # Wait for completion
    status_notify = storage.status_notify()
    try:
        await asyncio.wait_for(_wait_for_completion(storage, task_id, status_notify), timeout=10.0)
    except TimeoutError:
        print("\nWARNING: Timeout waiting for completion")

    # Get final status
    scheduled = await storage.get_scheduled_flow(task_id)

    # Verify results
    is_correct = (
        PARENT_EXECUTIONS == 2
        and scheduled
        and scheduled.retry_count == 0
        and scheduled.status == TaskStatus.FAILED
    )

    status_name = scheduled.status.name if scheduled else "UNKNOWN"
    retry_count = scheduled.retry_count if scheduled else "N/A"
    print(f"{task_id}: {status_name} (retries: {retry_count})")
    print(f"  Child executions: {CHILD_EXECUTIONS}")
    print(f"  Parent executions: {PARENT_EXECUTIONS}")
    if scheduled and scheduled.error_message:
        print(f"  Error: {scheduled.error_message}")

    # Assert correctness
    retry_count_val = scheduled.retry_count if scheduled else "N/A"
    assert is_correct, (
        f"Test failed: parent_executions={PARENT_EXECUTIONS} (expected 2), "
        f"retry_count={retry_count_val} (expected 0)"
    )

    await worker_handle.shutdown()
    await storage.close()


async def _wait_for_completion(storage, task_id: str, status_notify: asyncio.Event):
    """Wait for flow to complete using event-driven notifications."""
    while True:
        task = await storage.get_scheduled_flow(task_id)
        if task and task.status in (TaskStatus.COMPLETE, TaskStatus.FAILED):
            break
        await status_notify.wait()
        status_notify.clear()


if __name__ == "__main__":
    asyncio.run(main())
