"""
Retryable error handling with in-memory storage.

Demonstrates differential retry behavior:
- Retryable errors (network timeout) trigger automatic retry
- Non-retryable errors (not found) fail immediately

Run:
    PYTHONPATH=src python examples/retryable_error_proof.py
"""

import asyncio
from dataclasses import dataclass

from pyergon import Scheduler, Worker, flow, flow_type, step
from pyergon.core import RetryableError, RetryPolicy, TaskStatus
from pyergon.storage.memory import InMemoryExecutionLog

STEP_A_EXECUTIONS = 0
STEP_B_EXECUTIONS = 0


class InventoryError(RetryableError):
    """Base class for inventory errors."""

    pass


class ApiTimeoutError(InventoryError):
    """Transient network timeout error."""

    def __str__(self):
        return "API timeout - transient network error"

    def is_retryable(self) -> bool:
        return True


class ItemNotFoundError(InventoryError):
    """Permanent item not found error."""

    def __init__(self, item: str):
        self.item = item

    def __str__(self):
        return f"Item '{self.item}' not found in catalog"

    def is_retryable(self) -> bool:
        return False


@dataclass
@flow_type
class OrderA:
    """Order flow with retryable error."""

    order_id: str

    @step
    async def check_inventory(self) -> str:
        """Check inventory with transient failures."""
        global STEP_A_EXECUTIONS
        STEP_A_EXECUTIONS += 1

        if STEP_A_EXECUTIONS < 3:
            raise ApiTimeoutError()

        return f"Inventory reserved for {self.order_id}"

    @flow(retry_policy=RetryPolicy.STANDARD)
    async def process_order(self) -> str:
        """Process order with automatic retry on transient errors."""
        return await self.check_inventory()


@dataclass
@flow_type
class OrderB:
    """Order flow with non-retryable error."""

    order_id: str
    item_sku: str

    @step
    async def check_inventory(self) -> str:
        """Check inventory with permanent failure."""
        global STEP_B_EXECUTIONS
        STEP_B_EXECUTIONS += 1
        raise ItemNotFoundError(self.item_sku)

    @flow(retry_policy=RetryPolicy.STANDARD)
    async def process_order(self) -> str:
        """Process order - fails immediately on permanent errors."""
        return await self.check_inventory()


async def run_scenario_a(storage):
    """Run order with retryable error."""
    scheduler = Scheduler(storage).with_version("v1.0")
    order = OrderA(order_id="ORD-A-001")
    task_id = await scheduler.schedule(order)

    worker = Worker(storage, "Worker-A")
    await worker.register(OrderA, lambda flow: flow.process_order())
    handle = await worker.start()

    await asyncio.sleep(5.0)
    await handle.shutdown()

    return await storage.get_scheduled_flow(task_id)


async def run_scenario_b(storage):
    """Run order with non-retryable error."""
    scheduler = Scheduler(storage).with_version("v1.0")
    order = OrderB(order_id="ORD-B-002", item_sku="INVALID-SKU-999")
    task_id = await scheduler.schedule(order)

    worker = Worker(storage, "Worker-B")
    await worker.register(OrderB, lambda flow: flow.process_order())
    handle = await worker.start()

    await asyncio.sleep(5.0)
    await handle.shutdown()

    return await storage.get_scheduled_flow(task_id)


async def main():
    """Demonstrate retry behavior for retryable vs non-retryable errors."""
    storage = InMemoryExecutionLog()

    result_a = await run_scenario_a(storage)
    result_b = await run_scenario_b(storage)

    print(f"Scenario A (retryable): executions={STEP_A_EXECUTIONS}, status={result_a.status.name}")
    print(
        f"Scenario B (non-retryable): executions={STEP_B_EXECUTIONS}, status={result_b.status.name}"
    )

    assert STEP_A_EXECUTIONS == 3
    assert STEP_B_EXECUTIONS == 1
    assert result_a.status == TaskStatus.COMPLETE
    assert result_b.status == TaskStatus.FAILED


if __name__ == "__main__":
    asyncio.run(main())
