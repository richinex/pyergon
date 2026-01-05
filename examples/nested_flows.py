"""
Nested flows with inline child execution.

Parent flow executes child flows directly within its steps,
not via invoke(). Only parent flow is scheduled with worker.

Run:
    PYTHONPATH=src python examples/nested_flows.py
"""

import asyncio
from dataclasses import dataclass

from pyergon import InMemoryExecutionLog, Scheduler, Worker, flow, flow_type, step
from pyergon.core import TaskStatus


@dataclass
class ValidationResult:
    """Order validation result."""

    order_id: str


@dataclass
class PaymentResult:
    """Payment processing result."""

    transaction_id: str
    status: str


@dataclass
class InventoryResult:
    """Inventory reservation result."""

    reservation_id: str
    status: str


@dataclass
class OrderResult:
    """Final order result."""

    order_id: str
    status: str
    items_count: int


@dataclass
@flow_type
class OrderProcessor:
    """Parent flow that executes child flows inline."""

    order_id: str
    items: list[str]
    total_amount: float

    @flow
    async def process_order(self) -> OrderResult:
        """Process order with inline child flow execution."""
        validation = await self.validate_order()
        payment = await self.process_payment(validation)
        inventory = await self.reserve_inventory(payment)
        return await self.finalize_order(inventory)

    @step
    async def validate_order(self) -> ValidationResult:
        """Validate order details."""
        await asyncio.sleep(0.05)

        if not self.items or self.total_amount <= 0.0:
            raise ValueError("Invalid order")

        return ValidationResult(order_id=self.order_id)

    @step
    async def process_payment(self, validation: ValidationResult) -> PaymentResult:
        """Process payment via inline child flow."""
        child = PaymentFlow(transaction_id=f"TXN-{self.order_id}", amount=self.total_amount)
        return await child.process()

    @step
    async def reserve_inventory(self, payment: PaymentResult) -> InventoryResult:
        """Reserve inventory via inline child flow."""
        child = InventoryFlow(reservation_id=f"RES-{self.order_id}", items=self.items)
        return await child.process()

    @step
    async def finalize_order(self, inventory: InventoryResult) -> OrderResult:
        """Finalize the order."""
        await asyncio.sleep(0.05)
        return OrderResult(order_id=self.order_id, status="completed", items_count=len(self.items))


@dataclass
@flow_type
class PaymentFlow:
    """Child flow for payment processing."""

    transaction_id: str
    amount: float

    async def process(self) -> PaymentResult:
        """Process payment with authorization and capture."""
        auth = await self.authorize()
        return await self.capture(auth)

    @step
    async def authorize(self) -> str:
        """Authorize payment."""
        await asyncio.sleep(0.03)
        return f"AUTH-{self.transaction_id}"

    @step
    async def capture(self, auth_code: str) -> PaymentResult:
        """Capture authorized payment."""
        await asyncio.sleep(0.03)
        return PaymentResult(transaction_id=self.transaction_id, status="captured")


@dataclass
@flow_type
class InventoryFlow:
    """Child flow for inventory management."""

    reservation_id: str
    items: list[str]

    async def process(self) -> InventoryResult:
        """Reserve inventory items."""
        checked = await self.check_stock()
        return await self.reserve(checked)

    @step
    async def check_stock(self) -> list[str]:
        """Check stock availability."""
        await asyncio.sleep(0.03)
        return self.items.copy()

    @step
    async def reserve(self, items: list[str]) -> InventoryResult:
        """Reserve inventory items."""
        await asyncio.sleep(0.03)
        return InventoryResult(reservation_id=self.reservation_id, status="reserved")


async def main():
    """Run nested flows example."""
    storage = InMemoryExecutionLog()
    scheduler = Scheduler(storage).with_version("v1.0")

    order = OrderProcessor(order_id="ORD-001", items=["Widget", "Gadget"], total_amount=99.99)
    task_id = await scheduler.schedule(order)

    worker = Worker(storage, "worker-1")
    await worker.register(OrderProcessor, lambda flow: flow.process_order())
    handle = await worker.start()

    while True:
        await asyncio.sleep(0.1)
        scheduled = await storage.get_scheduled_flow(task_id)
        if scheduled and scheduled.status in (TaskStatus.COMPLETE, TaskStatus.FAILED):
            break

    await handle.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
