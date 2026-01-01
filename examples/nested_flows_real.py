"""
Nested flows example - parent flow spawns and executes child flows inline

**Rust Reference**: ergon_rust/ergon/examples/nested_flows_real.rs

This example demonstrates:
- Parent flow that executes child flows inline (not using invoke())
- Child flows have their own @step methods
- Children execute as part of parent's step execution
- Only parent flow is scheduled/registered with worker

Pattern:
- Parent flow creates child flow instances
- Parent directly calls child.process() within its steps
- Child flows are NOT scheduled separately
- All execution happens synchronously within parent

Run with:
    PYTHONPATH=src python3 examples/nested_flows_real.py
"""

import asyncio
from dataclasses import dataclass
from typing import List

from ergon import flow, flow_type, step, Scheduler, Worker, InMemoryExecutionLog
from ergon.core import TaskStatus


# ============================================================================
# Data Structures
# ============================================================================

@dataclass
class ValidationResult:
    """Result of order validation."""
    order_id: str


@dataclass
class PaymentResult:
    """Result of payment processing."""
    transaction_id: str
    status: str


@dataclass
class InventoryResult:
    """Result of inventory reservation."""
    reservation_id: str
    status: str


@dataclass
class OrderResult:
    """Final order processing result."""
    order_id: str
    status: str
    items_count: int


# ============================================================================
# PARENT FLOW: Actually spawns child flows inline
# ============================================================================

@dataclass
@flow_type
class OrderProcessor:
    """
    Parent flow that processes orders with inline child flow execution.

    **Rust Reference**: OrderProcessor in nested_flows_real.rs lines 16-113
    """
    order_id: str
    items: List[str]
    total_amount: float

    @flow
    async def process_order(self) -> OrderResult:
        """
        Main order processing flow.

        **Rust Reference**: process_order() lines 24-35

        Executes child flows inline (not using invoke()).
        """
        print(f"\n[PARENT] Processing order: {self.order_id}")

        # Step 1: Validate order
        validation = await self.validate_order()

        # Step 2: Process payment (spawns child flow inline)
        payment = await self.process_payment(validation)

        # Step 3: Reserve inventory (spawns child flow inline)
        inventory = await self.reserve_inventory(payment)

        # Step 4: Finalize order
        result = await self.finalize_order(inventory)

        print(f"[PARENT] Order {self.order_id} completed!")
        return result

    @step
    async def validate_order(self) -> ValidationResult:
        """
        Validate order details.

        **Rust Reference**: validate_order() lines 37-49
        """
        print(f"  [Step] Validating order {self.order_id}")
        await asyncio.sleep(0.05)

        if not self.items or self.total_amount <= 0.0:
            raise ValueError("Invalid order")

        return ValidationResult(order_id=self.order_id)

    @step
    async def process_payment(self, validation: ValidationResult) -> PaymentResult:
        """
        Process payment via inline child flow execution.

        **Rust Reference**: process_payment() lines 51-72

        Key pattern: Creates child flow and executes it inline (not scheduled).
        """
        print(f"  [Step] Spawning child payment flow for {validation.order_id}")

        # Create and execute child flow inline
        child = PaymentFlow(
            transaction_id=f"TXN-{self.order_id}",
            amount=self.total_amount
        )

        # Execute child flow directly (it has its own steps)
        result = await child.process()

        print(f"  [Step] Child payment completed: {result}")
        return result

    @step
    async def reserve_inventory(self, payment: PaymentResult) -> InventoryResult:
        """
        Reserve inventory via inline child flow execution.

        **Rust Reference**: reserve_inventory() lines 74-94
        """
        print(f"  [Step] Spawning child inventory flow (payment: {payment.transaction_id})")

        # Create and execute child flow inline
        child = InventoryFlow(
            reservation_id=f"RES-{self.order_id}",
            items=self.items
        )

        result = await child.process()

        print(f"  [Step] Child inventory completed: {result}")
        return result

    @step
    async def finalize_order(self, inventory: InventoryResult) -> OrderResult:
        """
        Finalize the order.

        **Rust Reference**: finalize_order() lines 96-112
        """
        print(f"  [Step] Finalizing order (reservation: {inventory.reservation_id})")
        await asyncio.sleep(0.05)

        return OrderResult(
            order_id=self.order_id,
            status="completed",
            items_count=len(self.items)
        )


# ============================================================================
# CHILD FLOW: Payment processing
# ============================================================================

@dataclass
@flow_type
class PaymentFlow:
    """
    Child flow for payment processing.

    **Rust Reference**: PaymentFlow in nested_flows_real.rs lines 119-154

    Executed inline by parent flow (not scheduled separately).
    """
    transaction_id: str
    amount: float

    async def process(self) -> PaymentResult:
        """
        Payment processing entry point.

        **Rust Reference**: process() lines 126-135
        """
        print(f"    [CHILD-Payment] Starting: {self.transaction_id}")

        auth = await self.authorize()
        result = await self.capture(auth)

        print(f"    [CHILD-Payment] Done: {self.transaction_id}")
        return result

    @step
    async def authorize(self) -> str:
        """
        Authorize payment.

        **Rust Reference**: authorize() lines 137-142
        """
        print(f"      [Step] Authorizing ${self.amount:.2f}")
        await asyncio.sleep(0.03)
        return f"AUTH-{self.transaction_id}"

    @step
    async def capture(self, auth_code: str) -> PaymentResult:
        """
        Capture authorized payment.

        **Rust Reference**: capture() lines 144-153
        """
        print(f"      [Step] Capturing payment (auth: {auth_code})")
        await asyncio.sleep(0.03)

        return PaymentResult(
            transaction_id=self.transaction_id,
            status="captured"
        )


# ============================================================================
# CHILD FLOW: Inventory management
# ============================================================================

@dataclass
@flow_type
class InventoryFlow:
    """
    Child flow for inventory management.

    **Rust Reference**: InventoryFlow in nested_flows_real.rs lines 160-195

    Executed inline by parent flow (not scheduled separately).
    """
    reservation_id: str
    items: List[str]

    async def process(self) -> InventoryResult:
        """
        Inventory processing entry point.

        **Rust Reference**: process() lines 167-176
        """
        print(f"    [CHILD-Inventory] Starting: {self.reservation_id}")

        checked = await self.check_stock()
        result = await self.reserve(checked)

        print(f"    [CHILD-Inventory] Done: {self.reservation_id}")
        return result

    @step
    async def check_stock(self) -> List[str]:
        """
        Check stock availability.

        **Rust Reference**: check_stock() lines 178-183
        """
        print(f"      [Step] Checking stock for {len(self.items)} items")
        await asyncio.sleep(0.03)
        return self.items.copy()

    @step
    async def reserve(self, items: List[str]) -> InventoryResult:
        """
        Reserve inventory items.

        **Rust Reference**: reserve() lines 185-194
        """
        print(f"      [Step] Reserving {len(items)} items")
        await asyncio.sleep(0.03)

        return InventoryResult(
            reservation_id=self.reservation_id,
            status="reserved"
        )


# ============================================================================
# Main
# ============================================================================

async def main():
    """
    Run nested flows example.

    **Rust Reference**: main() lines 229-266
    """
    storage = InMemoryExecutionLog()

    scheduler = Scheduler(storage).with_version("v1.0")

    # Schedule parent flow only - it will spawn children inline
    order = OrderProcessor(
        order_id="ORD-001",
        items=["Widget", "Gadget"],
        total_amount=99.99
    )
    task_id = await scheduler.schedule(order)

    # Worker only needs to handle the parent flow type
    # Child flows are executed inline, not scheduled separately
    # **Rust Reference**: worker.register(|flow: Arc<OrderProcessor>| flow.process_order())
    worker = Worker(storage, "worker-1")
    await worker.register(OrderProcessor, lambda flow: flow.process_order())
    handle = await worker.start()

    # Wait for completion
    while True:
        await asyncio.sleep(0.1)
        scheduled = await storage.get_scheduled_flow(task_id)
        if scheduled and scheduled.status in (TaskStatus.COMPLETE, TaskStatus.FAILED):
            break

    await handle.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
