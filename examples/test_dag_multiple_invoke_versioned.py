"""
Test: Sequential execution with multiple .invoke() calls (versioned)

Note: With atomic steps, .invoke() must be at flow level.
This means we can't use dag! macro when child results are needed in steps.

This example demonstrates:
1. Parent flow invoking multiple child flows sequentially
2. Child flows implementing InvokableFlow protocol for type safety
3. Versioned flow scheduling
4. Multi-worker coordination with type registration

Run with:
```bash
PYTHONPATH=src python examples/test_dag_multiple_invoke_versioned.py
```
"""

import asyncio
import uuid
import os
from dataclasses import dataclass
from datetime import datetime
from ergon import flow, flow_type, step, Scheduler, Worker
from ergon.storage.sqlite import SqliteExecutionLog
from ergon.core.invokable_flow import InvokableFlow
from ergon.core.status import TaskStatus


def ts() -> str:
    """Get timestamp string."""
    return datetime.now().strftime("%H:%M:%S.%f")[:-3]


# =============================================================================
# Child Flow 1 - Payment
# =============================================================================

@dataclass
class PaymentResult:
    """Payment processing result."""
    transaction_id: str


@dataclass
@flow_type(invokable=PaymentResult)
class PaymentFlow:
    """Child flow for processing payments."""
    order_id: str
    amount: float

    @flow
    async def process(self) -> PaymentResult:
        """Process payment (flow entry point)."""
        print(f"[{ts()}]   CHILD: Processing payment ${self.amount:.2f}")
        await asyncio.sleep(0.1)  # 100ms

        return PaymentResult(
            transaction_id=f"TXN-{str(uuid.uuid4())[:8]}"
        )


# =============================================================================
# Child Flow 2 - Shipment
# =============================================================================

@dataclass
class ShipmentResult:
    """Shipment creation result."""
    tracking: str


@dataclass
@flow_type(invokable=ShipmentResult)
class ShipmentFlow:
    """Child flow for creating shipments."""
    order_id: str

    @flow
    async def create(self) -> ShipmentResult:
        """Create shipment (flow entry point)."""
        print(f"[{ts()}]   CHILD: Creating shipment")
        await asyncio.sleep(0.1)  # 100ms

        return ShipmentResult(
            tracking=f"TRK-{str(uuid.uuid4())[:8]}"
        )


# =============================================================================
# Parent Flow - Order
# =============================================================================

@dataclass
@flow_type
class Order:
    """Parent flow that orchestrates payment and shipment."""
    id: str

    @step
    async def validate(self) -> None:
        """Validate order."""
        print(f"[{ts()}] Step: validate {self.id}")

    @step
    async def finalize_payment(self, result: PaymentResult) -> PaymentResult:
        """Process payment result."""
        print(f"[{ts()}] Step: processing payment result: {result}")
        return result

    @step
    async def finalize_shipment(self, result: ShipmentResult) -> ShipmentResult:
        """Process shipment result."""
        print(f"[{ts()}] Step: processing shipment result: {result}")
        return result

    @flow
    async def process(self) -> ShipmentResult:
        """
        Main flow orchestration (entry point).

        Note: Child invocations must be at flow level, not in steps,
        because steps are atomic and cannot span child flow execution.
        """
        # Validate first
        await self.validate()

        # Invoke children at flow level
        print(f"[{ts()}] Flow: invoking Payment child")
        payment_result = await self.invoke(
            PaymentFlow(
                order_id=self.id,
                amount=99.99
            )
        ).result()

        print(f"[{ts()}] Flow: invoking Shipment child")
        shipment_result = await self.invoke(
            ShipmentFlow(
                order_id=self.id
            )
        ).result()

        # Process results in atomic steps
        await self.finalize_payment(payment_result)
        result = await self.finalize_shipment(shipment_result)

        return result


# =============================================================================
# Main
# =============================================================================

async def main():
    """Run the multi-invoke versioned example."""
    print("=" * 70)
    print("MULTI-INVOKE VERSIONED FLOW EXAMPLE")
    print("=" * 70)
    print()

    db = "data/test_dag_multiple_invoke_versioned.db"

    # Ensure data directory exists
    os.makedirs("data", exist_ok=True)

    # Remove old database
    try:
        os.remove(db)
    except FileNotFoundError:
        pass

    # Create storage and scheduler
    storage = SqliteExecutionLog(db)
    await storage.connect()

    scheduler = Scheduler(storage).with_version("v1.0")

    # Schedule parent flow
    order = Order(id="ORD-001")  # Uses dataclass constructor
    parent_task_id = await scheduler.schedule(order)

    print(f"Scheduled parent flow: {parent_task_id}")
    print()

    # Create worker and register handlers
    # Poll interval in seconds: 0.05 = 50ms (matching Rust Duration::from_millis(50))
    worker = Worker(
        storage=storage,
        worker_id="worker"
    ).with_poll_interval(0.05)

    await worker.register(Order)
    await worker.register(PaymentFlow)
    await worker.register(ShipmentFlow)

    # Start worker
    handle = await worker.start()

    # Wait for parent task to complete (matching Rust pattern)
    print("Worker started, waiting for completion...")
    print()
    timeout = 10.0  # 10 second timeout
    poll_interval = 0.1  # Check every 100ms
    elapsed = 0.0

    while elapsed < timeout:
        scheduled = await storage.get_scheduled_flow(parent_task_id)
        if scheduled and scheduled.status.is_terminal:
            print(f"[PASS] Task completed with status: {scheduled.status}")
            print()
            break
        await asyncio.sleep(poll_interval)
        elapsed += poll_interval
    else:
        print("[WARN] Timeout waiting for task completion")
        print()

    # Shutdown worker
    await handle.shutdown()

    # Check parent flow version
    parent_flow = await storage.get_scheduled_flow(parent_task_id)
    if parent_flow:
        version = parent_flow.version if parent_flow.version else "unversioned"
        print()
        print(f"Parent flow version: {version}")

    await storage.close()

    print()
    print("[PASS] Multi-invoke versioned example complete!")


if __name__ == "__main__":
    asyncio.run(main())
