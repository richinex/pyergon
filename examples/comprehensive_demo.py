"""
Comprehensive Ergon Demo: Complete Order Processing System

**Rust Reference**: ergon_rust/ergon/examples/comprehensive_demo.rs

This example demonstrates ALL major Ergon features in a single, realistic workflow:

# Features Demonstrated

1. **Normal Flows**: Main order processing flow with completion notification
2. **Child Flows (invoke API)**: Payment and shipping as separate invokable flows
3. **Timers**: Fraud check delay, shipping preparation delay
4. **DAG Execution**: Parallel validation steps (inventory, customer, fraud)
5. **Retry Policies**: Automatic retry for transient payment failures
6. **Step Caching**: Successful steps aren't re-executed on retry
7. **Error Handling**: Retryable (network) vs non-retryable (business logic) errors
8. **Distributed Workers**: Multiple workers processing flows concurrently
9. **Type Safety**: Flow coordination with InvokableFlow protocol

# Architecture

```text
OrderFlow (main)
  ├─ validate_customer ────┐
  ├─ check_inventory   ────┤─── (parallel DAG)
  └─ check_fraud (timer) ──┘
        │
        ├─ PaymentFlow (child, retryable) ── signal parent
        │
        └─ ShippingFlow (child, timer) ──── signal parent
```

Run:
    PYTHONPATH=src python3 examples/comprehensive_demo.py
"""

import asyncio
import hashlib
import uuid
from dataclasses import dataclass

from pyergon import Scheduler, Worker, flow, flow_type, step
from pyergon.core import RetryPolicy, TaskStatus
from pyergon.executor.timer import schedule_timer_named
from pyergon.storage.sqlite import SqliteExecutionLog

# ============================================================================
# CHILD FLOWS: Invokable flows that suspend parent until completion
# ============================================================================


@dataclass
@flow_type(invokable=str)
class PaymentFlow:
    """
    Payment processing child flow with retry policy.

    **Rust Reference**: PaymentFlow in comprehensive_demo.rs lines 47-84

    Features:
    - Retryable on network errors
    - Invokable by parent flow
    - Returns payment ID (str)
    """

    order_id: str
    amount: float

    @flow(retry_policy=RetryPolicy.STANDARD)
    async def process(self) -> str:
        """
        Process payment with simulated retryable failures.

        **Rust Reference**: process() lines 55-70
        """
        print(f"  [Payment] Processing ${self.amount:.2f} for order {self.order_id}")

        # Deterministic failure simulation based on order_id hash
        hash_value = int(hashlib.md5(self.order_id.encode()).hexdigest(), 16)
        should_fail = (hash_value % 10) < 3

        await self.charge_card(should_fail)

        return f"PAYMENT-{uuid.uuid4().hex[:8]}"

    @step
    async def charge_card(self, should_fail: bool) -> None:
        """
        Charge customer's card.

        **Rust Reference**: charge_card() lines 72-83

        Simulates network timeout on first attempt (retryable).
        """
        await asyncio.sleep(0.1)

        if should_fail:
            print("  [Payment] Network error (will retry)")
            raise Exception("NetworkTimeout")

        print("  [Payment] Card charged successfully")


@dataclass
@flow_type(invokable=str)
class ShippingFlow:
    """
    Shipping arrangement child flow with timer.

    **Rust Reference**: ShippingFlow in comprehensive_demo.rs lines 86-114

    Features:
    - Timer-based warehouse delay
    - Invokable by parent flow
    - Returns tracking ID (str)
    """

    order_id: str
    address: str

    @flow
    async def process(self) -> str:
        """
        Arrange shipping with warehouse preparation delay.

        **Rust Reference**: process() lines 94-105
        """
        print(f"  [Shipping] Preparing shipment to {self.address} for order {self.order_id}")

        # Timer: Wait for warehouse to prepare package
        await self.prepare_package()

        return f"TRACK-{uuid.uuid4().hex[:8]}"

    @step
    async def prepare_package(self) -> None:
        """
        Wait for warehouse package preparation.

        **Rust Reference**: prepare_package() lines 107-113

        Uses durable timer for 2-second delay.
        """
        print("  [Shipping] Waiting for warehouse (2s delay)...")
        await schedule_timer_named(2.0, "warehouse-prep")
        print("  [Shipping] Package prepared and labeled")


# ============================================================================
# MAIN FLOW: DAG, child invocations, event-driven completion
# ============================================================================


@dataclass
class ValidationResult:
    """Aggregated validation results."""

    customer_valid: bool
    inventory_available: bool
    fraud_passed: bool


@dataclass
class OrderReceipt:
    """Final order receipt."""

    order_id: str
    payment_id: str
    tracking_id: str
    status: str


@dataclass
@flow_type
class OrderFlow:
    """
    Main order processing flow.

    **Rust Reference**: OrderFlow in comprehensive_demo.rs lines 120-232

    Orchestrates:
    - Parallel validation (DAG)
    - Child flow invocations (payment, shipping)
    - Timer-based fraud check
    """

    order_id: str
    customer_id: str
    amount: float
    address: str

    @flow
    async def process(self) -> OrderReceipt:
        """
        Process order with validations and child flows.

        **Rust Reference**: process() lines 129-168
        """
        print(f"\n[Order {self.order_id}] Starting processing")

        # Run parallel validations (DAG)
        validation = await self.run_validations()
        print(f"[Order {self.order_id}] Validations complete: {validation}")

        # Invoke payment child flow
        print("  [Order] Invoking payment child flow...")
        payment_pending = self.invoke(PaymentFlow(order_id=self.order_id, amount=self.amount))
        payment_id = await payment_pending.result()
        print(f"  [Order] Payment completed: {payment_id}")

        # Invoke shipping child flow
        print("  [Order] Invoking shipping child flow...")
        shipping_pending = self.invoke(ShippingFlow(order_id=self.order_id, address=self.address))
        tracking_id = await shipping_pending.result()
        print(f"  [Order] Shipping arranged: {tracking_id}")

        receipt = OrderReceipt(
            order_id=self.order_id,
            payment_id=payment_id,
            tracking_id=tracking_id,
            status="CONFIRMED",
        )

        print(f"[Order {self.order_id}] Complete! Receipt: {receipt}")
        return receipt

    async def run_validations(self) -> ValidationResult:
        """
        Run parallel validations using DAG execution.

        **Rust Reference**: run_validations() lines 170-177

        Executes customer, inventory, and fraud checks in parallel,
        then aggregates results.
        """
        # Import DAG runtime
        from pyergon.executor.dag_runtime import dag

        # Execute DAG with parallel validation steps
        return await dag(self, final_step="aggregate_validations")

    @step
    async def validate_customer(self) -> bool:
        """
        Validate customer eligibility.

        **Rust Reference**: validate_customer() lines 179-190
        """
        print(f"  [Validation] Checking customer {self.customer_id}...")
        await asyncio.sleep(0.1)

        if self.customer_id.startswith("BLOCKED_"):
            raise Exception("CustomerBlocked")

        print("  [Validation] Customer valid")
        return True

    @step
    async def check_inventory(self) -> bool:
        """
        Check inventory availability.

        **Rust Reference**: check_inventory() lines 192-198
        """
        print("  [Validation] Checking inventory...")
        await asyncio.sleep(0.1)
        print("  [Validation] Inventory available")
        return True

    @step
    async def check_fraud(self) -> bool:
        """
        Run fraud check with timer delay.

        **Rust Reference**: check_fraud() lines 200-208

        Uses 1-second durable timer for fraud analysis delay.
        """
        print("  [Validation] Running fraud check (1s delay)...")
        await schedule_timer_named(1.0, "fraud-check")
        print("  [Validation] Fraud check passed")
        return True

    @step(
        depends_on=["validate_customer", "check_inventory", "check_fraud"],
        inputs={
            "customer": "validate_customer",
            "inventory": "check_inventory",
            "fraud": "check_fraud",
        },
    )
    async def aggregate_validations(
        self, customer: bool, inventory: bool, fraud: bool
    ) -> ValidationResult:
        """
        Aggregate parallel validation results.

        **Rust Reference**: aggregate_validations() lines 210-231

        Depends on: validate_customer, check_inventory, check_fraud
        """
        print("  [Validation] Aggregating results...")

        if not customer or not inventory or not fraud:
            raise Exception("ValidationFailed")

        return ValidationResult(
            customer_valid=customer, inventory_available=inventory, fraud_passed=fraud
        )


# ============================================================================
# MAIN
# ============================================================================


async def main():
    """
    Run comprehensive demo.

    **Rust Reference**: main() lines 253-374
    """
    print("=== COMPREHENSIVE ERGON DEMO ===\n")

    # Initialize storage
    storage = SqliteExecutionLog("data/comprehensive.db")
    await storage.connect()
    await storage.reset()
    scheduler = Scheduler(storage).unversioned()

    # Start 3 workers with timer support
    worker_handles = []
    for i in range(1, 4):
        worker = Worker(
            storage=storage,
            worker_id=f"worker-{i}",
            enable_timers=True,
            poll_interval=0.1,
            timer_interval=0.05,
        )

        # Register all flow types
        # **Rust Reference**: Lines 269-271
        await worker.register(OrderFlow)
        await worker.register(PaymentFlow)
        await worker.register(ShippingFlow)

        handle = await worker.start()
        worker_handles.append(handle)
        print(f"[Setup] Worker {i} started")

    print("\n[Setup] Scheduling orders...\n")

    # Schedule orders
    orders = [
        OrderFlow(
            order_id="ORDER-001", customer_id="CUST-123", amount=99.99, address="123 Main St"
        ),
        OrderFlow(
            order_id="ORDER-002", customer_id="CUST-456", amount=149.50, address="456 Oak Ave"
        ),
    ]

    task_ids: list[uuid.UUID] = []
    for order in orders:
        task_id = await scheduler.schedule(order)
        task_ids.append(task_id)
        print(f"[Client] Scheduled {order.order_id}, task_id={task_id}")

    print("\n[Client] Waiting for completion...\n")

    # Poll-based waiting (event notification not yet implemented in Python)
    timeout = 30.0
    start_time = asyncio.get_event_loop().time()

    while asyncio.get_event_loop().time() - start_time < timeout:
        all_complete = True

        for task_id in task_ids:
            task = await storage.get_scheduled_flow(task_id)
            if task:
                if task.status == TaskStatus.FAILED:
                    print(f"[Client] Flow {task_id} failed: {task.error_message}")
                elif task.status != TaskStatus.COMPLETE:
                    all_complete = False

        if all_complete:
            break

        await asyncio.sleep(0.2)

    print("\n[Client] All orders complete!")
    print("\n=== FINAL RESULTS ===\n")

    # Print final results
    for task_id in task_ids:
        task = await storage.get_scheduled_flow(task_id)
        if task:
            print(f"Task: {task_id}")
            print(f"  Status: {task.status}")
            print(f"  Retry Count: {task.retry_count}")

            if task.error_message:
                print(f"  Error: {task.error_message}")

            # Try to get result from invocation
            inv = await storage.get_invocation(task.flow_id, 0)
            if inv and inv.return_value:
                import pickle

                try:
                    result = pickle.loads(inv.return_value)
                    if isinstance(result, OrderReceipt):
                        print(f"  Receipt: {result}")
                except Exception as e:
                    print(f"  Could not deserialize result: {e}")

            print()

    # Shutdown workers
    for i, handle in enumerate(worker_handles):
        await handle.shutdown()
        print(f"[Cleanup] Worker {i + 1} stopped")

    await storage.close()
    print("[Cleanup] Storage closed")


if __name__ == "__main__":
    asyncio.run(main())
