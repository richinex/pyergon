"""
Complete order processing system with versioning (clean output)

**Rust Reference**: ergon_rust/ergon/examples/comprehensive_demo_3.rs

Clean production-ready example with:
- NO logging during execution (silent flows)
- Multiple deployment versions (v1.0 vs v2.0) running concurrently
- Event-driven completion waiting
- Only final results printed

Output format:
    {task_id}: {status} (retries: {count})
      {receipt}

Run:
    PYTHONPATH=src python3 examples/comprehensive_demo_3.py
"""

import asyncio
import hashlib
import logging
import uuid
from dataclasses import dataclass

from pyergon import Scheduler, Worker, flow, flow_type, step
from pyergon.core import RetryPolicy, TaskStatus
from pyergon.executor.timer import schedule_timer_named
from pyergon.storage.sqlite import SqliteExecutionLog

# Suppress worker logging for clean output (like Rust tracing::debug!)
logging.basicConfig(level=logging.CRITICAL)


# ============================================================================
# CHILD FLOWS
# ============================================================================


@dataclass
@flow_type(invokable=str)
class PaymentFlow:
    """Payment processing child flow (no logging)."""

    order_id: str
    amount: float

    @flow(retry_policy=RetryPolicy.STANDARD)
    async def process(self) -> str:
        """
        Process payment with simulated retryable failures.

        **Rust Reference**: process() lines 28-35
        """
        # Deterministic failure simulation
        hash_value = int(hashlib.md5(self.order_id.encode()).hexdigest(), 16)
        should_fail = (hash_value % 10) < 3

        await self.charge_card(should_fail)
        return f"PAYMENT-{uuid.uuid4().hex[:8]}"

    @step
    async def charge_card(self, should_fail: bool) -> None:
        """
        Charge customer's card.

        **Rust Reference**: charge_card() lines 37-44
        """
        await asyncio.sleep(0.1)
        if should_fail:
            raise Exception("NetworkTimeout")


@dataclass
@flow_type(invokable=str)
class ShippingFlow:
    """Shipping arrangement child flow (no logging)."""

    order_id: str
    address: str

    @flow
    async def process(self) -> str:
        """
        Arrange shipping with warehouse preparation delay.

        **Rust Reference**: process() lines 56-59
        """
        await self.prepare_package()
        return f"TRACK-{uuid.uuid4().hex[:8]}"

    @step
    async def prepare_package(self) -> None:
        """
        Wait for warehouse package preparation.

        **Rust Reference**: prepare_package() lines 61-65
        """
        await schedule_timer_named(2.0, "warehouse-prep")


# ============================================================================
# MAIN FLOW
# ============================================================================


@dataclass
class ValidationResult:
    """Aggregated validation results."""

    customer_valid: bool
    inventory_available: bool
    fraud_passed: bool


@dataclass
class OrderReceipt:
    """
    Final order receipt with versioning information.

    **Rust Reference**: OrderReceipt lines 170-178
    """

    order_id: str
    flow_id: str
    payment_id: str
    tracking_id: str
    status: str
    version: str


@dataclass
@flow_type
class OrderFlow:
    """
    Main order processing flow with versioning.

    **Rust Reference**: OrderFlow in comprehensive_demo_3.rs lines 68-161
    """

    order_id: str
    customer_id: str
    amount: float
    address: str
    version: str
    flow_id: str

    @flow
    async def process(self) -> OrderReceipt:
        """
        Process order (no logging).

        **Rust Reference**: process() lines 80-107
        """
        # Run parallel validations (DAG)
        await self.run_validations()

        # Invoke payment child flow
        payment_pending = self.invoke(PaymentFlow(order_id=self.order_id, amount=self.amount))
        payment_id = await payment_pending.result()

        # Invoke shipping child flow
        shipping_pending = self.invoke(ShippingFlow(order_id=self.order_id, address=self.address))
        tracking_id = await shipping_pending.result()

        return OrderReceipt(
            order_id=self.order_id,
            flow_id=self.flow_id,
            payment_id=payment_id,
            tracking_id=tracking_id,
            status="CONFIRMED",
            version=self.version,
        )

    async def run_validations(self) -> ValidationResult:
        """
        Run parallel validations using DAG execution.

        **Rust Reference**: run_validations() lines 109-116
        """
        from pyergon.executor.dag_runtime import dag

        return await dag(self, final_step="aggregate_validations")

    @step
    async def validate_customer(self) -> bool:
        """
        Validate customer eligibility.

        **Rust Reference**: validate_customer() lines 118-125
        """
        await asyncio.sleep(0.1)
        if self.customer_id.startswith("BLOCKED_"):
            raise Exception("CustomerBlocked")
        return True

    @step
    async def check_inventory(self) -> bool:
        """
        Check inventory availability.

        **Rust Reference**: check_inventory() lines 127-131
        """
        await asyncio.sleep(0.1)
        return True

    @step
    async def check_fraud(self) -> bool:
        """
        Run fraud check with timer delay.

        **Rust Reference**: check_fraud() lines 133-139
        """
        await schedule_timer_named(1.0, "fraud-check")
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

        **Rust Reference**: aggregate_validations() lines 141-160
        """
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
    Run comprehensive demo with versioning.

    **Rust Reference**: main() lines 181-269
    """
    # Initialize storage
    storage = SqliteExecutionLog("data/comprehensive_demo_3.db")
    await storage.connect()
    await storage.reset()

    # Start 3 workers with timer support
    worker_handles = []
    for i in range(1, 4):
        worker = (
            Worker(storage=storage, worker_id=f"worker-{i}")
            .with_timers(interval=0.05)
            .with_poll_interval(0.1)
        )

        # Register all flow types
        # **Rust Reference**: Lines 192-194
        await worker.register(OrderFlow)
        await worker.register(PaymentFlow)
        await worker.register(ShippingFlow)

        handle = await worker.start()
        worker_handles.append(handle)

    # Schedule orders with different versions
    # **Rust Reference**: Lines 199-218
    orders = [
        ("ORDER-001", "CUST-123", 99.99, "123 Main St", "v1.0"),
        ("ORDER-002", "CUST-456", 149.50, "456 Oak Ave", "v2.0"),
    ]

    task_ids: list[tuple[str, str]] = []
    for order_id, customer_id, amount, address, version in orders:
        flow_id = uuid.uuid4()
        order = OrderFlow(
            order_id=order_id,
            customer_id=customer_id,
            amount=amount,
            address=address,
            version=version,
            flow_id=str(flow_id),
        )

        # Create scheduler with version
        scheduler = Scheduler(storage).with_version(version)
        task_id = await scheduler.schedule(order, flow_id=str(flow_id))
        task_ids.append((task_id, version))

    # Event-driven waiting using status_notify()
    # **Rust Reference**: Lines 220-239
    status_notify = storage.status_notify()
    timeout = 30.0

    try:
        await asyncio.wait_for(
            _wait_for_completion(storage, task_ids, status_notify), timeout=timeout
        )
    except TimeoutError:
        print("[Warning] Timeout waiting for flows to complete")

    # Print final results
    # **Rust Reference**: Lines 241-261
    await _print_results(storage, task_ids)

    # Shutdown workers
    # **Rust Reference**: Lines 263-266
    for handle in worker_handles:
        await handle.shutdown()

    await storage.close()


async def _wait_for_completion(
    storage, task_ids: list[tuple[str, str]], status_notify: asyncio.Event
):
    """
    Wait for all flows to complete using event-driven notifications.

    **Rust Reference**: comprehensive_demo_3.rs lines 221-239
    """
    while True:
        all_complete = True

        for task_id, _ in task_ids:
            task = await storage.get_scheduled_flow(task_id)
            if task:
                if task.status not in (TaskStatus.COMPLETE, TaskStatus.FAILED):
                    all_complete = False
                    break

        if all_complete:
            break

        # Wait for status change notification
        await status_notify.wait()
        status_notify.clear()  # Reset for next notification


async def _print_results(storage, task_ids: list[tuple[str, str]]):
    """Print final results for all tasks."""
    for task_id, _ in task_ids:
        task = await storage.get_scheduled_flow(task_id)
        if task:
            print(f"{task_id}: {task.status} (retries: {task.retry_count})")

            # Try to get result from invocation
            inv = await storage.get_invocation(task.flow_id, 0)
            if inv and inv.return_value:
                import pickle

                try:
                    result = pickle.loads(inv.return_value)
                    if isinstance(result, OrderReceipt):
                        print(f"  {result}")
                except Exception as e:
                    print(f"  error: {e}")


if __name__ == "__main__":
    asyncio.run(main())
