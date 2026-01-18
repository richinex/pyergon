"""
Complete order processing system with versioning (clean output)

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
from pyergon.core import RetryPolicy
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

        """
        await self.prepare_package()
        return f"TRACK-{uuid.uuid4().hex[:8]}"

    @step
    async def prepare_package(self) -> None:
        """
        Wait for warehouse package preparation.

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

        """
        from pyergon.executor.dag_runtime import dag

        return await dag(self, final_step="aggregate_validations")

    @step
    async def validate_customer(self) -> bool:
        """
        Validate customer eligibility.

        """
        await asyncio.sleep(0.1)
        if self.customer_id.startswith("BLOCKED_"):
            raise Exception("CustomerBlocked")
        return True

    @step
    async def check_inventory(self) -> bool:
        """
        Check inventory availability.

        """
        await asyncio.sleep(0.1)
        return True

    @step
    async def check_fraud(self) -> bool:
        """
        Run fraud check with timer delay.

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
            .with_timers()
            .with_timer_interval(0.05)
            .with_poll_interval(0.1)
        )

        # Register all flow types
        await worker.register(OrderFlow)
        await worker.register(PaymentFlow)
        await worker.register(ShippingFlow)

        handle = await worker.start()
        worker_handles.append(handle)

    # Schedule orders with different versions
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

    # Wait for all tasks to complete using race-free helper method
    timeout = 30.0

    try:
        await asyncio.wait_for(
            storage.wait_for_all([task_id for task_id, _ in task_ids]), timeout=timeout
        )
    except TimeoutError:
        print("[Warning] Timeout waiting for flows to complete")

    await _print_results(storage, task_ids)

    # Shutdown workers
    for handle in worker_handles:
        await handle.shutdown()

    await storage.close()


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
