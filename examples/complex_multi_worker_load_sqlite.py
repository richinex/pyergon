"""Multi-Worker Stress Test with Deterministic Assertions.

Run with:
    PYTHONPATH=src python3 examples/complex_multi_worker_load_sqlite.py

Scenario:
- 4 concurrent workers
- 30 concurrent flows
- SQLite database with event-driven notifications
- Deterministic failure injection
- Child flow invocation testing

This example demonstrates:
1. Step caching works (deterministic execution counts)
2. Retry logic works (10 flows fail once, then succeed)
3. Child flows work (30 LabelFlow invocations)
4. Multi-worker coordination works (no race conditions)
"""

import asyncio
import threading
from dataclasses import dataclass

from pyergon import Scheduler, Worker, flow, flow_type, step
from pyergon.core import RetryPolicy
from pyergon.storage.sqlite import SqliteExecutionLog

# =============================================================================
# GLOBAL METRICS (The Source of Truth)
# =============================================================================


class GlobalMetrics:
    """Thread-safe global metrics for tracking step execution counts.

    Uses threading.Lock to ensure accurate counts with multiple workers
    executing flows concurrently.
    """

    def __init__(self):
        self._lock = threading.Lock()
        self.step_validate = 0
        self.step_fraud = 0
        self.step_inventory = 0
        self.step_payment = 0
        self.step_label = 0
        self.step_notify = 0
        self.completed_flows = 0

        # Per-order attempt tracking (to simulate transient failures)
        self.order_attempts: dict[str, int] = {}

        # Completion notification
        self.completion_event = asyncio.Event()

    def increment_validate(self):
        with self._lock:
            self.step_validate += 1

    def increment_fraud(self):
        with self._lock:
            self.step_fraud += 1

    def increment_inventory(self):
        with self._lock:
            self.step_inventory += 1

    def increment_payment(self):
        with self._lock:
            self.step_payment += 1

    def increment_label(self):
        with self._lock:
            self.step_label += 1

    def increment_notify(self):
        with self._lock:
            self.step_notify += 1

    def get_attempt(self, key: str) -> int:
        """Get and increment attempt counter for a key."""
        with self._lock:
            self.order_attempts[key] = self.order_attempts.get(key, 0) + 1
            return self.order_attempts[key]

    def increment_completed(self):
        """Increment completed flows and notify if reached target."""
        with self._lock:
            self.completed_flows += 1
            if self.completed_flows >= 30:
                # Signal completion asynchronously
                asyncio.create_task(self._signal_completion())

    async def _signal_completion(self):
        """Signal completion event."""
        self.completion_event.set()


# Global singleton
METRICS = GlobalMetrics()


# =============================================================================
# DOMAIN LOGIC
# =============================================================================


@dataclass
class ShippingLabel:
    """Shipping label data returned by child flow."""

    tracking: str


@dataclass
@flow_type
class OrderFlow:
    """Main order processing flow with retry policy.

    Demonstrates:
    - Deterministic failure injection (id % 3 == 0 fails validate)
    - Retry behavior (fails once, then succeeds)
    - Child flow invocation (LabelFlow)
    - Step execution tracking
    """

    id: int
    order_ref: str

    # 1. Validate: Fails once if ID % 3 == 0
    @step
    async def validate(self) -> None:
        """Validate order with deterministic failure injection.

        Fails on first attempt if id % 3 == 0 to test retry logic.
        """
        attempt = METRICS.get_attempt(f"{self.order_ref}-val")
        METRICS.increment_validate()

        # Deterministic failure: fail first attempt for every 3rd order
        if self.id % 3 == 0 and attempt == 1:
            raise ValueError("Simulated Network Blip")

    # 2. Fraud: Never fails
    @step
    async def fraud_check(self) -> None:
        """Fraud check - always succeeds."""
        METRICS.increment_fraud()

    # 3. Inventory: Fails once if ID % 3 == 1
    @step
    async def reserve_inventory(self) -> None:
        """Reserve inventory with deterministic failure injection.

        Fails on first attempt if id % 3 == 1 to test retry logic.
        """
        attempt = METRICS.get_attempt(f"{self.order_ref}-inv")
        METRICS.increment_inventory()

        if self.id % 3 == 1 and attempt == 1:
            raise ValueError("Simulated Deadlock")

    # 4. Payment: Never fails
    @step
    async def process_payment(self) -> None:
        """Process payment - always succeeds."""
        METRICS.increment_payment()

    # 5. Notify: Never fails
    @step
    async def notify(self, label: ShippingLabel) -> None:
        """Send notification - always succeeds."""
        METRICS.increment_notify()

    # MAIN FLOW
    @flow(
        retry_policy=RetryPolicy(
            max_attempts=3, initial_delay_ms=100, backoff_multiplier=2.0, max_delay_ms=1000
        )
    )
    async def run_order(self) -> None:
        """Main order processing flow with retry policy.

        Steps:
        1. Validate (may fail once)
        2. Fraud check
        3. Sleep 10ms (simulate DB contention)
        4. Reserve inventory (may fail once)
        5. Process payment
        6. Invoke child flow (LabelFlow)
        7. Notify
        8. Signal completion
        """
        # Run steps
        await self.validate()
        await self.fraud_check()

        # Simulate DB contention delay
        await asyncio.sleep(0.01)  # 10ms

        await self.reserve_inventory()
        await self.process_payment()

        # Invoke child flow to generate shipping label
        label = await self.invoke(LabelFlow(parent_id=self.id)).result()

        await self.notify(label)

        # Signal completion
        METRICS.increment_completed()


@dataclass
@flow_type(invokable=ShippingLabel)
class LabelFlow:
    """Child flow that generates shipping labels.

    Invoked by OrderFlow to generate tracking numbers.
    """

    parent_id: int

    @flow
    async def generate(self) -> ShippingLabel:
        """Generate shipping label with tracking number."""
        METRICS.increment_label()

        # Simulate label generation work
        await asyncio.sleep(0.02)  # 20ms

        return ShippingLabel(tracking=f"TRK-{self.parent_id}")


# =============================================================================
# MAIN EXECUTION
# =============================================================================


async def main():
    """Execute multi-worker stress test.

    Steps:
    1. Setup SQLite storage
    2. Schedule 30 orders with deterministic IDs
    3. Spawn 4 workers
    4. Wait for all 30 completions
    5. Verify deterministic execution counts
    """
    print("STARTING MULTI-WORKER STRESS TEST (SQLite)")
    print("   - Workers: 4")
    print("   - Orders:  30")
    print("   - DB:      SQLite (Event-Driven Notifications)")
    print()

    # 1. Setup database and scheduler with versioning
    storage = SqliteExecutionLog("test_load.db")
    await storage.connect()
    await storage.reset()  # Clear any previous test data
    scheduler = Scheduler(storage).with_version("test-v1.0")

    # 2. Schedule 30 orders with deterministic failure patterns
    # Group 0 (id % 3 == 0): 10 orders -> Fail validate on first attempt
    # Group 1 (id % 3 == 1): 10 orders -> Fail inventory on first attempt
    # Group 2 (id % 3 == 2): 10 orders -> No failures

    for i in range(30):
        flow = OrderFlow(id=i, order_ref=f"ORD-{i:02d}")
        await scheduler.schedule(flow)

    print("Scheduled 30 flows.")

    # 3. Spawn 4 workers
    workers = []
    for i in range(4):
        worker = Worker(storage=storage, worker_id=f"worker-{i}").with_poll_interval(
            0.05  # 50ms aggressive polling for high concurrency
        )

        await worker.register(OrderFlow)
        await worker.register(LabelFlow)

        # Start worker
        handle = await worker.start()
        workers.append(handle)

    print("4 Workers running. High concurrency imminent...")
    print("Waiting for 30 completions...")
    print()

    # 4. Wait for completion with timeout safety
    try:
        await asyncio.wait_for(METRICS.completion_event.wait(), timeout=30.0)
    except TimeoutError:
        print("WARNING: Timeout waiting for completions")

    # Shutdown workers
    for handle in workers:
        await handle.shutdown()

    # Expected counts based on failure injection logic:
    # - 10 orders (id % 3 == 0): fail validate once -> 2 validate attempts each = 20 total
    # - 10 orders (id % 3 == 1): fail inventory once -> 2 inventory attempts each = 20 total
    # - 10 orders (id % 3 == 2): no failures -> 1 attempt each = 10 total
    # Expected validate: 10 (group 0 first) + 10 (group 0 retry) + 10 (group 1) + 10 (group 2) = 40
    # Expected inventory: 10 (group 1 first) + 10 (group 1 retry) + 10 (group 0) + 10 (group 2) = 40
    # All others: 30 (no retries)

    # Get actual counts
    actual_validate = METRICS.step_validate
    actual_fraud = METRICS.step_fraud
    actual_inventory = METRICS.step_inventory
    actual_payment = METRICS.step_payment
    actual_label = METRICS.step_label
    actual_notify = METRICS.step_notify

    # Print results
    print("FINAL STATISTICS REPORT")
    print("---------------------------------------------------")
    print("Step          | Expected | Actual | Status")
    print("--------------|----------|--------|-------")

    def status(expected, actual):
        return "OK" if actual == expected else "FAIL"

    print(f"Validate      | 40       | {actual_validate:<6} | {status(40, actual_validate)}")
    print(f"Fraud Check   | 30       | {actual_fraud:<6} | {status(30, actual_fraud)}")
    print(f"Inventory     | 40       | {actual_inventory:<6} | {status(40, actual_inventory)}")
    print(f"Payment       | 30       | {actual_payment:<6} | {status(30, actual_payment)}")
    print(f"Child Flow    | 30       | {actual_label:<6} | {status(30, actual_label)}")
    print(f"Notify        | 30       | {actual_notify:<6} | {status(30, actual_notify)}")
    print("---------------------------------------------------")

    # Verify deterministic execution
    if (
        actual_validate == 40
        and actual_fraud == 30
        and actual_inventory == 40
        and actual_payment == 30
        and actual_label == 30
        and actual_notify == 30
    ):
        print("SUCCESS: SYSTEM IS DETERMINISTIC UNDER LOAD")
        await storage.close()
        return 0
    else:
        print("FAILURE: NON-DETERMINISTIC BEHAVIOR DETECTED")
        await storage.close()
        return 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    exit(exit_code)
