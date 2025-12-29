"""
Multi-Worker Stress Test with Deterministic Assertions

**Rust Reference**: ergon_rust/ergon/examples/complex_multi_worker_load.rs

Run with:
    PYTHONPATH=src python3 examples/complex_multi_worker_load.py

Scenario:
- 4 Concurrent Workers
- 30 Concurrent Flows
- Single InMemory Database (High Concurrency)
- Deterministic Failure Injection
- Child Flow Invocation Testing

This example proves:
1. Step caching works (deterministic execution counts)
2. Retry logic works (10 flows fail once, then succeed)
3. Child flows work (30 LabelFlow invocations)
4. Multi-worker coordination works (no race conditions)
"""

import asyncio
from dataclasses import dataclass
from typing import Dict
import threading

from ergon import flow, flow_type, step, Scheduler, Worker
from ergon.core import RetryPolicy
from ergon.storage.memory import InMemoryExecutionLog


# =============================================================================
# GLOBAL METRICS (The Source of Truth)
# =============================================================================

class GlobalMetrics:
    """
    Thread-safe global metrics using threading.Lock.

    **Rust Reference**: AtomicU32 counters in complex_multi_worker_load.rs

    In Python, we use a Lock to protect increment operations.
    This ensures accurate counts even with multiple workers.
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
        self.order_attempts: Dict[str, int] = {}

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
    """
    Main order processing flow with retry policy.

    **Rust Reference**: OrderFlow struct in complex_multi_worker_load.rs lines 54-141

    This flow demonstrates:
    - Deterministic failure injection (id % 3 == 0 fails validate)
    - Retry behavior (fails once, then succeeds)
    - Child flow invocation (LabelFlow)
    - Step execution tracking
    - Retry policy via decorator (matches Rust #[flow(retry = ...)])
    """
    id: int
    order_ref: str

    # 1. Validate: Fails once if ID % 3 == 0
    @step
    async def validate(self) -> None:
        """
        Validate order.

        **Deterministic Failure**: id % 3 == 0, first attempt → fails
        """
        attempt = METRICS.get_attempt(f"{self.order_ref}-val")
        METRICS.increment_validate()

        # Deterministic failure logic (same as Rust)
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
        """
        Reserve inventory.

        **Deterministic Failure**: id % 3 == 1, first attempt → fails
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
    @flow(retry_policy=RetryPolicy(
        max_attempts=3,
        initial_delay_ms=100,
        backoff_multiplier=2.0,
        max_delay_ms=1000
    ))
    async def run_order(self) -> None:
        """
        Main order processing flow.

        **Rust Reference**: run_order() in complex_multi_worker_load.rs lines 108-141

        **Python Note**: Retry policy is set via @flow decorator,
        matching Rust's #[flow(retry = ...)] syntax.

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

        # Simulate DB contention delay (like Rust line 114)
        await asyncio.sleep(0.01)  # 10ms

        await self.reserve_inventory()
        await self.process_payment()

        # Invoke Child Flow
        # **Rust Reference**: lines 125-129
        label = await self.invoke(LabelFlow(parent_id=self.id)).result()

        await self.notify(label)

        # Signal completion
        METRICS.increment_completed()


@dataclass
@flow_type(invokable=ShippingLabel)
class LabelFlow:
    """
    Child flow that generates shipping label.

    **Rust Reference**: LabelFlow struct in complex_multi_worker_load.rs lines 143-163

    This is invoked by OrderFlow as a child flow.
    """
    parent_id: int

    @flow
    async def generate(self) -> ShippingLabel:
        """
        Generate shipping label.

        **Rust Reference**: generate() in complex_multi_worker_load.rs lines 154-162
        """
        METRICS.increment_label()

        # Simulate work (like Rust line 158)
        await asyncio.sleep(0.02)  # 20ms

        return ShippingLabel(tracking=f"TRK-{self.parent_id}")


# =============================================================================
# MAIN EXECUTION
# =============================================================================

async def main():
    """
    Main test execution.

    **Rust Reference**: main() in complex_multi_worker_load.rs lines 169-290

    Steps:
    1. Setup InMemory storage
    2. Schedule 30 orders with deterministic IDs
    3. Spawn 4 workers
    4. Wait for all 30 completions
    5. Verify deterministic execution counts
    """
    print("STARTING MULTI-WORKER STRESS TEST")
    print("   - Workers: 4")
    print("   - Orders:  30")
    print("   - DB:      InMemory (Max Concurrency)")
    print()

    # 1. Setup Database and Scheduler with Versioning
    # **Rust Reference**: lines 180-181
    storage = InMemoryExecutionLog()
    scheduler = Scheduler(storage).with_version("test-v1.0")

    # 2. Schedule 30 Orders (Deterministic Batch)
    # **Rust Reference**: lines 187-193
    # Group 0 (Mod 0): 10 orders -> Fail Validate
    # Group 1 (Mod 1): 10 orders -> Fail Inventory
    # Group 2 (Mod 2): 10 orders -> Clean
    #
    # **Python Note**: Retry policy is now set via @flow decorator (line 121),
    # matching Rust's #[flow(retry = ...)] syntax. No need to pass it here.

    for i in range(30):
        flow = OrderFlow(id=i, order_ref=f"ORD-{i:02d}")
        await scheduler.schedule(flow)

    print("Scheduled 30 flows.")

    # 3. Spawn 4 Workers
    # **Rust Reference**: lines 198-210
    workers = []
    for i in range(4):
        worker = Worker(
            storage=storage,
            worker_id=f"worker-{i}",
            enable_timers=False,
            poll_interval=0.05  # 50ms aggressive polling (like Rust line 203)
        )

        await worker.register(OrderFlow)
        await worker.register(LabelFlow)

        # Start worker
        handle = await worker.start()
        workers.append(handle)

    print("4 Workers running. High concurrency imminent...")
    print("Waiting for 30 completions...")
    print()

    # 4. Wait for completion (Timeout safety)
    # **Rust Reference**: lines 216-218
    try:
        await asyncio.wait_for(METRICS.completion_event.wait(), timeout=30.0)
    except asyncio.TimeoutError:
        print("WARNING: Timeout waiting for completions")

    # Shutdown workers
    # **Rust Reference**: lines 220-223
    for handle in workers:
        await handle.shutdown()

    # Calculate expected counts based on failure injection logic:
    # **Rust Reference**: lines 225-231
    # - 10 orders (id % 3 == 0): fail validate once -> 2 validate attempts each = 20 total
    # - 10 orders (id % 3 == 1): fail inventory once -> 2 inventory attempts each = 20 total
    # - 10 orders (id % 3 == 2): no failures -> 1 attempt each = 10 total
    # Expected validate: 10 (group 0 first) + 10 (group 0 retry) + 10 (group 1) + 10 (group 2) = 40
    # Expected inventory: 10 (group 1 first) + 10 (group 1 retry) + 10 (group 0) + 10 (group 2) = 40
    # All others: 30 (no retries)

    # Get actual counts
    # **Rust Reference**: lines 233-238
    actual_validate = METRICS.step_validate
    actual_fraud = METRICS.step_fraud
    actual_inventory = METRICS.step_inventory
    actual_payment = METRICS.step_payment
    actual_label = METRICS.step_label
    actual_notify = METRICS.step_notify

    # Print results
    # **Rust Reference**: lines 240-274
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

    # Final verdict
    # **Rust Reference**: lines 276-286
    if (actual_validate == 40 and
        actual_fraud == 30 and
        actual_inventory == 40 and
        actual_payment == 30 and
        actual_label == 30 and
        actual_notify == 30):
        print("SUCCESS: SYSTEM IS DETERMINISTIC UNDER LOAD")
        return 0
    else:
        print("FAILURE: NON-DETERMINISTIC BEHAVIOR DETECTED")
        return 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    exit(exit_code)
