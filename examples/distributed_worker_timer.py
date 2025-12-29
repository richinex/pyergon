"""
Distributed worker example with timer processing.

This example demonstrates:
- Scheduling flows that use durable timers
- Multiple workers processing both flows and timers
- Timer coordination across distributed workers
- Event-driven timer notifications (sub-millisecond latency)
- Unified worker shutdown (stops both flow and timer processing)

Scenario:
- 3 order processing flows (each with 2s + 3s timers)
- 2 trial expiry flows (each with 5s timer)
- 2 workers processing flows in parallel
- Event-driven notifications wake workers immediately when timers are scheduled

From Rust: ergon/examples/distributed_worker_timer_sqlite.rs
"""

import asyncio
import logging
from dataclasses import dataclass
from typing import Optional
from uuid import uuid4

from ergon import flow, flow_type, step, Scheduler, Worker
from ergon.executor.timer import schedule_timer_named
from ergon.storage import InMemoryExecutionLog

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
@flow_type
class TimedOrderProcessor:
    """An order processing flow with timed delays."""
    order_id: str
    customer: str
    amount: float

    @flow
    async def process_order(self):
        """Main flow that orchestrates order processing with timed delays."""
        print(f"[{self.order_id}] Starting order processing")

        # Validate the order
        await self.validate_order()

        # Wait for fraud check (simulated with 2 second timer)
        await self.wait_for_fraud_check()

        # Process payment
        await self.process_payment()

        # Wait for warehouse processing (simulated with 3 second timer)
        await self.wait_for_warehouse()

        # Ship the order
        await self.ship_order()

        print(f"[{self.order_id}] Order completed!")

        return {"order_id": self.order_id, "status": "Completed"}

    @step
    async def validate_order(self) -> bool:
        """Validate order details."""
        print(f"[{self.order_id}] Validating order for {self.customer} (amount: ${self.amount})")

        if self.amount <= 0.0:
            raise ValueError(f"Invalid amount: {self.amount}")

        return True

    @step
    async def wait_for_fraud_check(self) -> None:
        """Wait for fraud check using durable timer."""
        print(f"[{self.order_id}] Waiting 2s for fraud check...")
        await schedule_timer_named(2.0, f"fraud-check-{self.order_id}")
        print(f"[{self.order_id}] Fraud check complete")

    @step
    async def process_payment(self) -> str:
        """Process payment."""
        print(f"[{self.order_id}] Processing payment of ${self.amount}")
        return f"payment-{self.order_id}"

    @step
    async def wait_for_warehouse(self) -> None:
        """Wait for warehouse processing using durable timer."""
        print(f"[{self.order_id}] Waiting 3s for warehouse processing...")
        await schedule_timer_named(3.0, f"warehouse-{self.order_id}")
        print(f"[{self.order_id}] Warehouse processing complete")

    @step
    async def ship_order(self) -> str:
        """Ship the order."""
        print(f"[{self.order_id}] Shipping order")
        return f"tracking-{self.order_id}"


@dataclass
@flow_type
class TrialExpiryNotification:
    """A trial expiry notification flow with timer."""
    user_id: str
    email: str

    @flow
    async def send_expiry_notice(self):
        """Send trial expiry notice after waiting for trial period."""
        print(f"[Trial {self.user_id}] Starting trial expiry flow")

        # Wait for trial period (simulated with 5 second timer)
        await self.wait_for_trial_period()

        # Send expiry notice
        await self.send_notification()

        print(f"[Trial {self.user_id}] Trial expiry flow completed!")

        return f"Trial expiry notice sent to {self.user_id}"

    @step
    async def wait_for_trial_period(self) -> None:
        """Wait for trial period to expire using durable timer."""
        print(f"[Trial {self.user_id}] Waiting 5s for trial period to expire...")
        await schedule_timer_named(5.0, f"trial-expiry-{self.user_id}")
        print(f"[Trial {self.user_id}] Trial period expired")

    @step
    async def send_notification(self) -> bool:
        """Send expiry notification email."""
        print(f"[Trial {self.user_id}] Sending expiry notice to {self.email}")
        return True


async def main():
    """
    Run distributed timer example with event-driven notifications.

    This demonstrates the notification system improvements:
    - Workers wake immediately when timers are scheduled (fast path)
    - Fallback to calculated sleep if notification missed (safety net)
    - Sub-millisecond latency vs 100ms polling
    """
    print("\nDISTRIBUTED TIMER PROCESSING DEMO")
    print("=" * 60)
    print("Event-Driven Features:")
    print("  - Timer notifications wake workers immediately")
    print("  - Work notifications wake workers when flows enqueued")
    print("  - Sub-millisecond latency vs polling")
    print("=" * 60)
    print()

    # Initialize storage
    storage = InMemoryExecutionLog()

    # Start worker 1 with timer processing enabled
    worker1 = Worker(storage, "worker-1", enable_timers=True, poll_interval=0.1)
    await worker1.register(TimedOrderProcessor)
    await worker1.register(TrialExpiryNotification)
    handle1 = await worker1.start()

    # Start worker 2 with timer processing enabled
    worker2 = Worker(storage, "worker-2", enable_timers=True, poll_interval=0.1)
    await worker2.register(TimedOrderProcessor)
    await worker2.register(TrialExpiryNotification)
    handle2 = await worker2.start()

    # Give workers time to initialize
    await asyncio.sleep(0.5)

    # Create scheduler
    scheduler = Scheduler(storage).with_version("v1.0")

    print("Scheduling 3 order processing flows...")
    # Schedule order processing flows
    for i in range(1, 4):
        order = TimedOrderProcessor(
            order_id=f"ORD-{i:03d}",
            customer=f"Customer {i}",
            amount=100.0 * i
        )
        await scheduler.schedule(order)

    print("Scheduling 2 trial expiry flows...")
    # Schedule trial expiry flows
    for i in range(1, 3):
        trial = TrialExpiryNotification(
            user_id=f"user-{i}",
            email=f"user{i}@example.com"
        )
        await scheduler.schedule(trial)

    print("\n2 workers running. Processing 5 flows with timers...")
    print("Expected: ~6 seconds total (with parallelism)")
    print()

    # Give workers time to pick up flows
    await asyncio.sleep(0.5)

    # Wait for all flows to complete with timeout
    timeout_duration = 30.0
    try:
        await asyncio.wait_for(
            wait_for_completion(storage),
            timeout=timeout_duration
        )
        print("\n" + "=" * 60)
        print("SUCCESS: All flows completed!")
        print("=" * 60)
    except asyncio.TimeoutError:
        incomplete = await storage.get_incomplete_flows()
        print(f"\nTimeout - Incomplete flows: {len(incomplete)}")
        for inv in incomplete:
            print(f"  - {inv.id()} ({inv.class_name()})")

    # Shutdown workers
    print("\nShutting down workers...")
    await handle1.shutdown()
    await handle2.shutdown()


async def wait_for_completion(storage: InMemoryExecutionLog):
    """Wait for all flows to complete."""
    while True:
        incomplete = await storage.get_incomplete_flows()
        if not incomplete:
            break
        await asyncio.sleep(0.5)


if __name__ == "__main__":
    asyncio.run(main())
