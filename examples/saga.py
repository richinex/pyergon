"""
Saga Pattern (Compensating Transactions)

Demonstrates how to implement "Backward Recovery".
If a later step fails, we manually trigger "Undo" steps for
previous successes.

**Rust Reference**: `/home/richinex/Documents/devs/rust_projects/ergon/ergon/examples/saga.rs`
"""

import asyncio
import logging
from dataclasses import dataclass

from pyergon import InMemoryExecutionLog, Scheduler, Worker, flow, flow_type, step
from pyergon.core import TaskStatus

# Suppress worker logging for clean output
logging.basicConfig(level=logging.CRITICAL)


# =============================================================================
# DOMAIN LOGIC
# =============================================================================


class SagaError(Exception):
    """Base class for saga errors with retry protocol."""

    def is_retryable(self) -> bool:
        """
        Determine if error is retriable.

        Saga compensation failures are NOT retriable because:
        1. They represent business logic failures (e.g., inventory exhausted)
        2. Compensation has already been executed
        3. Retrying would re-run compensation incorrectly
        """
        return False


@dataclass
@flow_type
class HolidaySaga:
    """
    Saga for booking a holiday package.

    **Rust Reference**: lines 25-127

    Flow:
    1. Book flight
    2. Book hotel (if fails, cancel flight)
    3. Book car (if fails, cancel hotel + flight)

    Scenarios:
    - destination="Paris" -> Success (all bookings succeed)
    - destination="Atlantis" -> Failure at car booking (triggers compensation)
    """

    destination: str  # "Paris" = Success, "Atlantis" = Car Failure

    # --- FORWARD STEPS ---

    @step
    async def book_flight(self) -> str:
        """
        Book flight to destination.

        **Rust Reference**: lines 33-38
        """
        await asyncio.sleep(0.05)
        return f"FLIGHT-{self.destination.upper()}"

    @step
    async def book_hotel(self) -> str:
        """
        Book hotel at destination.

        **Rust Reference**: lines 40-45
        """
        await asyncio.sleep(0.05)
        return f"HOTEL-{self.destination.upper()}"

    @step
    async def book_car(self) -> str:
        """
        Book rental car at destination.

        **Rust Reference**: lines 47-58

        Fails if destination is "Atlantis" (no cars underwater!)
        """
        await asyncio.sleep(0.05)

        if self.destination == "Atlantis":
            raise Exception("InventoryExhausted")

        return f"CAR-{self.destination.upper()}"

    # --- COMPENSATING STEPS (UNDO) ---

    @step
    async def cancel_hotel(self, hotel_id: str) -> None:
        """
        Cancel hotel reservation (compensation).

        **Rust Reference**: lines 62-67
        """
        await asyncio.sleep(0.05)

    @step
    async def cancel_flight(self, flight_id: str) -> None:
        """
        Cancel flight reservation (compensation).

        **Rust Reference**: lines 69-74
        """
        await asyncio.sleep(0.05)

    # --- FLOW ENTRY POINT ---

    @flow
    async def run_saga(self) -> str:
        """
        Execute saga with automatic compensation on failure.

        In Rust, Result<String, String> means both Ok and Err are terminal states.
        In Python, we use SagaError with is_retryable()=False so the worker
        treats compensation failures as terminal states (no retry).
        """
        try:
            return await self.execute_logic()
        except SagaError:
            raise
        except Exception as e:
            raise SagaError(str(e)) from e

    async def execute_logic(self) -> str:
        """
        Saga execution logic with manual compensation.

        **Rust Reference**: lines 97-126

        Pattern:
        - Try each step sequentially
        - On failure, manually compensate previous steps
        - Return error after compensation
        """
        # 1. Book Flight
        flight_id = await self.book_flight()

        # 2. Book Hotel
        try:
            hotel_id = await self.book_hotel()
        except Exception as e:
            await self.cancel_flight(flight_id)
            raise SagaError(f"Hotel failed: {e}")

        # 3. Book Car (Trip Wire)
        try:
            car_id = await self.book_car()
        except Exception as e:
            # Compensation Logic: Reverse Order
            await self.cancel_hotel(hotel_id)
            await self.cancel_flight(flight_id)
            raise SagaError(f"Saga Failed (Rolled Back): {e}")

        return f"CONFIRMED: {flight_id} / {hotel_id} / {car_id}"


# =============================================================================
# MAIN
# =============================================================================


async def _wait_for_completion(storage, task_ids, status_notify):
    """Wait for all sagas to complete using event-driven notifications."""
    while True:
        all_complete = True

        for task_id in task_ids:
            task = await storage.get_scheduled_flow(task_id)
            if task:
                if task.status not in (TaskStatus.COMPLETE, TaskStatus.FAILED):
                    all_complete = False
                    break

        if all_complete:
            break

        await status_notify.wait()
        status_notify.clear()


async def _print_results(storage, scenarios):
    """Print final results for all scenarios."""
    for destination, task_id in scenarios:
        task = await storage.get_scheduled_flow(task_id)
        if task:
            result = "success" if task.status == TaskStatus.COMPLETE else "compensated"
            print(f"{destination}: {result}")


async def main():
    """
    Main saga orchestrator.

    **Rust Reference**: lines 133-160

    Runs two scenarios:
    1. Paris -> Success (all steps complete)
    2. Atlantis -> Failure at car booking (triggers compensation)
    """
    storage = InMemoryExecutionLog()
    scheduler = Scheduler(storage).with_version("v1.0")

    # Schedule sagas
    scenarios = []

    saga_paris = HolidaySaga(destination="Paris")
    task_id_paris = await scheduler.schedule(saga_paris)
    scenarios.append(("Paris", task_id_paris))

    saga_atlantis = HolidaySaga(destination="Atlantis")
    task_id_atlantis = await scheduler.schedule(saga_atlantis)
    scenarios.append(("Atlantis", task_id_atlantis))

    # Start worker
    worker = Worker(storage, "saga-worker").with_poll_interval(0.1)
    await worker.register(HolidaySaga)
    handle = await worker.start()

    # Wait for completion using event-driven approach
    status_notify = storage.status_notify()
    task_ids = [task_id for _, task_id in scenarios]

    try:
        await asyncio.wait_for(_wait_for_completion(storage, task_ids, status_notify), timeout=10.0)
    except TimeoutError:
        print("[Warning] Timeout waiting for sagas to complete")

    # Print results
    await _print_results(storage, scenarios)

    await handle.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
