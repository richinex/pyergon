"""
Saga Pattern (Compensating Transactions)

Demonstrates how to implement "Backward Recovery".
If a later step fails, we manually trigger "Undo" steps for
previous successes.

**Rust Reference**: `/home/richinex/Documents/devs/rust_projects/ergon/ergon/examples/saga.rs`
"""

import asyncio
from dataclasses import dataclass

from ergon import flow, flow_type, step, Worker, Scheduler, InMemoryExecutionLog

# Track completions to exit cleanly
completed_count = 0
done_event = None
count_lock = None


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
        print(f"   [1] Booking flight to {self.destination}...")
        await asyncio.sleep(0.05)
        return f"FLIGHT-{self.destination.upper()}"

    @step
    async def book_hotel(self) -> str:
        """
        Book hotel at destination.

        **Rust Reference**: lines 40-45
        """
        print(f"   [2] Booking hotel in {self.destination}...")
        await asyncio.sleep(0.05)
        return f"HOTEL-{self.destination.upper()}"

    @step
    async def book_car(self) -> str:
        """
        Book rental car at destination.

        **Rust Reference**: lines 47-58

        Fails if destination is "Atlantis" (no cars underwater!)
        """
        print(f"   [3] Attempting to book car...")
        await asyncio.sleep(0.05)

        if self.destination == "Atlantis":
            print(f"      Error: No cars available in Atlantis!")
            raise Exception("InventoryExhausted")

        return f"CAR-{self.destination.upper()}"

    # --- COMPENSATING STEPS (UNDO) ---

    @step
    async def cancel_hotel(self, hotel_id: str) -> None:
        """
        Cancel hotel reservation (compensation).

        **Rust Reference**: lines 62-67
        """
        print(f"   [Rollback] Cancelling Hotel Reservation: {hotel_id}")
        await asyncio.sleep(0.05)

    @step
    async def cancel_flight(self, flight_id: str) -> None:
        """
        Cancel flight reservation (compensation).

        **Rust Reference**: lines 69-74
        """
        print(f"   [Rollback] Cancelling Flight Reservation: {flight_id}")
        await asyncio.sleep(0.05)

    # --- ORCHESTRATOR ---

    @staticmethod
    async def mark_complete():
        """
        Helper to increment counter when flow ends (Success or Failure).

        **Rust Reference**: lines 78-85
        """
        global completed_count, done_event, count_lock

        async with count_lock:
            completed_count += 1
            count = completed_count

        if count >= 2:
            # We expect 2 flows (Paris + Atlantis)
            done_event.set()

    @flow
    async def run_saga(self) -> str:
        """
        Execute saga with automatic compensation on failure.

        **Rust Reference**: lines 87-126 (run_saga method)

        Now mirrors Rust exactly - method is named `run_saga()` and uses
        explicit registration via lambda: `worker.register(HolidaySaga, lambda saga: saga.run_saga())`

        In Rust, Result<String, String> means both Ok and Err are terminal states.
        In Python, we use SagaError with is_retryable()=False so the worker
        treats compensation failures as terminal states (no retry).
        """
        try:
            result = await self.execute_logic()
            await self.mark_complete()
            return result
        except SagaError:
            # Already a SagaError (from execute_logic), just mark complete and re-raise
            await self.mark_complete()
            raise
        except Exception as e:
            # Unexpected error - wrap in SagaError
            await self.mark_complete()
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
            print(f"   Hotel failed. Compensating Flight...")
            await self.cancel_flight(flight_id)
            raise SagaError(f"Hotel failed: {e}")

        # 3. Book Car (Trip Wire)
        try:
            car_id = await self.book_car()
        except Exception as e:
            print(f"   Car failed. Initiating Rollback Sequence...")
            # Compensation Logic: Reverse Order
            await self.cancel_hotel(hotel_id)
            await self.cancel_flight(flight_id)
            raise SagaError(f"Saga Failed (Rolled Back): {e}")

        msg = f"CONFIRMED: {flight_id} / {hotel_id} / {car_id}"
        print(f"   {msg}")
        return msg


# =============================================================================
# MAIN
# =============================================================================

async def main():
    """
    Main saga orchestrator.

    **Rust Reference**: lines 133-160

    Runs two scenarios:
    1. Paris -> Success (all steps complete)
    2. Atlantis -> Failure at car booking (triggers compensation)
    """
    global completed_count, done_event, count_lock

    # Initialize global state
    completed_count = 0
    done_event = asyncio.Event()
    count_lock = asyncio.Lock()

    print("\n" + "="*70)
    print("SAGA PATTERN - Compensating Transactions")
    print("="*70)

    storage = InMemoryExecutionLog()
    scheduler = Scheduler(storage).with_version("v1.0")

    # Scenario 1: Success (Paris)
    print("\n[Scenario 1] Booking trip to Paris (should succeed)...")
    saga_success = HolidaySaga(destination="Paris")
    await scheduler.schedule(saga_success)

    # Scenario 2: Failure + Compensation (Atlantis)
    print("\n[Scenario 2] Booking trip to Atlantis (will fail at car booking)...")
    saga_fail = HolidaySaga(destination="Atlantis")
    await scheduler.schedule(saga_fail)

    # Start worker
    worker = Worker(
        storage=storage,
        worker_id="saga-worker",
        poll_interval=0.1
    )

    await worker.register(HolidaySaga)
    handle = await worker.start()

    # Wait until both flows finish (Success + Failure)
    await done_event.wait()

    await handle.shutdown()

    print("\n" + "="*70)
    print("SAGA DEMO COMPLETE")
    print("="*70)
    print("Both scenarios executed:")
    print("  1. Paris: Succeeded with all bookings")
    print("  2. Atlantis: Failed at car, compensated hotel and flight")
    print("="*70 + "\n")


if __name__ == "__main__":
    asyncio.run(main())
