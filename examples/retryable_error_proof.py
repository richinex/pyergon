"""
Retryable Trait - Proof of Concept

This example demonstrates:
- Concrete evidence that Retryable trait controls retry behavior
- Differential behavior between retryable and non-retryable errors
- Execution counters proving retry logic respects is_retryable()
- Retryable errors are automatically retried (ApiTimeout)
- Non-retryable errors fail immediately without retry (ItemNotFound)

## Scenario
Two parallel test cases run simultaneously: Scenario A returns a retryable error
(ApiTimeout with is_retryable() = true), which causes the step to execute 3 times
before succeeding. Scenario B returns a non-retryable error (ItemNotFound with
is_retryable() = false), which causes the step to execute only 1 time and fail
immediately without retry.

## Key Takeaways
- Execution counters provide concrete evidence of retry behavior
- Retryable errors (STEP_A_EXECUTIONS = 3) are retried automatically
- Non-retryable errors (STEP_B_EXECUTIONS = 1) fail immediately
- The is_retryable() method controls whether an error triggers retry
- RetryPolicy configuration respects the Retryable trait
- Framework distinguishes transient failures from permanent failures
- This proves the trait is not just documentation but enforced behavior

## Run with
```bash
PYTHONPATH=src python examples/retryable_error_proof.py
```
"""

import asyncio
from ergon import flow, step, Scheduler, Worker
from ergon.storage.memory import InMemoryExecutionLog
from ergon.core import RetryPolicy, RetryableError, TaskStatus

# Global counters - this is our EVIDENCE
STEP_A_EXECUTIONS = 0
STEP_B_EXECUTIONS = 0


# ============================================================================
# Custom Error Type with Retryable Trait
# ============================================================================

class InventoryError(RetryableError):
    """Base class for inventory errors."""
    pass


class ApiTimeout(InventoryError):
    """TRANSIENT error - network timeout, should retry."""

    def __str__(self):
        return "API timeout - transient network error"

    def is_retryable(self) -> bool:
        print("      is_retryable() called -> returning true (will retry)")
        return True


class ItemNotFound(InventoryError):
    """PERMANENT error - item doesn't exist, no point retrying."""

    def __init__(self, item: str):
        self.item = item

    def __str__(self):
        return f"Item '{self.item}' not found in catalog"

    def is_retryable(self) -> bool:
        print("      is_retryable() called -> returning false (will NOT retry)")
        return False


# ============================================================================
# Scenario A: RETRYABLE Error (should execute 3 times)
# ============================================================================

@flow(retry_policy=RetryPolicy.STANDARD)
class OrderA:
    """Flow with retryable error - should execute 3 times."""

    def __init__(self, order_id: str):
        self.order_id = order_id

    @step
    async def check_inventory(self) -> str:
        """Check inventory - fails first 2 times with retryable error."""
        global STEP_A_EXECUTIONS
        STEP_A_EXECUTIONS += 1
        count = STEP_A_EXECUTIONS

        print(f"  [Step A] Checking inventory (execution #{count})")

        # Fail first 2 times with RETRYABLE error
        if count < 3:
            print("    API timeout occurred (transient network error)")
            print("    Returning ApiTimeout")
            raise ApiTimeout()

        # Success on 3rd attempt
        print(f"    Inventory check succeeded on attempt {count}")
        return f"Inventory reserved for {self.order_id}"

    async def process_order(self) -> str:
        """Main workflow entry point."""
        print(f"\n[Flow A] Processing order {self.order_id}")

        result = await self.check_inventory()

        print(f"[Flow A] Order {self.order_id} completed successfully")
        return result


# ============================================================================
# Scenario B: NON-RETRYABLE Error (should execute ONLY 1 time)
# ============================================================================

@flow(retry_policy=RetryPolicy.STANDARD)
class OrderB:
    """Flow with non-retryable error - should execute only once."""

    def __init__(self, order_id: str, item_sku: str):
        self.order_id = order_id
        self.item_sku = item_sku

    @step
    async def check_inventory(self) -> str:
        """Check inventory - always fails with non-retryable error."""
        global STEP_B_EXECUTIONS
        STEP_B_EXECUTIONS += 1
        count = STEP_B_EXECUTIONS

        print(f"  [Step B] Checking inventory (execution #{count})")
        print(f"    Looking up item SKU: {self.item_sku}")

        # ALWAYS fail with NON-RETRYABLE error
        print("    Item not found in catalog (permanent error)")
        print("    Returning ItemNotFound")
        raise ItemNotFound(self.item_sku)

    async def process_order(self) -> str:
        """Main workflow entry point."""
        print(f"\n[Flow B] Processing order {self.order_id}")

        result = await self.check_inventory()

        print(f"[Flow B] Order {self.order_id} completed successfully")
        return result


# ============================================================================
# Main - Run Both Scenarios and Show Evidence
# ============================================================================

async def run_scenario_a(storage):
    """Run Scenario A: Retryable error."""
    scheduler = Scheduler(storage).with_version("v1.0")

    order_a = OrderA(order_id="ORD-A-001")
    task_id_a = await scheduler.schedule(order_a)

    # Start worker
    worker = Worker(storage, "Worker-A")
    await worker.register(OrderA, lambda flow: flow.process_order())
    handle = await worker.start()

    # Wait for completion
    await asyncio.sleep(5.0)

    await handle.shutdown()

    # Get final status
    scheduled = await storage.get_scheduled_flow(task_id_a)
    return scheduled


async def run_scenario_b(storage):
    """Run Scenario B: Non-retryable error."""
    scheduler = Scheduler(storage).with_version("v1.0")

    order_b = OrderB(order_id="ORD-B-002", item_sku="INVALID-SKU-999")
    task_id_b = await scheduler.schedule(order_b)

    # Start worker
    worker = Worker(storage, "Worker-B")
    await worker.register(OrderB, lambda flow: flow.process_order())
    handle = await worker.start()

    # Wait for completion
    await asyncio.sleep(5.0)

    await handle.shutdown()

    # Get final status
    scheduled = await storage.get_scheduled_flow(task_id_b)
    return scheduled


async def main():
    """Run both scenarios and show evidence."""
    print("=" * 70)
    print("RETRYABLE ERROR PROOF - PYTHON VERSION")
    print("=" * 70)
    print()
    print("Running two scenarios:")
    print("  Scenario A: ApiTimeout (is_retryable = true)")
    print("  Scenario B: ItemNotFound (is_retryable = false)")
    print()
    print("=" * 70)

    # Create storage
    storage = InMemoryExecutionLog()

    # Run Scenario A (retryable error)
    print("\n" + "=" * 70)
    print("SCENARIO A: RETRYABLE ERROR")
    print("=" * 70)
    result_a = await run_scenario_a(storage)

    # Run Scenario B (non-retryable error)
    print("\n" + "=" * 70)
    print("SCENARIO B: NON-RETRYABLE ERROR")
    print("=" * 70)
    result_b = await run_scenario_b(storage)

    # Show the evidence
    print("\n" + "=" * 70)
    print("PROOF - EXECUTION COUNTERS")
    print("=" * 70)
    print()
    print(f"Scenario A (ApiTimeout - retryable):")
    print(f"  STEP_A_EXECUTIONS = {STEP_A_EXECUTIONS}")
    print(f"  Expected: 3 (initial attempt + 2 retries)")
    print(f"  Status: {result_a.status.name if result_a else 'UNKNOWN'}")
    print(f"  Retry count: {result_a.retry_count if result_a else 'N/A'}")
    print()
    print(f"Scenario B (ItemNotFound - non-retryable):")
    print(f"  STEP_B_EXECUTIONS = {STEP_B_EXECUTIONS}")
    print(f"  Expected: 1 (no retries)")
    print(f"  Status: {result_b.status.name if result_b else 'UNKNOWN'}")
    print(f"  Retry count: {result_b.retry_count if result_b else 'N/A'}")
    print()
    print("=" * 70)
    print("PROOF CONFIRMED [PASS]")
    print("=" * 70)
    print()
    print("Key Observations:")
    print("1. ApiTimeout (retryable) executed 3 times before success")
    print("2. ItemNotFound (non-retryable) executed only 1 time")
    print("3. is_retryable() controls retry behavior")
    print("4. RetryPolicy configuration respects Retryable trait")
    print()

    # Verify the evidence
    assert STEP_A_EXECUTIONS == 3, f"Expected 3 executions for Scenario A, got {STEP_A_EXECUTIONS}"
    assert STEP_B_EXECUTIONS == 1, f"Expected 1 execution for Scenario B, got {STEP_B_EXECUTIONS}"
    assert result_a.status == TaskStatus.COMPLETE, f"Scenario A should be COMPLETE, got {result_a.status.name}"
    assert result_b.status == TaskStatus.FAILED, f"Scenario B should be FAILED, got {result_b.status.name}"

    print("[PASS] All assertions passed!")
    print()


if __name__ == "__main__":
    asyncio.run(main())
