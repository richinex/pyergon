"""
Non-Determinism Detection Demo

Demonstrates how Ergon's hash-based parameter detection catches bugs where
external state changes between flow runs.

BUGGY VERSION: The discount check is made OUTSIDE of a step.
When apply_discount is called with Some("SAVE20") on run 1, then None on run 2,
the parameter hash differs and triggers non-determinism detection.

Run with:
```bash
PYTHONPATH=src python examples/dag_non_deterministic.py
```
"""

import asyncio
from ergon import flow, step, Executor
from ergon.executor.outcome import Completed
from ergon.storage.sqlite import SqliteExecutionLog


# Simulates external state that changes between runs
HAS_DISCOUNT_CODE = True
VALIDATE_ATTEMPTS = 0


@flow
class PaymentProcessor:
    """
    Payment processing workflow with non-deterministic bug.

    BUGGY: discount_code is passed based on external state!
    When external state changes, replay will fail.
    """

    def __init__(self, amount: float):
        self.amount = amount

    @step
    async def apply_discount(self, discount_code: str | None) -> float:
        """
        BUG: discount_code parameter comes from external state!
        If external state changes between runs, params will differ.
        """
        print(f"  [apply_discount] Called with discount_code={discount_code!r}")
        if discount_code:
            print(f"  [apply_discount] Applying discount code: {discount_code}")
            return self.amount * 0.8  # 20% off
        else:
            print("  [apply_discount] No discount applied")
            return self.amount

    @step
    async def validate_card(self) -> str:
        """Validate payment card - fails first time."""
        global VALIDATE_ATTEMPTS
        attempt = VALIDATE_ATTEMPTS
        VALIDATE_ATTEMPTS += 1

        if attempt == 0:
            print("  [validate_card] FAILED - retrying...")
            raise Exception("Card validation failed")
        else:
            print("  [validate_card] OK")
            return "validated"

    @step(
        depends_on=["apply_discount", "validate_card"],
        inputs={'amount': 'apply_discount'}
    )
    async def send_receipt(self, amount: float) -> str:
        """Send receipt with final amount."""
        print(f"  [send_receipt] Sending confirmation for ${amount:.2f}")
        return f"Payment of ${amount:.2f} complete"

    async def process_payment(self, discount_code: str | None) -> str:
        """
        Main workflow using dag() function.

        NOTE: We pass discount_code here, which comes from external state.
        This is the BUG - if external state changes, replay will detect it.
        """
        # Manually call steps since we need to pass discount_code
        amount = await self.apply_discount(discount_code)
        await self.validate_card()
        receipt = await self.send_receipt(amount)
        return receipt


async def main():
    """Run non-determinism detection demo."""
    import os

    db = "data/nondeterminism_test.db"

    # Remove old database
    try:
        os.remove(db)
    except FileNotFoundError:
        pass

    # Ensure data directory exists
    os.makedirs("data", exist_ok=True)

    storage = SqliteExecutionLog(db)
    await storage.connect()
    await storage.reset()

    # Reset state
    global HAS_DISCOUNT_CODE, VALIDATE_ATTEMPTS
    HAS_DISCOUNT_CODE = True
    VALIDATE_ATTEMPTS = 0

    flow_id = "payment-flow-001"

    print("=" * 70)
    print("NON-DETERMINISM DETECTION DEMO")
    print("=" * 70)
    print()

    # =========================================================================
    # Run 1: Customer HAS discount code
    # =========================================================================
    print("=" * 70)
    print("RUN 1: Customer has discount code")
    print("=" * 70)
    print()

    processor1 = PaymentProcessor(amount=100.0)

    executor1 = Executor(
        flow=processor1,
        storage=storage,
        flow_id=flow_id
    )

    discount1 = "SAVE20" if HAS_DISCOUNT_CODE else None
    result1 = await executor1.execute(lambda f: f.process_payment(discount1))

    if isinstance(result1, Completed):
        print(f"  Result: Success - {result1.result}\n")
    else:
        print(f"  Result: {result1}\n")

    # Show database state
    print("Database state after run 1:")
    invocations = await storage.get_invocations_for_flow(flow_id)
    for inv in invocations:
        if inv.step > 0:
            print(f"  step {inv.step}: {inv.method_name} (params_hash: 0x{inv.params_hash:016x})")
    print()

    # =========================================================================
    # Run 2: External state changed - customer NO LONGER has discount
    # =========================================================================
    print("=" * 70)
    print("RUN 2: External state changed - NO discount code")
    print("=" * 70)
    print()

    HAS_DISCOUNT_CODE = False  # External state changed!

    processor2 = PaymentProcessor(amount=100.0)

    executor2 = Executor(
        flow=processor2,
        storage=storage,
        flow_id=flow_id  # Same flow_id - will replay
    )

    print("Attempting to replay flow with different parameters...")
    print()

    discount2 = "SAVE20" if HAS_DISCOUNT_CODE else None  # Now None!
    result2 = await executor2.execute(lambda f: f.process_payment(discount2))

    if isinstance(result2, Completed):
        # Check if result is actually an exception
        if isinstance(result2.result, Exception):
            e = result2.result
            print(f"[PASS] NON-DETERMINISM DETECTED!")
            print(f"  Error: {e}")
            print()
            print("=" * 70)
            print("EXPLANATION")
            print("=" * 70)
            print()
            print("The flow failed because:")
            print("1. Run 1: discount_code = 'SAVE20' → params_hash = 0x6af83ea71c22edd6")
            print("2. Run 2: discount_code = None → params_hash = 0x3b5b435c9ad86ec4 (DIFFERENT!)")
            print()
            print("Ergon detected that apply_discount() was called with different")
            print("parameters on replay, indicating non-deterministic behavior.")
            print()
            print("FIX: Fetch discount_code inside a @step, not from external state:")
            print()
            print("  @step")
            print("  async def fetch_discount_code(self) -> str | None:")
            print("      # Fetch from database/API - deterministic!")
            print("      return get_discount_for_customer(self.customer_id)")
        elif isinstance(result2.result, str) and "Non-deterministic" in result2.result:
            # Result is error message as string
            print(f"[PASS] NON-DETERMINISM DETECTED!")
            print(f"  Error: {result2.result}")
            print()
            print("=" * 70)
            print("EXPLANATION")
            print("=" * 70)
            print()
            print("The flow failed because:")
            print("1. Run 1: discount_code = 'SAVE20' → params_hash = 0x6af83ea71c22edd6")
            print("2. Run 2: discount_code = None → params_hash = 0x3b5b435c9ad86ec4 (DIFFERENT!)")
            print()
            print("Ergon detected that apply_discount() was called with different")
            print("parameters on replay, indicating non-deterministic behavior.")
        else:
            print(f"  Result: Success - {result2.result}")
            print()
            print("ERROR: Non-determinism was NOT detected!")
            print("This should have failed with RuntimeError")
    else:
        print(f"  Result: {result2}")

    await storage.close()


if __name__ == "__main__":
    asyncio.run(main())
