"""
Custom error types with retry control.

**Rust Reference**: ergon_rust/ergon/examples/custom_error_boxed.rs

Run with:
    PYTHONPATH=src python3 examples/custom_error_boxed.py

Demonstrates:
1. Custom exception classes with retryable/non-retryable semantics
2. Deterministic failure injection for testing
3. Error message preservation in storage
4. Retry behavior based on error type
"""

import asyncio
import logging
import threading
from dataclasses import dataclass

from pyergon import Scheduler, Worker, flow, flow_type
from pyergon.core import RetryPolicy, TaskStatus
from pyergon.storage.sqlite import SqliteExecutionLog

logging.basicConfig(level=logging.CRITICAL)


# =============================================================================
# CUSTOM ERROR TYPES
# =============================================================================


class PaymentError(Exception):
    """Base class for payment errors."""

    def is_retryable(self) -> bool:
        """Override in subclasses to control retry behavior."""
        return False


class NetworkTimeoutError(PaymentError):
    """Network timeout connecting to payment gateway."""

    def __init__(self):
        super().__init__("Network timeout connecting to payment gateway")

    def is_retryable(self) -> bool:
        return True


class GatewayDownError(PaymentError):
    """Payment gateway temporarily unavailable."""

    def __init__(self):
        super().__init__("Payment gateway temporarily unavailable")

    def is_retryable(self) -> bool:
        return True


class InsufficientFundsError(PaymentError):
    """Insufficient funds for payment."""

    def __init__(self, available: float, required: float):
        super().__init__(
            f"Insufficient funds: available ${available:.2f}, required ${required:.2f}"
        )
        self.available = available
        self.required = required

    def is_retryable(self) -> bool:
        return False


class InvalidCardError(PaymentError):
    """Invalid card number."""

    def __init__(self, card_number: str):
        super().__init__(f"Invalid card number: {card_number}")
        self.card_number = card_number

    def is_retryable(self) -> bool:
        return False


class ShippingError(Exception):
    """Base class for shipping errors."""

    def is_retryable(self) -> bool:
        """Override in subclasses to control retry behavior."""
        return False


class WarehouseTimeoutError(ShippingError):
    """Warehouse API timeout."""

    def __init__(self):
        super().__init__("Warehouse API timeout")

    def is_retryable(self) -> bool:
        return True


class OutOfStockError(ShippingError):
    """Item out of stock."""

    def __init__(self, item_id: str):
        super().__init__(f"Item out of stock: {item_id}")
        self.item_id = item_id

    def is_retryable(self) -> bool:
        return False


class InvalidAddressError(ShippingError):
    """Invalid shipping address."""

    def __init__(self, reason: str):
        super().__init__(f"Invalid shipping address: {reason}")
        self.reason = reason

    def is_retryable(self) -> bool:
        return False


# =============================================================================
# GLOBAL ATTEMPT COUNTERS
# =============================================================================


class AttemptCounter:
    """Thread-safe attempt counter for simulating transient failures."""

    def __init__(self):
        self._lock = threading.Lock()
        self._counters: dict[str, int] = {}

    def increment(self, key: str) -> int:
        """Increment and return the attempt count for a key."""
        with self._lock:
            self._counters[key] = self._counters.get(key, 0) + 1
            return self._counters[key]

    def reset(self, key: str) -> None:
        """Reset the attempt count for a key."""
        with self._lock:
            self._counters[key] = 0


PAYMENT_ATTEMPTS = AttemptCounter()
SHIPPING_ATTEMPTS = AttemptCounter()


# =============================================================================
# FLOWS
# =============================================================================


@dataclass
@flow_type
class PaymentFlow:
    """
    Payment processing flow with custom error handling.

    **Rust Reference**: PaymentFlow struct in custom_error_boxed.rs lines 69-106
    """

    order_id: str
    amount: float
    simulate_error: str

    @flow(retry_policy=RetryPolicy.STANDARD)
    async def process(self) -> str:
        """
        Process payment with deterministic failure injection.

        **Rust Reference**: process() in custom_error_boxed.rs lines 77-105
        """
        attempt = PAYMENT_ATTEMPTS.increment(self.order_id)

        # Simulate different error scenarios
        if self.simulate_error == "network_timeout":
            if attempt < 3:
                raise NetworkTimeoutError()
        elif self.simulate_error == "gateway_down":
            raise GatewayDownError()
        elif self.simulate_error == "insufficient_funds":
            raise InsufficientFundsError(available=50.00, required=self.amount)
        elif self.simulate_error == "invalid_card":
            raise InvalidCardError(card_number="**** **** **** 1234")

        # Success
        return f"PAY-{self.order_id}-{attempt}"


@dataclass
@flow_type
class ShippingFlow:
    """
    Shipping flow with custom error handling.

    **Rust Reference**: ShippingFlow struct in custom_error_boxed.rs lines 108-141
    """

    order_id: str
    item_id: str
    simulate_error: str

    @flow(retry_policy=RetryPolicy.STANDARD)
    async def process(self) -> str:
        """
        Process shipping with deterministic failure injection.

        **Rust Reference**: process() in custom_error_boxed.rs lines 116-140
        """
        attempt = SHIPPING_ATTEMPTS.increment(self.order_id)

        # Simulate different error scenarios
        if self.simulate_error == "warehouse_timeout":
            if attempt < 2:
                raise WarehouseTimeoutError()
        elif self.simulate_error == "out_of_stock":
            raise OutOfStockError(item_id=self.item_id)
        elif self.simulate_error == "invalid_address":
            raise InvalidAddressError(reason="Missing postal code")

        # Success
        return f"TRACK-{self.order_id}-{attempt}"


# =============================================================================
# MAIN EXECUTION
# =============================================================================


async def main():
    """
    Main test execution.

    **Rust Reference**: main() in custom_error_boxed.rs lines 144-236
    """
    storage = SqliteExecutionLog("data/custom_error_boxed.db")
    await storage.connect()
    await storage.reset()

    worker = Worker(storage, "worker-1").with_poll_interval(0.1)
    await worker.register(PaymentFlow)
    await worker.register(ShippingFlow)
    worker_handle = await worker.start()

    scheduler = Scheduler(storage).with_version("v1.0")
    task_ids = []

    # Schedule payment flows with different error scenarios
    PAYMENT_ATTEMPTS.reset("ORD-001")
    task_id = await scheduler.schedule(
        PaymentFlow(order_id="ORD-001", amount=99.99, simulate_error="network_timeout")
    )
    task_ids.append(("Network timeout (retryable)", task_id))

    PAYMENT_ATTEMPTS.reset("ORD-002")
    task_id = await scheduler.schedule(
        PaymentFlow(order_id="ORD-002", amount=150.00, simulate_error="insufficient_funds")
    )
    task_ids.append(("Insufficient funds (permanent)", task_id))

    # Schedule shipping flows with different error scenarios
    SHIPPING_ATTEMPTS.reset("ORD-003")
    task_id = await scheduler.schedule(
        ShippingFlow(order_id="ORD-003", item_id="ITEM-123", simulate_error="warehouse_timeout")
    )
    task_ids.append(("Warehouse timeout (retryable)", task_id))

    SHIPPING_ATTEMPTS.reset("ORD-004")
    task_id = await scheduler.schedule(
        ShippingFlow(order_id="ORD-004", item_id="ITEM-456", simulate_error="out_of_stock")
    )
    task_ids.append(("Out of stock (permanent)", task_id))

    # Wait for all tasks to complete
    status_notify = storage.status_notify()
    try:
        await asyncio.wait_for(_wait_for_completion(storage, task_ids, status_notify), timeout=15.0)
    except TimeoutError:
        print("WARNING: Timeout waiting for completions")

    # Print results
    print()
    print("RESULTS")
    print("=" * 80)
    for description, task_id in task_ids:
        task = await storage.get_scheduled_flow(task_id)
        if task:
            status_str = task.status.name
            retry_str = f"retries: {task.retry_count}"
            print(f"{description}")
            print(f"  Task ID: {task_id}")
            print(f"  Status: {status_str} ({retry_str})")
            if task.error_message:
                print(f"  Error: {task.error_message}")
            print()

    await worker_handle.shutdown()
    await storage.close()


async def _wait_for_completion(storage, task_ids, status_notify):
    """Wait for all tasks to reach terminal state."""
    while True:
        all_complete = True
        for _, task_id in task_ids:
            task = await storage.get_scheduled_flow(task_id)
            if task and task.status not in (TaskStatus.COMPLETE, TaskStatus.FAILED):
                all_complete = False
                break

        if all_complete:
            break

        await status_notify.wait()
        status_notify.clear()


if __name__ == "__main__":
    asyncio.run(main())
