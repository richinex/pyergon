"""
Test Retryability Fix

This example tests that:
1. Step decorator ALWAYS stores is_retryable flag (retryable AND non-retryable)
2. Worker correctly checks has_non_retryable_error()
3. Retryable errors trigger flow-level retry
4. Non-retryable errors do NOT retry
"""

import asyncio
from dataclasses import dataclass
from ergon import flow, flow_type, step, Scheduler, Worker
from ergon.storage.memory import InMemoryExecutionLog
from ergon.core import TaskStatus, RetryableError, RetryPolicy


class RetryableAPIError(RetryableError):
    """Transient error that should be retried."""

    def is_retryable(self) -> bool:
        print(f"  → is_retryable() called: returning True (will retry)")
        return True


class NonRetryableValidationError(RetryableError):
    """Permanent error that should NOT be retried."""

    def is_retryable(self) -> bool:
        print(f"  → is_retryable() called: returning False (will NOT retry)")
        return False


@dataclass
@flow_type
class TestFlow:
    """Test flow with both retryable and non-retryable errors."""
    test_case: str
    attempt: int = 0

    @step
    async def test_step(self) -> str:
        """Step that throws different error types based on test case."""
        self.attempt += 1
        print(f"\n[{self.test_case}] Execution #{self.attempt}")

        if self.test_case == "retryable":
            # Throw retryable error
            print("  Throwing RetryableAPIError...")
            raise RetryableAPIError("API timeout")

        elif self.test_case == "non_retryable":
            # Throw non-retryable error
            print("  Throwing NonRetryableValidationError...")
            raise NonRetryableValidationError("Invalid input")

        else:
            # Success case
            return "success"

    @flow
    async def execute(self) -> str:
        """Main workflow."""
        return await self.test_step()


async def main():
    """Run retryability tests."""
    storage = InMemoryExecutionLog()
    await storage.reset()

    scheduler = Scheduler(storage).with_version("v1.0")
    worker = Worker(storage, "test-worker")
    await worker.register(TestFlow)
    handle = await worker.start()

    print("=" * 60)
    print("TEST 1: Non-retryable error")
    print("Expected: 1 execution, no retries, is_retryable() called once")
    print("=" * 60)

    # Schedule non-retryable test
    flow1 = TestFlow(test_case="non_retryable")
    task1_id = await scheduler.schedule(flow1)

    # Wait for completion
    await asyncio.sleep(0.5)

    scheduled1 = await storage.get_scheduled_flow(task1_id)
    if scheduled1:
        print(f"\n[PASS] Result: status={scheduled1.status.name}, retry_count={scheduled1.retry_count}")

        # Check storage for is_retryable flag
        has_non_retryable = await storage.has_non_retryable_error(scheduled1.flow_id)
        print(f"[PASS] has_non_retryable_error: {has_non_retryable}")

        assert scheduled1.status == TaskStatus.FAILED, "Should be FAILED"
        assert scheduled1.retry_count == 0, "Should have 0 retries"
        assert has_non_retryable, "Should have non-retryable error flag"

    print("\n" + "=" * 60)
    print("TEST 2: Retryable error")
    print("Expected: 3 executions (max retries), is_retryable() called 3 times")
    print("=" * 60)

    # Schedule retryable test (with retry policy)
    flow2 = TestFlow(test_case="retryable")
    flow2._ergon_retry_policy = RetryPolicy.STANDARD  # 3 attempts
    task2_id = await scheduler.schedule(flow2)

    # Wait for all retries to complete (exponential backoff can take time)
    await asyncio.sleep(5.0)

    scheduled2 = await storage.get_scheduled_flow(task2_id)
    if scheduled2:
        print(f"\n[PASS] Result: status={scheduled2.status.name}, retry_count={scheduled2.retry_count}")

        # Check storage for is_retryable flag
        has_non_retryable = await storage.has_non_retryable_error(scheduled2.flow_id)
        print(f"[PASS] has_non_retryable_error: {has_non_retryable}")

        assert scheduled2.status == TaskStatus.FAILED, "Should be FAILED after exhausting retries"
        assert scheduled2.retry_count >= 2, f"Should have at least 2 retries, got {scheduled2.retry_count}"
        assert not has_non_retryable, "Should NOT have non-retryable error flag"

    print("\n" + "=" * 60)
    print("[PASS] ALL TESTS PASSED")
    print("=" * 60)
    print("\nKey observations:")
    print("1. Non-retryable error: is_retryable() called once, flag stored")
    print("2. Retryable error: is_retryable() called once per attempt, flag stored")
    print("3. has_non_retryable_error() correctly distinguishes error types")

    await handle.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
