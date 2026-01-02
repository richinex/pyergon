"""
Validation test: Verify both Celery and Ergon compute correct results.

Expected workflow result:
  step1() -> 1
  step2(1) -> 2
  step3(2) -> 4
"""

import asyncio
from dataclasses import dataclass

# Celery imports
from benchmark_celery import celery_app, celery_step1, celery_step2, celery_step3

# Ergon imports
from ergon import flow, flow_type, step, Scheduler, Worker
from ergon.storage.redis import RedisExecutionLog
from ergon.core import TaskStatus

@dataclass
@flow_type
class ValidationFlow:
    """Test flow for validation."""
    id: str

    @step
    async def step1(self) -> int:
        return 1

    @step
    async def step2(self, value: int) -> int:
        return value + 1

    @step
    async def step3(self, value: int) -> int:
        return value * 2

    @flow
    async def run(self) -> int:
        v1 = await self.step1()
        v2 = await self.step2(v1)
        return await self.step3(v2)

def test_celery():
    """Test Celery workflow returns correct result."""
    print("Testing Celery...")

    # Execute workflow: step1 -> step2 -> step3
    r1 = celery_step1.apply_async()
    v1 = r1.get(timeout=5)
    print(f"  step1() = {v1} (expected: 1)")

    r2 = celery_step2.apply_async(args=[v1])
    v2 = r2.get(timeout=5)
    print(f"  step2({v1}) = {v2} (expected: 2)")

    r3 = celery_step3.apply_async(args=[v2])
    v3 = r3.get(timeout=5)
    print(f"  step3({v2}) = {v3} (expected: 4)")

    if v3 == 4:
        print("✓ Celery: PASSED - result is correct (4)")
        return True
    else:
        print(f"✗ Celery: FAILED - result is {v3}, expected 4")
        return False

async def test_ergon():
    """Test Ergon workflow returns correct result."""
    print("\nTesting Ergon...")

    storage = RedisExecutionLog("redis://localhost:6379/0", max_connections=10)
    await storage.connect()

    try:
        await storage.reset()

        scheduler = Scheduler(storage)
        worker = Worker(storage, "validator")
        await worker.register(ValidationFlow)
        handle = await worker.start()

        # Schedule flow
        flow_obj = ValidationFlow("validation-test")
        task_id = await scheduler.schedule(flow_obj)

        # Wait for completion
        for _ in range(100):  # 10 second timeout
            task = await storage.get_scheduled_flow(task_id)
            if task:
                if task.status == TaskStatus.COMPLETE:
                    # Get all invocations to find the final result
                    import pickle
                    for step_num in range(10):  # Check up to 10 steps
                        inv = await storage.get_invocation(task_id, step_num)
                        if inv and inv.return_value:
                            result = pickle.loads(inv.return_value)
                            print(f"  step {step_num} result = {result}")
                        if not inv:
                            # Last step was step_num - 1
                            final_inv = await storage.get_invocation(task_id, step_num - 1)
                            if final_inv and final_inv.return_value:
                                final_result = pickle.loads(final_inv.return_value)
                                print(f"  Final result = {final_result} (expected: 4)")

                                if final_result == 4:
                                    print("✓ Ergon: PASSED - result is correct (4)")
                                    await handle.shutdown()
                                    return True
                                else:
                                    print(f"✗ Ergon: FAILED - result is {final_result}, expected 4")
                                    await handle.shutdown()
                                    return False
                            break
                elif task.status == TaskStatus.FAILED:
                    print(f"✗ Ergon: FAILED - workflow failed with status {task.status}")
                    await handle.shutdown()
                    return False
            await asyncio.sleep(0.1)

        print("✗ Ergon: TIMEOUT - workflow did not complete in 10 seconds")
        await handle.shutdown()
        return False

    finally:
        await storage.close()

async def main():
    """Run validation tests."""
    print("="*60)
    print("VALIDATION TEST: Verify Correct Results")
    print("="*60)

    celery_ok = test_celery()
    ergon_ok = await test_ergon()

    print("\n" + "="*60)
    print("VALIDATION SUMMARY")
    print("="*60)

    if celery_ok and ergon_ok:
        print("✓ Both frameworks compute correct results!")
        print("✓ Both use Redis backend successfully!")
    else:
        print("✗ Some tests failed - check above for details")

    return celery_ok and ergon_ok

if __name__ == "__main__":
    result = asyncio.run(main())
    exit(0 if result else 1)
