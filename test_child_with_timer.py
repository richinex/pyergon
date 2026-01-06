"""Test child flow with timer."""

import asyncio
from dataclasses import dataclass

from pyergon import Scheduler, Worker, flow, flow_type, step
from pyergon.executor.timer import schedule_timer_named
from pyergon.storage.sqlite import SqliteExecutionLog


@dataclass
@flow_type(invokable=str)
class ChildFlow:
    """Child flow with a timer."""

    value: int

    @step
    async def wait_step(self) -> None:
        """Wait for timer."""
        print("[CHILD] Scheduling timer...")
        await schedule_timer_named(1.0, "child-timer")
        print("[CHILD] Timer complete")

    @flow
    async def execute(self) -> str:
        """Execute child flow."""
        print(f"[CHILD] Starting with value={self.value}")
        await self.wait_step()
        print("[CHILD] After timer, returning result")
        return f"Result: {self.value}"


@dataclass
@flow_type
class ParentFlow:
    """Parent flow that invokes a child."""

    name: str

    @flow
    async def run(self) -> str:
        """Run parent flow."""
        print(f"[PARENT] Starting {self.name}")
        result = await self.invoke(ChildFlow(value=42)).result()
        print(f"[PARENT] Child returned: {result}")
        return f"Parent {self.name}: {result}"


async def main():
    """Run test."""
    print("[MAIN] Starting...")
    storage = SqliteExecutionLog("data/test_child_timer.db")
    await storage.connect()
    await storage.reset()

    worker = Worker(storage, "test-worker", enable_timers=True, poll_interval=0.05)
    await worker.register(ParentFlow)
    await worker.register(ChildFlow)

    worker_handle = await worker.start()
    await asyncio.sleep(0.1)

    scheduler = Scheduler(storage)
    task_id = await scheduler.schedule(ParentFlow(name="Test1"))
    print(f"[MAIN] Scheduled task {task_id}")

    # Wait for completion
    for i in range(50):
        await asyncio.sleep(0.2)
        scheduled = await storage.get_scheduled_flow(task_id)
        if scheduled:
            if i % 5 == 0:  # Print every second
                print(f"[MAIN] {i * 0.2:.1f}s: Status={scheduled.status}")
            if scheduled.status.value in ("COMPLETE", "FAILED"):
                print(f"[MAIN] Flow completed with status {scheduled.status}")
                if scheduled.error_message:
                    print(f"[MAIN] Error: {scheduled.error_message}")
                break
    else:
        print("[MAIN] Timeout after 10 seconds")

    await worker_handle.shutdown()
    await storage.close()


if __name__ == "__main__":
    asyncio.run(main())
