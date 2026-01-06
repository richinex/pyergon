"""Test child flow invocation without timers."""

import asyncio
from dataclasses import dataclass

from pyergon import Scheduler, Worker, flow, flow_type
from pyergon.storage.sqlite import SqliteExecutionLog


@dataclass
@flow_type(invokable=str)
class ChildFlow:
    """Child flow that returns a simple value."""

    value: int

    @flow
    async def execute(self) -> str:
        """Execute child flow."""
        print(f"[CHILD] Executing with value={self.value}")
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

        # Invoke child
        print("[PARENT] Invoking child...")
        result = await self.invoke(ChildFlow(value=42)).result()

        print(f"[PARENT] Child returned: {result}")
        return f"Parent {self.name}: {result}"


async def main():
    """Run child flow test."""
    print("[MAIN] Starting...")
    storage = SqliteExecutionLog("data/test_child.db")
    await storage.connect()
    await storage.reset()
    print("[MAIN] Storage ready")

    worker = Worker(storage, "test-worker", poll_interval=0.05)
    await worker.register(ParentFlow)
    await worker.register(ChildFlow)
    print("[MAIN] Registered flows")

    worker_handle = await worker.start()
    print("[MAIN] Worker started")
    await asyncio.sleep(0.1)

    scheduler = Scheduler(storage)
    print("[MAIN] Scheduling parent flow...")
    task_id = await scheduler.schedule(ParentFlow(name="Test1"))
    print(f"[MAIN] Scheduled task {task_id}")

    # Wait for completion
    for i in range(100):
        await asyncio.sleep(0.1)
        scheduled = await storage.get_scheduled_flow(task_id)
        if scheduled:
            print(f"[MAIN] Iteration {i}: Status={scheduled.status}")
            if scheduled.status.value in ("COMPLETE", "FAILED"):
                print(f"[MAIN] Flow completed with status {scheduled.status}")
                if scheduled.error_message:
                    print(f"[MAIN] Error: {scheduled.error_message}")
                break
    else:
        print("[MAIN] Timeout waiting for completion")

    await worker_handle.shutdown()
    await storage.close()
    print("[MAIN] Done")


if __name__ == "__main__":
    asyncio.run(main())
