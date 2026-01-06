"""Simple test to debug flow execution."""

import asyncio
from dataclasses import dataclass

from pyergon import Scheduler, Worker, flow, flow_type
from pyergon.storage.sqlite import SqliteExecutionLog


@dataclass
@flow_type
class SimpleFlow:
    """Simplest possible flow."""

    name: str

    @flow
    async def run(self) -> str:
        """Simple flow that just returns a value."""
        print(f"[SIMPLE_FLOW] Running with name={self.name}")
        return f"Hello {self.name}"


async def main():
    """Run simple flow test."""
    print("[MAIN] Starting...")
    storage = SqliteExecutionLog("data/test_simple.db")
    await storage.connect()
    await storage.reset()
    print("[MAIN] Storage ready")

    worker = Worker(storage, "test-worker", poll_interval=0.05)
    await worker.register(SimpleFlow)
    print("[MAIN] Registered SimpleFlow")

    worker_handle = await worker.start()
    print("[MAIN] Worker started")
    await asyncio.sleep(0.1)

    scheduler = Scheduler(storage)
    print("[MAIN] Scheduling flow...")
    task_id = await scheduler.schedule(SimpleFlow(name="World"))
    print(f"[MAIN] Scheduled task {task_id}")

    # Wait for completion
    for i in range(50):
        await asyncio.sleep(0.1)
        scheduled = await storage.get_scheduled_flow(task_id)
        if scheduled:
            print(f"[MAIN] Status: {scheduled.status}")
            if scheduled.status.value in ("COMPLETE", "FAILED"):
                print(f"[MAIN] Flow completed with status {scheduled.status}")
                break
    else:
        print("[MAIN] Timeout waiting for completion")

    await worker_handle.shutdown()
    await storage.close()
    print("[MAIN] Done")


if __name__ == "__main__":
    asyncio.run(main())
