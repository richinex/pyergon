"""
Simple Timer Example - SQLite Version

This example demonstrates using durable timers with SQLite storage.

## Pattern Shown: Durable Timers with SQLite

This variant shows how to:
- Schedule timers that survive process restarts
- Use SQLite for persistent timer storage
- Enable timer processing in workers

## Run with:
```bash
PYTHONPATH=src python examples/simple_timer_sqlite.py
```
"""

import asyncio
from dataclasses import dataclass

from pyergon import Scheduler, Worker, flow, flow_type, step
from pyergon.core import TaskStatus
from pyergon.executor.timer import schedule_timer_named
from pyergon.storage.sqlite import SqliteExecutionLog


@dataclass
@flow_type
class TimedTask:
    """A simple task that waits on a timer."""

    id: str

    @step
    async def wait_delay(self) -> None:
        """Wait for 2 seconds using a durable timer."""
        print(f"{self.id} Waiting 2 seconds...")
        await schedule_timer_named(2.0, "delay")

    @step
    async def complete_task(self) -> str:
        """Complete the task."""
        print(f"{self.id} Complete!")
        return f"Task {self.id} done"

    @flow
    async def execute(self) -> str:
        """
        Main workflow entry point.

        Steps:
        1. Wait for timer to fire
        2. Complete the task
        """
        await self.wait_delay()
        return await self.complete_task()


async def main():
    """Run the simple timer example with SQLite."""
    # Set up SQLite storage
    storage = SqliteExecutionLog("data/simple_timer.db")
    await storage.connect()
    await storage.reset()

    # Create scheduler and worker
    scheduler = Scheduler(storage).with_version("v1.0")
    worker = Worker(storage, "worker").with_timers()
    await worker.register(TimedTask)
    handle = await worker.start()

    # Schedule the task
    task = TimedTask(id="Task_001")
    task_id = await scheduler.schedule(task)

    # Wait for task to complete
    while True:
        scheduled = await storage.get_scheduled_flow(task_id)
        if scheduled and scheduled.status in (TaskStatus.COMPLETE, TaskStatus.FAILED):
            if scheduled.status == TaskStatus.FAILED:
                print(f"Task failed: {scheduled.error_message}")
            break
        await asyncio.sleep(0.1)

    print("Timer Done!")
    await handle.shutdown()
    await storage.close()


if __name__ == "__main__":
    asyncio.run(main())
