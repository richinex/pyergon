"""
Graceful Shutdown Demonstration

Workers track background flow execution tasks and wait for them to
complete during shutdown, ensuring no work is lost.

Scenario:
- 10 long-running flows (each takes 3 seconds)
- 2 workers processing flows in parallel
- Shutdown triggered while flows are still executing
- Workers wait for all background tasks to complete before exiting

Key Features:
- Background task tracking prevents garbage collection
- Shutdown waits for in-flight flows to complete
- No work is lost during worker shutdown
- Clean resource cleanup

Run:
    PYTHONPATH=src python examples/graceful_shutdown_demo.py
"""

import asyncio
import logging
import time
from dataclasses import dataclass

from pyergon import Scheduler, Worker, flow, flow_type, step
from pyergon.core import TaskStatus
from pyergon.storage import InMemoryExecutionLog

# Configure detailed logging to show shutdown process
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


@dataclass
@flow_type
class LongRunningTask:
    """A task that simulates long-running work."""

    task_id: str
    duration: float

    @flow
    async def execute(self):
        """Main flow that performs long-running work."""
        logger.info(f"[{self.task_id}] Starting long-running task ({self.duration}s)")

        # Simulate multiple steps of long work
        await self.step_1()
        await self.step_2()
        await self.step_3()

        logger.info(f"[{self.task_id}] Task completed!")
        return {"task_id": self.task_id, "status": "completed"}

    @step
    async def step_1(self) -> str:
        """First processing step."""
        logger.info(f"[{self.task_id}] Step 1: Initializing...")
        await asyncio.sleep(self.duration / 3)
        logger.info(f"[{self.task_id}] Step 1: Complete")
        return "step1_done"

    @step
    async def step_2(self) -> str:
        """Second processing step."""
        logger.info(f"[{self.task_id}] Step 2: Processing...")
        await asyncio.sleep(self.duration / 3)
        logger.info(f"[{self.task_id}] Step 2: Complete")
        return "step2_done"

    @step
    async def step_3(self) -> str:
        """Third processing step."""
        logger.info(f"[{self.task_id}] Step 3: Finalizing...")
        await asyncio.sleep(self.duration / 3)
        logger.info(f"[{self.task_id}] Step 3: Complete")
        return "step3_done"


async def main():
    """
    Demonstrate graceful shutdown with background tasks.

    This shows that workers properly wait for all in-flight flows
    to complete before shutting down, ensuring no work is lost.
    """
    # Initialize storage
    storage = InMemoryExecutionLog()

    # Start 2 workers
    worker1 = Worker(storage, "worker-1", poll_interval=0.1)
    await worker1.register(LongRunningTask)
    handle1 = await worker1.start()

    worker2 = Worker(storage, "worker-2", poll_interval=0.1)
    await worker2.register(LongRunningTask)
    handle2 = await worker2.start()

    await asyncio.sleep(0.5)

    # Create scheduler and schedule tasks
    scheduler = Scheduler(storage).with_version("v1.0")
    num_tasks = 10
    task_duration = 3.0

    start_time = time.time()
    task_ids = []
    for i in range(1, num_tasks + 1):
        task = LongRunningTask(task_id=f"TASK-{i:03d}", duration=task_duration)
        task_id = await scheduler.schedule(task)
        task_ids.append(task_id)

    # Give workers time to pick up tasks
    await asyncio.sleep(1.0)

    # Trigger shutdown while tasks are still running
    shutdown_start = time.time()
    await handle1.shutdown()
    await handle2.shutdown()
    shutdown_duration = time.time() - shutdown_start
    total_duration = time.time() - start_time

    # Check results
    completed_count = 0
    failed_count = 0
    for task_id in task_ids:
        task = await storage.get_scheduled_flow(task_id)
        if task:
            if task.status == TaskStatus.COMPLETE:
                completed_count += 1
            elif task.status == TaskStatus.FAILED:
                failed_count += 1

    # Print results
    print(f"Shutdown duration: {shutdown_duration:.2f}s")
    print(f"Total duration: {total_duration:.2f}s")
    print(f"Tasks completed: {completed_count}/{num_tasks}")
    print(f"Tasks failed: {failed_count}/{num_tasks}")


if __name__ == "__main__":
    asyncio.run(main())
