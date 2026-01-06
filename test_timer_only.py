"""Test timer only - no child flows."""
import asyncio
import sys
from dataclasses import dataclass

print("IMPORT: Starting imports...", file=sys.stderr, flush=True)
from pyergon import Scheduler, Worker, flow, flow_type, step
from pyergon.executor.timer import schedule_timer_named
from pyergon.storage.sqlite import SqliteExecutionLog

print("IMPORT: All imports done", file=sys.stderr, flush=True)


@dataclass
@flow_type
class TimerFlow:
    """Flow with a timer."""

    name: str

    @step
    async def wait_step(self) -> None:
        """Wait for timer."""
        print("[FLOW] Scheduling timer...", flush=True)
        await schedule_timer_named(1.0, "test-timer")
        print("[FLOW] Timer complete", flush=True)

    @flow
    async def run(self) -> str:
        """Run flow."""
        print(f"[FLOW] Starting {self.name}", flush=True)
        await self.wait_step()
        print("[FLOW] After timer", flush=True)
        return "done"


async def main():
    """Run test."""
    print("[MAIN] Starting main()", flush=True)
    storage = SqliteExecutionLog("data/test_timer_only.db")
    await storage.connect()
    await storage.reset()
    print("[MAIN] Storage ready", flush=True)

    worker = Worker(storage, "worker", enable_timers=True, poll_interval=0.05)
    await worker.register(TimerFlow)
    print("[MAIN] Registered", flush=True)

    worker_handle = await worker.start()
    print("[MAIN] Worker started", flush=True)
    await asyncio.sleep(0.1)

    scheduler = Scheduler(storage)
    task_id = await scheduler.schedule(TimerFlow(name="Test"))
    print(f"[MAIN] Scheduled {task_id}", flush=True)

    # Wait for completion
    for i in range(50):
        await asyncio.sleep(0.2)
        scheduled = await storage.get_scheduled_flow(task_id)
        if scheduled and scheduled.status.value in ("COMPLETE", "FAILED"):
            print(f"[MAIN] Completed: {scheduled.status}", flush=True)
            break
    else:
        print("[MAIN] Timeout", flush=True)

    await worker_handle.shutdown()
    await storage.close()
    print("[MAIN] Done", flush=True)


if __name__ == "__main__":
    print("MAIN: Calling asyncio.run()", file=sys.stderr, flush=True)
    asyncio.run(main())
    print("MAIN: asyncio.run() completed", file=sys.stderr, flush=True)
