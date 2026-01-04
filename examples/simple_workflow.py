import asyncio
import logging
from dataclasses import dataclass

from pyergon import Scheduler, Worker, flow, flow_type, step
from pyergon.core import TaskStatus
from pyergon.storage.sqlite import SqliteExecutionLog

logging.basicConfig(level=logging.CRITICAL)


@dataclass
@flow_type
class DataPipeline:
    id: str
    value: int

    @step
    async def validate(self) -> int:
        print(f"{self.id} Validating: {self.value}")
        return self.value

    @step
    async def transform(self, input: int) -> int:
        result = input * 2
        print(f"{self.id} Transforming: {input} -> {result}")
        return result

    @flow
    async def process(self) -> int:
        validated = await self.validate()
        return await self.transform(validated)


async def main():
    storage = SqliteExecutionLog("data/simple_workflow.db")
    await storage.connect()
    await storage.reset()

    worker = Worker(storage, "worker")
    await worker.register(DataPipeline)
    worker_handle = await worker.start()

    scheduler = Scheduler(storage).with_version("v1.0")
    task_id = await scheduler.schedule(DataPipeline(id="data_001", value=42))

    status_notify = storage.status_notify()
    while True:
        task = await storage.get_scheduled_flow(task_id)
        if task and task.status in (TaskStatus.COMPLETE, TaskStatus.FAILED):
            break
        await status_notify.wait()
        status_notify.clear()

    print("Pipeline complete!")
    await worker_handle.shutdown()
    await storage.close()


if __name__ == "__main__":
    asyncio.run(main())
