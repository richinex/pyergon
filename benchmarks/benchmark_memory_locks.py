"""
Benchmark comparison: Single Lock vs Sharded Locks

This benchmark compares the old single-lock InMemory implementation
against the new sharded locks implementation.
"""

import asyncio
import sys
import time
from dataclasses import dataclass

sys.path.insert(0, 'src')

from pyergon import Scheduler, Worker, flow, flow_type, step
from pyergon.storage import InMemoryExecutionLog


@dataclass
@flow_type
class BenchmarkFlow:
    """Simple flow for benchmarking."""
    id: str

    @flow
    async def run(self):
        await self.step1()
        await self.step2()
        return "done"

    @step
    async def step1(self):
        return 1

    @step
    async def step2(self):
        return 2


async def benchmark_single_worker(storage, num_tasks, label):
    """Benchmark single worker execution."""
    scheduler = Scheduler(storage).with_version("v1.0")

    worker = Worker(storage, "worker-1").with_poll_interval(0.01)
    await worker.register(BenchmarkFlow)
    handle = await worker.start()

    start = time.time()

    # Schedule tasks
    for i in range(num_tasks):
        await scheduler.schedule(BenchmarkFlow(id=f"task-{i}"))

    # Wait for completion
    await asyncio.sleep(0.5)
    while True:
        pending = sum(
            1 for inv in await storage.get_incomplete_flows()
            if not inv.is_complete
        )
        if pending == 0:
            break
        await asyncio.sleep(0.1)

    duration = time.time() - start
    await handle.shutdown()

    print(f"  {label:30s} {duration:6.3f}s ({duration*1000/num_tasks:6.2f}ms per task)")
    return duration


async def benchmark_multi_worker(storage, num_workers, num_tasks, label):
    """Benchmark multiple workers execution."""
    scheduler = Scheduler(storage).with_version("v1.0")

    # Start workers
    workers = []
    handles = []
    for i in range(num_workers):
        worker = Worker(storage, f"worker-{i}").with_poll_interval(0.01)
        await worker.register(BenchmarkFlow)
        handle = await worker.start()
        workers.append(worker)
        handles.append(handle)

    await asyncio.sleep(0.1)

    start = time.time()

    # Schedule tasks
    for i in range(num_tasks):
        await scheduler.schedule(BenchmarkFlow(id=f"task-{i}"))

    # Wait for completion
    await asyncio.sleep(0.5)
    while True:
        pending = sum(
            1 for inv in await storage.get_incomplete_flows()
            if not inv.is_complete
        )
        if pending == 0:
            break
        await asyncio.sleep(0.1)

    duration = time.time() - start

    # Shutdown workers
    for handle in handles:
        await handle.shutdown()

    print(f"  {label:30s} {duration:6.3f}s ({duration*1000/num_tasks:6.2f}ms per task)")
    return duration


async def main():
    print("=" * 80)
    print("InMemory Storage Benchmark: Single Lock vs Sharded Locks")
    print("=" * 80)

    # Test 1: Single worker - 100 tasks
    print("\n[Test 1] Single Worker - 100 tasks")
    print("-" * 80)

    storage1 = InMemoryExecutionLog(num_shards=16)
    t1_sharded = await benchmark_single_worker(storage1, 100, "Sharded (16 shards)")
    await storage1.reset()

    # Note: To test single lock, we would need to pass num_shards=1
    # But the real old implementation had a different structure
    # For now, we'll test with num_shards=1 to simulate single lock
    storage1_single = InMemoryExecutionLog(num_shards=1)
    t1_single = await benchmark_single_worker(storage1_single, 100, "Single lock (1 shard)")
    await storage1_single.reset()

    speedup1 = t1_single / t1_sharded if t1_sharded > 0 else 0
    print(f"  Speedup: {speedup1:.2f}x")

    # Test 2: Multi-worker (8 workers) - 400 tasks
    print("\n[Test 2] Multi-Worker (8 workers) - 400 tasks")
    print("-" * 80)

    storage2 = InMemoryExecutionLog(num_shards=16)
    t2_sharded = await benchmark_multi_worker(storage2, 8, 400, "Sharded (16 shards)")
    await storage2.reset()

    storage2_single = InMemoryExecutionLog(num_shards=1)
    t2_single = await benchmark_multi_worker(storage2_single, 8, 400, "Single lock (1 shard)")
    await storage2_single.reset()

    speedup2 = t2_single / t2_sharded if t2_sharded > 0 else 0
    print(f"  Speedup: {speedup2:.2f}x")

    # Test 3: High concurrency (16 workers) - 800 tasks
    print("\n[Test 3] High Concurrency (16 workers) - 800 tasks")
    print("-" * 80)

    storage3 = InMemoryExecutionLog(num_shards=16)
    t3_sharded = await benchmark_multi_worker(storage3, 16, 800, "Sharded (16 shards)")
    await storage3.reset()

    storage3_single = InMemoryExecutionLog(num_shards=1)
    t3_single = await benchmark_multi_worker(storage3_single, 16, 800, "Single lock (1 shard)")
    await storage3_single.reset()

    speedup3 = t3_single / t3_sharded if t3_sharded > 0 else 0
    print(f"  Speedup: {speedup3:.2f}x")

    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print(f"Single worker (100 tasks):        {speedup1:.2f}x")
    print(f"Multi-worker 8x (400 tasks):      {speedup2:.2f}x")
    print(f"High concurrency 16x (800 tasks): {speedup3:.2f}x")
    print(f"Average speedup:                  {(speedup1 + speedup2 + speedup3) / 3:.2f}x")
    print("\n✓ Benchmark complete!")
    print("=" * 80)


if __name__ == "__main__":
    asyncio.run(main())
