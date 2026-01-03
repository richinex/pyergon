"""
Benchmark and profiling script for Ergon framework.

Profiles key workflow scenarios:
1. Simple step execution overhead
2. Multi-worker concurrent execution
3. Parallel step execution within flows

Usage:
    PYTHONPATH=src uv run python benchmark.py
"""

import asyncio
import cProfile
import pstats
import logging
from dataclasses import dataclass
from io import StringIO

from pyergon import flow, flow_type, step, Executor, Scheduler, Worker
from pyergon.storage.memory import InMemoryExecutionLog
from pyergon.core import TaskStatus

logging.basicConfig(level=logging.CRITICAL)


@dataclass
@flow_type
class SimpleStepFlow:
    """Minimal flow to measure step execution overhead."""
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


@dataclass
@flow_type
class ParallelStepFlow:
    """Flow with parallel steps for concurrency testing."""
    id: str

    @step
    async def compute_a(self) -> int:
        await asyncio.sleep(0.001)
        return 10

    @step
    async def compute_b(self) -> int:
        await asyncio.sleep(0.001)
        return 20

    @step
    async def compute_c(self) -> int:
        await asyncio.sleep(0.001)
        return 30

    @step
    async def aggregate(self, a: int, b: int, c: int) -> int:
        return a + b + c

    @flow
    async def run(self) -> int:
        results = await asyncio.gather(
            self.compute_a(),
            self.compute_b(),
            self.compute_c()
        )
        return await self.aggregate(results[0], results[1], results[2])


async def benchmark_simple_steps(count: int = 100) -> float:
    """Benchmark simple step execution overhead."""
    storage = InMemoryExecutionLog()

    start = asyncio.get_event_loop().time()
    for i in range(count):
        flow_obj = SimpleStepFlow(f"simple-{i}")
        executor = Executor(flow_obj, storage, f"task-{i}")
        await executor.run(lambda f: f.run())
    elapsed = asyncio.get_event_loop().time() - start

    return elapsed


async def benchmark_multi_worker(worker_count: int = 3, flow_count: int = 100) -> float:
    """Benchmark multi-worker concurrent execution."""
    storage = InMemoryExecutionLog()
    scheduler = Scheduler(storage)

    handles = []
    for i in range(worker_count):
        worker = Worker(storage, f"worker-{i}")
        await worker.register(SimpleStepFlow)
        handle = await worker.start()
        handles.append(handle)

    start = asyncio.get_event_loop().time()

    task_ids = []
    for i in range(flow_count):
        flow_obj = SimpleStepFlow(f"flow-{i}")
        task_id = await scheduler.schedule(flow_obj)
        task_ids.append(task_id)

    # Wait for completion
    while True:
        all_done = True
        for task_id in task_ids:
            task = await storage.get_scheduled_flow(task_id)
            if task and task.status not in (TaskStatus.COMPLETE, TaskStatus.FAILED):
                all_done = False
                break
        if all_done:
            break
        await asyncio.sleep(0.01)

    elapsed = asyncio.get_event_loop().time() - start

    for handle in handles:
        await handle.shutdown()

    return elapsed


async def benchmark_parallel_steps(count: int = 50) -> float:
    """Benchmark parallel step execution within flows."""
    storage = InMemoryExecutionLog()

    start = asyncio.get_event_loop().time()
    for i in range(count):
        flow_obj = ParallelStepFlow(f"parallel-{i}")
        executor = Executor(flow_obj, storage, f"task-{i}")
        await executor.run(lambda f: f.run())
    elapsed = asyncio.get_event_loop().time() - start

    return elapsed


async def run_all_benchmarks():
    """Run all benchmarks with profiling."""
    print("Starting Ergon Framework Benchmarks\n")
    print("="*70)

    # Profile simple steps
    print("[1/3] Benchmarking simple step execution...")
    pr = cProfile.Profile()
    pr.enable()
    elapsed = await benchmark_simple_steps(100)
    pr.disable()
    print(f"  100 flows, 3 steps each: {elapsed:.3f}s ({elapsed*10:.2f}ms per flow)")

    s = StringIO()
    ps = pstats.Stats(pr, stream=s).sort_stats('cumulative')
    ps.print_stats(15)
    simple_profile = s.getvalue()

    # Profile multi-worker
    print("\n[2/3] Benchmarking multi-worker execution...")
    pr = cProfile.Profile()
    pr.enable()
    elapsed = await benchmark_multi_worker(3, 100)
    pr.disable()
    print(f"  3 workers, 100 flows: {elapsed:.3f}s ({elapsed*10:.2f}ms per flow)")

    s = StringIO()
    ps = pstats.Stats(pr, stream=s).sort_stats('cumulative')
    ps.print_stats(15)
    multiworker_profile = s.getvalue()

    # Profile parallel steps
    print("\n[3/3] Benchmarking parallel step execution...")
    pr = cProfile.Profile()
    pr.enable()
    elapsed = await benchmark_parallel_steps(50)
    pr.disable()
    print(f"  50 flows with parallel steps: {elapsed:.3f}s ({elapsed*20:.2f}ms per flow)")

    s = StringIO()
    ps = pstats.Stats(pr, stream=s).sort_stats('cumulative')
    ps.print_stats(15)
    parallel_profile = s.getvalue()

    # Write detailed profiling results
    print("\n" + "="*70)
    print("DETAILED PROFILING RESULTS")
    print("="*70)

    print("\n[1] Simple Step Execution Profile (Top 15 Functions)")
    print("-"*70)
    print(simple_profile)

    print("\n[2] Multi-Worker Profile (Top 15 Functions)")
    print("-"*70)
    print(multiworker_profile)

    print("\n[3] Parallel Steps Profile (Top 15 Functions)")
    print("-"*70)
    print(parallel_profile)

    print("\n" + "="*70)
    print("SUMMARY")
    print("="*70)
    print("All benchmarks completed successfully.")
    print("Review the profiling data above to identify bottlenecks.")


if __name__ == "__main__":
    asyncio.run(run_all_benchmarks())
