"""
PyErgon benchmark using Redis backend.

This benchmark measures PyErgon's performance for distributed task execution
using Redis as the backend.

Configuration:
- Redis (localhost:6379, db=0 for PyErgon)
- 3 workers for multi-worker test

Usage:
    # Start Redis (if not running)
    redis-server

    # Run benchmark
    PYTHONPATH=src uv run python benchmark_pyergon.py
"""

import asyncio
import time
import sys
from dataclasses import dataclass

from pyergon import flow, flow_type, step, Scheduler, Worker
from pyergon.storage.redis import RedisExecutionLog
from pyergon.core import TaskStatus

# ============================================================================
# PyErgon Flow Definitions
# ============================================================================

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
class SingleTaskFlow:
    """Single independent task for parallel execution benchmark."""
    id: str

    @step
    async def compute(self) -> int:
        await asyncio.sleep(0.001)
        return 1

    @flow
    async def run(self) -> int:
        return await self.compute()

# ============================================================================
# PyErgon Benchmarks
# ============================================================================

async def benchmark_ergon_simple(count: int = 100) -> float:
    """
    Benchmark PyErgon simple step execution with distributed workers.

    Uses distributed workers (not in-process Executor).
    """
    print(f"[PyErgon] {count} workflows...")

    storage = RedisExecutionLog("redis://localhost:6379/0", max_connections=50)
    await storage.connect()

    try:
        # Clear old data
        await storage.reset()

        scheduler = Scheduler(storage)

        # Start 1 worker for sequential execution
        worker = Worker(storage, "worker-simple")
        await worker.register(SimpleStepFlow)
        handle = await worker.start()

        start = time.time()

        # Schedule all flows
        task_ids = []
        for i in range(count):
            flow_obj = SimpleStepFlow(f"simple-{i}")
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

        elapsed = time.time() - start

        # Shutdown worker
        await handle.shutdown()

        print(f"  {elapsed:.3f}s ({elapsed*10:.2f}ms per workflow)")
        return elapsed
    finally:
        await storage.close()

async def benchmark_ergon_concurrent(worker_count: int = 3, flow_count: int = 100) -> float:
    """Benchmark PyErgon multi-worker execution with Redis backend."""
    print(f"[PyErgon] {worker_count} workers, {flow_count} workflows...")

    storage = RedisExecutionLog("redis://localhost:6379/0", max_connections=50)
    await storage.connect()

    try:
        # Clear old data
        await storage.reset()

        scheduler = Scheduler(storage)

        # Start workers
        handles = []
        for i in range(worker_count):
            worker = Worker(storage, f"worker-{i}")
            await worker.register(SimpleStepFlow)
            handle = await worker.start()
            handles.append(handle)

        start = time.time()

        # Schedule all flows
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

        elapsed = time.time() - start

        # Shutdown workers
        for handle in handles:
            await handle.shutdown()

        print(f"  {elapsed:.3f}s ({elapsed*10:.2f}ms per workflow)")
        return elapsed
    finally:
        await storage.close()

async def benchmark_ergon_parallel(count: int = 50) -> float:
    """
    Benchmark PyErgon parallel task execution with distributed workers.

    Submits 3 independent tasks in parallel (not in-process asyncio.gather).
    """
    print(f"[PyErgon] {count} workflows, 3 parallel tasks each...")

    storage = RedisExecutionLog("redis://localhost:6379/0", max_connections=50)
    await storage.connect()

    try:
        # Clear old data
        await storage.reset()

        scheduler = Scheduler(storage)

        # Start 3 workers for parallel execution
        handles = []
        for i in range(3):
            worker = Worker(storage, f"worker-parallel-{i}")
            await worker.register(SingleTaskFlow)
            handle = await worker.start()
            handles.append(handle)

        start = time.time()

        # For each workflow, submit 3 tasks in parallel
        all_task_ids = []
        for i in range(count):
            task_ids = []
            for j in range(3):
                flow_obj = SingleTaskFlow(f"parallel-{i}-{j}")
                task_id = await scheduler.schedule(flow_obj)
                task_ids.append(task_id)
            all_task_ids.extend(task_ids)

        # Wait for all tasks to complete
        while True:
            all_done = True
            for task_id in all_task_ids:
                task = await storage.get_scheduled_flow(task_id)
                if task and task.status not in (TaskStatus.COMPLETE, TaskStatus.FAILED):
                    all_done = False
                    break
            if all_done:
                break
            await asyncio.sleep(0.01)

        elapsed = time.time() - start

        # Shutdown workers
        for handle in handles:
            await handle.shutdown()

        print(f"  {elapsed:.3f}s ({elapsed*20:.2f}ms per workflow)")
        return elapsed
    finally:
        await storage.close()

# ============================================================================
# Helper Functions
# ============================================================================

def check_redis_connection():
    """Check if Redis is running."""
    import redis as redis_client
    try:
        r = redis_client.Redis(host='localhost', port=6379, db=0)
        r.ping()
        print("✓ Redis connection successful")
        return True
    except Exception as e:
        print(f"✗ Redis connection failed: {e}")
        print("\n  Please start Redis:")
        print("    redis-server")
        return False

# ============================================================================
# Main Benchmark Runner
# ============================================================================

async def main():
    """Run all PyErgon benchmarks."""
    print("="*80)
    print("PyErgon Benchmark")
    print("="*80)

    if not check_redis_connection():
        sys.exit(1)

    print("\n" + "="*80)

    results = {}

    print("\n[1/3] Simple Sequential Execution")
    results['simple'] = await benchmark_ergon_simple(100)

    print("\n[2/3] Multi-Worker Concurrent")
    results['concurrent'] = await benchmark_ergon_concurrent(3, 100)

    print("\n[3/3] Parallel Task Execution")
    results['parallel'] = await benchmark_ergon_parallel(50)

    print("\n" + "="*80)
    print("SUMMARY")
    print("="*80)
    print(f"\nSimple execution:  {results['simple']:.3f}s")
    print(f"Multi-worker:      {results['concurrent']:.3f}s")
    print(f"Parallel:          {results['parallel']:.3f}s")
    print("\n" + "="*80)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\nBenchmark interrupted by user")
        sys.exit(0)
