"""
Benchmark comparison: Celery vs PyErgon using Redis backend.

This benchmark provides an apples-to-apples comparison between Celery and PyErgon
for distributed task execution using Redis as the backend.

Configuration:
- Both frameworks use Redis (localhost:6379, db=0 for PyErgon, db=1 for Celery)
- Same workload: 100 flows with 3 steps each
- 3 workers for multi-worker test
- JSON serialization for fair comparison

Based on Celery documentation:
- https://docs.celeryq.dev/en/stable/getting-started/backends-and-brokers/redis.html
- https://docs.celeryq.dev/en/stable/userguide/optimizing.html

Usage:
    # Start Redis (if not running)
    redis-server

    # Start Celery workers (in separate terminal)
    celery -A benchmark_celery_vs_ergon worker --loglevel=info --concurrency=3 --prefetch-multiplier=1

    # Run benchmark
    PYTHONPATH=src uv run python benchmark_celery_vs_pyergon.py
"""

import asyncio
import time
import sys
from dataclasses import dataclass

# Celery imports
from celery import Celery, group

# PyErgon imports
from pyergon import flow, flow_type, step, Executor, Scheduler, Worker
from pyergon.storage.redis import RedisExecutionLog
from pyergon.core import TaskStatus

# ============================================================================
# Celery Configuration (following best practices)
# ============================================================================

celery_app = Celery(
    'benchmark',
    broker='redis://localhost:6379/1',      # Redis DB 1 for Celery broker
    backend='redis://localhost:6379/1',     # Redis DB 1 for results
)

# Celery configuration following 2025 best practices
celery_app.conf.update(
    task_serializer='json',                 # JSON for fair comparison
    accept_content=['json'],
    result_serializer='json',
    timezone='UTC',
    enable_utc=True,
    task_track_started=True,                # Track task state
    task_acks_late=True,                    # Acknowledge after task completes
    worker_prefetch_multiplier=1,           # Fair task distribution (from docs)
    broker_connection_retry_on_startup=True,
    result_expires=3600,                    # Results expire after 1 hour
)

# ============================================================================
# Celery Tasks (matching PyErgon's step structure)
# ============================================================================

@celery_app.task(name='benchmark.celery_step1')
def celery_step1():
    """Equivalent to PyErgon's step1."""
    return 1

@celery_app.task(name='benchmark.celery_step2')
def celery_step2(value):
    """Equivalent to PyErgon's step2."""
    return value + 1

@celery_app.task(name='benchmark.celery_step3')
def celery_step3(value):
    """Equivalent to PyErgon's step3."""
    return value * 2

# ============================================================================
# PyErgon Flow Definition (matching Celery structure)
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
# Celery Benchmarks
# ============================================================================

def benchmark_celery_simple(count: int = 100) -> float:
    """
    Benchmark Celery simple task execution.

    Executes tasks synchronously (one after another) to match PyErgon's
    simple step execution benchmark.
    """
    print(f"[Celery] {count} workflows...")

    start = time.time()
    for i in range(count):
        # Execute 3 steps sequentially
        r1 = celery_step1.apply_async()
        v1 = r1.get()

        r2 = celery_step2.apply_async(args=[v1])
        v2 = r2.get()

        r3 = celery_step3.apply_async(args=[v2])
        r3.get()  # Wait for final result
    elapsed = time.time() - start

    print(f"  {elapsed:.3f}s ({elapsed*10:.2f}ms per workflow)")
    return elapsed

def benchmark_celery_concurrent(worker_count: int = 3, flow_count: int = 100) -> float:
    """
    Benchmark Celery multi-worker concurrent execution.

    Submits all task chains concurrently and waits for completion.
    Uses Celery chain() for sequential step execution within each workflow.

    From Celery docs: https://docs.celeryq.dev/en/stable/userguide/canvas.html#chains
    """
    from celery import chain

    print(f"[Celery] {worker_count} workers, {flow_count} workflows...")

    # Submit all workflows concurrently using chains
    start = time.time()
    results = []
    for i in range(flow_count):
        # Chain: step1 | step2 | step3
        workflow = chain(celery_step1.s(), celery_step2.s(), celery_step3.s())
        result = workflow.apply_async()
        results.append(result)

    # Wait for all to complete
    for result in results:
        result.get()

    elapsed = time.time() - start
    print(f"  {elapsed:.3f}s ({elapsed*10:.2f}ms per workflow)")
    return elapsed

def benchmark_celery_parallel(count: int = 50) -> float:
    """
    Benchmark Celery parallel task execution using groups.

    Executes 3 tasks in parallel for each workflow using Celery groups,
    matching PyErgon's parallel step execution.
    """
    print(f"[Celery] {count} workflows, 3 parallel tasks each...")

    start = time.time()
    for _ in range(count):
        # Create a group of 3 parallel tasks
        job = group([
            celery_step1.s(),
            celery_step1.s(),
            celery_step1.s(),
        ])
        result = job.apply_async()
        result.get()  # Wait for all tasks in group to complete

    elapsed = time.time() - start
    print(f"  {elapsed:.3f}s ({elapsed*20:.2f}ms per workflow)")
    return elapsed

# ============================================================================
# PyErgon Benchmarks
# ============================================================================

async def benchmark_ergon_simple(count: int = 100) -> float:
    """
    Benchmark PyErgon simple step execution with distributed workers.

    APPLES-TO-APPLES: Uses distributed workers like Celery (not in-process Executor).
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

    APPLES-TO-APPLES: Submits 3 independent tasks in parallel (not in-process asyncio.gather).
    Matches Celery's group() behavior.
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
# Main Benchmark Runner
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

def check_celery_workers():
    """Check if Celery workers are running."""
    try:
        # Inspect active workers
        inspect = celery_app.control.inspect()
        active_workers = inspect.active()

        if active_workers:
            worker_count = len(active_workers)
            print(f"✓ Celery workers running: {worker_count} worker(s)")
            for worker_name in active_workers.keys():
                print(f"  - {worker_name}")
            return True
        else:
            print("✗ No Celery workers detected")
            print("\n  Please start Celery workers:")
            print("    celery -A benchmark_celery_vs_ergon worker --loglevel=info --concurrency=3 --prefetch-multiplier=1")
            return False
    except Exception as e:
        print(f"✗ Cannot connect to Celery broker: {e}")
        print("\n  Please start Celery workers:")
        print("    celery -A benchmark_celery_vs_ergon worker --loglevel=info --concurrency=3 --prefetch-multiplier=1")
        return False

async def main():
    """Run all benchmarks and compare results."""
    print("="*80)
    print("Celery vs PyErgon Benchmark")
    print("="*80)

    if not check_redis_connection():
        sys.exit(1)

    celery_workers_available = check_celery_workers()

    print("\n" + "="*80)

    results = {}

    print("\n[1/3] Simple Sequential Execution")

    if celery_workers_available:
        results['celery_simple'] = benchmark_celery_simple(100)

    results['ergon_simple'] = await benchmark_ergon_simple(100)

    print("\n[2/3] Multi-Worker Concurrent")

    if celery_workers_available:
        results['celery_concurrent'] = benchmark_celery_concurrent(3, 100)

    results['ergon_concurrent'] = await benchmark_ergon_concurrent(3, 100)

    print("\n[3/3] Parallel Task Execution")

    if celery_workers_available:
        results['celery_parallel'] = benchmark_celery_parallel(50)

    results['ergon_parallel'] = await benchmark_ergon_parallel(50)

    print("\n" + "="*80)
    print("SUMMARY")
    print("="*80)

    if celery_workers_available:
        print("\n| Benchmark | Celery | Ergon | Winner |")
        print("|-----------|--------|-------|--------|")

        for bench_type in ['simple', 'concurrent', 'parallel']:
            celery_time = results.get(f'celery_{bench_type}', 0)
            ergon_time = results.get(f'ergon_{bench_type}', 0)

            if celery_time > 0 and ergon_time > 0:
                if celery_time < ergon_time:
                    winner = f"Celery ({ergon_time/celery_time:.2f}x)"
                else:
                    winner = f"PyErgon ({celery_time/ergon_time:.2f}x)"

                print(f"| {bench_type.capitalize():9} | {celery_time:6.3f}s | {ergon_time:5.3f}s | {winner} |")
    else:
        print("\nCelery workers not available - showing PyErgon-only results:")
        print(f"  Simple execution: {results.get('ergon_simple', 0):.3f}s")
        print(f"  Multi-worker:     {results.get('ergon_concurrent', 0):.3f}s")
        print(f"  Parallel:         {results.get('ergon_parallel', 0):.3f}s")

    print("\n" + "="*80)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\nBenchmark interrupted by user")
        sys.exit(0)
