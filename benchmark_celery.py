"""
Celery benchmark using Redis backend.

This benchmark measures Celery's performance for distributed task execution
using Redis as the backend.

Configuration:
- Redis (localhost:6379, db=1 for Celery)
- JSON serialization
- 3 workers for multi-worker test

Based on Celery documentation:
- https://docs.celeryq.dev/en/stable/getting-started/backends-and-brokers/redis.html
- https://docs.celeryq.dev/en/stable/userguide/optimizing.html

Usage:
    # Start Redis (if not running)
    redis-server

    # Start Celery workers (in separate terminal)
    celery -A benchmark_celery worker --loglevel=info --concurrency=3 --prefetch-multiplier=1

    # Run benchmark
    PYTHONPATH=src uv run python benchmark_celery.py
"""

import time
import sys
from celery import Celery, group

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
# Celery Tasks
# ============================================================================

@celery_app.task(name='benchmark.celery_step1')
def celery_step1():
    """Step 1: Return initial value."""
    return 1

@celery_app.task(name='benchmark.celery_step2')
def celery_step2(value):
    """Step 2: Increment value."""
    return value + 1

@celery_app.task(name='benchmark.celery_step3')
def celery_step3(value):
    """Step 3: Double value."""
    return value * 2

# ============================================================================
# Celery Benchmarks
# ============================================================================

def benchmark_celery_simple(count: int = 100) -> float:
    """
    Benchmark Celery simple task execution.

    Executes tasks synchronously (one after another).
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

    Executes 3 tasks in parallel for each workflow using Celery groups.
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
# Helper Functions
# ============================================================================

def check_redis_connection():
    """Check if Redis is running."""
    import redis as redis_client
    try:
        r = redis_client.Redis(host='localhost', port=6379, db=1)
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
            print("    celery -A benchmark_celery worker --loglevel=info --concurrency=3 --prefetch-multiplier=1")
            return False
    except Exception as e:
        print(f"✗ Cannot connect to Celery broker: {e}")
        print("\n  Please start Celery workers:")
        print("    celery -A benchmark_celery worker --loglevel=info --concurrency=3 --prefetch-multiplier=1")
        return False

# ============================================================================
# Main Benchmark Runner
# ============================================================================

def main():
    """Run all Celery benchmarks."""
    print("="*80)
    print("Celery Benchmark")
    print("="*80)

    if not check_redis_connection():
        sys.exit(1)

    if not check_celery_workers():
        sys.exit(1)

    print("\n" + "="*80)

    results = {}

    print("\n[1/3] Simple Sequential Execution")
    results['simple'] = benchmark_celery_simple(100)

    print("\n[2/3] Multi-Worker Concurrent")
    results['concurrent'] = benchmark_celery_concurrent(3, 100)

    print("\n[3/3] Parallel Task Execution")
    results['parallel'] = benchmark_celery_parallel(50)

    print("\n" + "="*80)
    print("SUMMARY")
    print("="*80)
    print(f"\nSimple execution:  {results['simple']:.3f}s")
    print(f"Multi-worker:      {results['concurrent']:.3f}s")
    print(f"Parallel:          {results['parallel']:.3f}s")
    print("\n" + "="*80)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nBenchmark interrupted by user")
        sys.exit(0)
