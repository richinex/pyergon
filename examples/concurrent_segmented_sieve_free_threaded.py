"""
Concurrent Segmented Sieve - Free-Threaded Python (No GIL) Version

This version is optimized for Python 3.13t+ free-threaded builds where the GIL
is disabled. It uses asyncio.to_thread() to achieve true parallelism.

**Performance Expectations**:
- Python 3.12 (GIL enabled):      ~1.6s (sequential, no parallelism)
- Python 3.13t+ (GIL disabled):   ~0.2-0.3s (8x parallel speedup expected!)
- Rust (compiled):                ~57ms (20x faster due to compilation)

To run with free-threading:
  python3.13t -m pip install -e .
  python3.13t examples/concurrent_segmented_sieve_free_threaded.py

**Rust Reference**: `/home/richinex/Documents/devs/rust_projects/ergon/ergon/examples/concurrent_segmented_sieve.rs`
"""

import asyncio
import time
import sys
from dataclasses import dataclass
from typing import List

from ergon import flow, step, Worker, Scheduler, InMemoryExecutionLog

# =============================================================================
# GLOBAL CONFIG
# =============================================================================

LARGEST_PRIME = 10_000_000
SEGMENT_SIZE = 1_000_000

completed_segments = 0
total_primes_found = 0
done_event = None
state_lock = None


# =============================================================================
# MATH LOGIC (Pure Python)
# =============================================================================

def simple_sieve(limit: int) -> List[int]:
    """Calculate base primes up to limit."""
    is_prime = [True] * (limit + 1)
    primes = []
    for p in range(2, limit + 1):
        if is_prime[p]:
            primes.append(p)
            i = p * p
            while i <= limit:
                is_prime[i] = False
                i += p
    return primes


def calculate_segment(low: int, high: int, base_primes: List[int]) -> int:
    """Calculate primes in segment [low, high)."""
    segment = [True] * (high - low)

    for p in base_primes:
        p_sq = p * p
        if p_sq >= high:
            break

        start = (low // p) * p
        if start < low:
            start += p
        if start < p_sq:
            start = p_sq

        j = start
        while j < high:
            segment[j - low] = False
            j += p

    count = 0
    for i, is_prime_flag in enumerate(segment):
        if is_prime_flag:
            num = low + i
            if num > 1:
                count += 1

    return count


# =============================================================================
# ERGON FLOW
# =============================================================================

@dataclass
class PrimeSegmentFlow:
    """
    Segment calculator flow.

    This version uses asyncio.to_thread() which will:
    - Python 3.12 (GIL):          No benefit (GIL serializes execution)
    - Python 3.13t+ (no GIL):     True parallelism (8x speedup!)
    """
    low: int
    high: int
    base_primes: List[int]

    @step
    async def calculate(self) -> int:
        """
        Calculate primes using to_thread for parallelism.

        With GIL disabled, multiple segments can run truly in parallel!
        """
        global completed_segments, total_primes_found, done_event, state_lock

        # Offload to thread pool - parallel if GIL disabled!
        count = await asyncio.to_thread(
            calculate_segment,
            self.low,
            self.high,
            self.base_primes
        )

        async with state_lock:
            total_primes_found += count
            completed_segments += 1
            finished = completed_segments

        expected_segments = (LARGEST_PRIME + SEGMENT_SIZE - 1) // SEGMENT_SIZE
        if finished >= expected_segments:
            done_event.set()

        return count

    @flow
    async def run(self) -> None:
        await self.calculate()


# =============================================================================
# MAIN
# =============================================================================

async def main():
    global completed_segments, total_primes_found, done_event, state_lock

    # Check GIL status
    gil_enabled = getattr(sys, '_is_gil_enabled', lambda: True)()

    print("\n" + "="*70)
    print("CONCURRENT SEGMENTED SIEVE - FREE-THREADED TEST")
    print("="*70)
    print(f"Python version: {sys.version.split()[0]}")
    print(f"GIL status:     {'ENABLED (standard)' if gil_enabled else 'DISABLED (free-threaded)'}")

    if gil_enabled:
        print("\n⚠️  WARNING: Running with GIL enabled!")
        print("   This version uses asyncio.to_thread() which has overhead.")
        print("   Expected performance: ~1.6s (slower than direct calls)")
        print("\n   To test free-threading:")
        print("   1. Install Python 3.13t+ from python.org")
        print("   2. Run: python3.13t examples/concurrent_segmented_sieve_free_threaded.py")
    else:
        print("\n✅ GIL DISABLED - True parallelism available!")
        print("   Expected performance: ~0.2-0.3s (8x speedup!)")

    print("="*70 + "\n")

    # Initialize global state
    completed_segments = 0
    total_primes_found = 0
    done_event = asyncio.Event()
    state_lock = asyncio.Lock()

    start_time = time.time()

    # Setup
    storage = InMemoryExecutionLog()
    scheduler = Scheduler(storage).unversioned()

    # Calculate base primes
    sqrt_limit = int(LARGEST_PRIME ** 0.5)
    print(f"Calculating base primes up to {sqrt_limit}...")
    base_primes = await asyncio.to_thread(simple_sieve, sqrt_limit)
    print(f"Found {len(base_primes)} base primes")

    # Schedule segments
    low = 0
    segment_count = 0
    print(f"Scheduling segments (size={SEGMENT_SIZE:,})...")

    while low < LARGEST_PRIME:
        high = min(low + SEGMENT_SIZE, LARGEST_PRIME)
        flow_instance = PrimeSegmentFlow(
            low=low,
            high=high,
            base_primes=base_primes
        )
        await scheduler.schedule(flow_instance)
        segment_count += 1
        low += SEGMENT_SIZE

    print(f"Scheduled {segment_count} segments")

    # Start workers
    print("Starting 8 workers...")
    worker_handles = []

    for i in range(8):
        worker = Worker(
            storage=storage,
            worker_id=f"worker-{i}",
            poll_interval=0.001
        )
        await worker.register(PrimeSegmentFlow)
        handle = await worker.start()
        worker_handles.append(handle)

    print("Workers started, processing segments...")

    # Wait for completion
    await done_event.wait()

    # Shutdown
    print("All segments complete, shutting down workers...")
    for handle in worker_handles:
        await handle.shutdown()

    elapsed = time.time() - start_time

    # Results
    print("\n" + "="*70)
    print("RESULTS")
    print("="*70)
    print(f"Computation time: {elapsed:.3f}s")
    print(f"Number of primes: {total_primes_found:,}")
    print(f"Segments processed: {completed_segments}")

    # Performance analysis
    print("\n" + "-"*70)
    print("PERFORMANCE ANALYSIS")
    print("-"*70)

    if gil_enabled:
        expected_time = 1.6
        if elapsed < expected_time * 0.8:
            print(f"⚠️  Unexpectedly fast ({elapsed:.3f}s vs expected ~{expected_time}s)")
        else:
            print(f"✅ As expected for GIL-enabled Python (~{expected_time}s)")
            print(f"   No parallelism benefit from 8 workers")
    else:
        expected_time = 0.25
        speedup = 1.6 / elapsed  # vs GIL-enabled baseline

        print(f"Actual time:    {elapsed:.3f}s")
        print(f"Expected time:  ~{expected_time}s (with true parallelism)")
        print(f"Speedup vs GIL: {speedup:.1f}x")

        if speedup > 5:
            print(f"✅ Excellent! True parallelism achieved!")
        elif speedup > 3:
            print(f"⚠️  Good parallelism, but below theoretical 8x")
        else:
            print(f"❌ Limited parallelism - check thread contention")

    print("="*70 + "\n")


if __name__ == "__main__":
    asyncio.run(main())
