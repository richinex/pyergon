"""
Concurrent Segmented Sieve with Ergon (Python) - Using asyncio.to_thread()

Benchmark Comparison:
- Go: Uses Goroutines + Channels + Mutex
- Rust Ergon: Uses Persistent Flows + Task Queue + Scatter/Gather
- Python Ergon: Same pattern as Rust, using asyncio + thread offloading

Scenario:
Find primes up to 10,000,000.
We split the work into chunks (Segments) and let 8 workers crunch them.

Mathematical Fix applied: Sieve marking now starts at max(low, p*p).

**Expected Output**:
- Base primes calculated: 446 (primes up to sqrt(10,000,000))
- Segments scheduled: 10 (chunks of 1M each)
- Primes found: 664,579 (mathematically correct)

**Performance**:
- Rust (release): ~57ms
- Python 3.12 (GIL, with to_thread): ~1.6s
- Python 3.13t+ (free-threaded, with to_thread): Should achieve true parallelism!

**Python 3.13+ Free-Threading**:
Python 3.13 introduced experimental free-threaded mode (PEP 703) which removes
the GIL. In Python 3.14 (2025), free-threading became non-experimental.

To test with free-threading:
  python3.13t -m pip install -e .
  python3.13t examples/concurrent_segmented_sieve.py

With GIL disabled, asyncio.to_thread() should achieve near-linear speedup!

**Rust Reference**: `/home/richinex/Documents/devs/rust_projects/ergon/ergon/examples/concurrent_segmented_sieve.rs`
"""

import asyncio
import time
from dataclasses import dataclass
from typing import List

from ergon import flow, step, Worker, Scheduler, InMemoryExecutionLog

# =============================================================================
# GLOBAL CONFIG
# =============================================================================

LARGEST_PRIME = 10_000_000
SEGMENT_SIZE = 1_000_000  # 10 chunks of 1M each

# To track completion (using asyncio primitives, not threading)
# These will be set in main() to avoid module-level async issues
completed_segments = 0
total_primes_found = 0
done_event = None
state_lock = None


# =============================================================================
# MATH LOGIC (Pure Python)
# =============================================================================

def simple_sieve(limit: int) -> List[int]:
    """
    Standard non-concurrent Sieve to get the "Base Primes" up to sqrt(N).
    Needed to seed the segments.

    **Rust Reference**: lines 39-54

    This runs sequentially to get all primes up to sqrt(LARGEST_PRIME).
    These base primes are then used by each segment worker to mark composites.
    """
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
    """
    Calculate primes in a segment [low, high).

    **Rust Reference**: lines 69-126

    This is the CPU-intensive computation that we'll offload to a thread
    to avoid blocking the event loop.

    Args:
        low: Start of segment (inclusive)
        high: End of segment (exclusive)
        base_primes: Primes up to sqrt(high) for sieving

    Returns:
        Number of primes found in this segment
    """
    segment = [True] * (high - low)

    # Sieve Logic adapted from Rust
    for p in base_primes:
        p_sq = p * p

        # Optimization: If p*p is beyond this segment, we can stop checking primes
        if p_sq >= high:
            break

        # Find the first multiple of p >= low
        start = (low // p) * p
        if start < low:
            start += p

        # --- LOGIC FIX (Rust lines 86-92) ---
        # We must start marking at p*p.
        # 1. Optimization: Multiples < p*p are already handled by smaller primes.
        # 2. Correctness: Ensures we never mark 'p' itself as non-prime (since p*p > p).
        if start < p_sq:
            start = p_sq

        # Mark multiples
        j = start
        while j < high:
            segment[j - low] = False
            j += p

    # Count primes in this segment
    count = 0
    for i, is_prime_flag in enumerate(segment):
        if is_prime_flag:
            # Edge case: 0 and 1 are not prime
            num = low + i
            if num > 1:
                count += 1

    return count


# =============================================================================
# ERGON FLOW: The Segment Calculator
# =============================================================================

@dataclass
class PrimeSegmentFlow:
    """
    Flow that processes one segment of the prime range.

    **Rust Reference**: lines 60-134

    Each flow instance handles one chunk (e.g., [0, 1M), [1M, 2M), etc.)
    and reports the number of primes found.
    """
    low: int
    high: int
    base_primes: List[int]  # Passed in so each worker is independent

    @step
    async def calculate(self) -> int:
        """
        Calculate primes in this segment.

        **Rust Reference**: lines 69-127

        Uses asyncio.to_thread() to offload CPU-intensive sieve computation
        to thread pool.
        """
        global completed_segments, total_primes_found, done_event, state_lock

        # Offload CPU-intensive computation to thread pool
        count = await asyncio.to_thread(
            calculate_segment,
            self.low,
            self.high,
            self.base_primes
        )

        # Update Global Stats (In a real app, we might write to DB)
        # Using asyncio.Lock for safe concurrent access
        async with state_lock:
            total_primes_found += count
            completed_segments += 1
            finished = completed_segments

        # Notify orchestrator when all segments complete
        # Calculate expected number of segments
        expected_segments = (LARGEST_PRIME + SEGMENT_SIZE - 1) // SEGMENT_SIZE  # div_ceil
        if finished >= expected_segments:
            done_event.set()

        return count

    @flow
    async def run(self) -> None:
        """
        Main flow entry point.

        **Rust Reference**: lines 129-133
        """
        await self.calculate()


# =============================================================================
# MAIN BENCHMARK
# =============================================================================

async def main():
    """
    Main benchmark orchestrator.

    **Rust Reference**: lines 140-208

    Pattern:
    1. Setup storage and scheduler
    2. Calculate base primes (sequential)
    3. Scatter: Schedule segment flows
    4. Start workers (parallel processing)
    5. Gather: Wait for completion
    6. Report results
    """
    global completed_segments, total_primes_found, done_event, state_lock

    # Check if running in free-threaded mode
    import sys
    gil_disabled = getattr(sys, '_is_gil_enabled', lambda: True)() == False

    print("="*70)
    print("Python Ergon Concurrent Segmented Sieve (with to_thread)")
    print("="*70)
    print(f"Python version: {sys.version.split()[0]}")
    print(f"GIL status: {'DISABLED (free-threaded)' if gil_disabled else 'ENABLED (standard)'}")
    if gil_disabled:
        print("  -> True parallelism available!")
    else:
        print("  -> Limited by GIL (sequential execution)")
    print()

    # Initialize global state
    completed_segments = 0
    total_primes_found = 0
    done_event = asyncio.Event()
    state_lock = asyncio.Lock()

    start_time = time.time()

    # 1. Setup In-Memory Storage (Fastest for Benchmarking)
    # Matches Rust example which uses InMemoryExecutionLog
    storage = InMemoryExecutionLog()

    scheduler = Scheduler(storage).unversioned()

    # 2. Pre-calculate Base Primes (Sequential Step)
    # We need primes up to sqrt(10,000,000) ~= 3162
    sqrt_limit = int(LARGEST_PRIME ** 0.5)
    print(f"Calculating base primes up to {sqrt_limit}...")
    base_primes = await asyncio.to_thread(simple_sieve, sqrt_limit)
    print(f"Found {len(base_primes)} base primes")

    # 3. Scatter: Schedule Flows
    # This is equivalent to `go primesBetween(...)` in the Go example
    # or `scheduler.schedule(flow)` in Rust (lines 156-172)
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

    # 4. Start Workers (Simulating `runtime.NumCPU()` cores)
    # We spin up 8 concurrent workers to consume the segments
    # Rust lines 176-187
    print("Starting 8 workers...")
    workers = []
    worker_handles = []

    for i in range(8):
        worker = Worker(
            storage=storage,
            worker_id=f"worker-{i}",
            poll_interval=0.001  # 1ms, very aggressive for benchmark
        )

        # Register flow handler
        await worker.register(PrimeSegmentFlow)

        # Start worker
        handle = await worker.start()

        workers.append(worker)
        worker_handles.append(handle)

    print("Workers started, processing segments...")

    # 5. Gather: Wait for completion
    # Rust line 190
    await done_event.wait()

    # 6. Cleanup
    # Rust lines 193-195
    print("All segments complete, shutting down workers...")
    for handle in worker_handles:
        await handle.shutdown()

    elapsed = time.time() - start_time

    # 7. Print results
    # Rust lines 200-205
    print("\n" + "="*60)
    print("Python Ergon Concurrent Segmented Sieve")
    print("="*60)
    print(f"Computation time: {elapsed:.3f}s")
    print(f"Number of primes: {total_primes_found:,}")
    print(f"Segments processed: {completed_segments}")
    print("="*60)


if __name__ == "__main__":
    asyncio.run(main())
