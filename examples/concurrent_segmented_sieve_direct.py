"""
Concurrent Segmented Sieve with Ergon (Python) - Direct call (no to_thread)

Benchmark Comparison:
- Go: Uses Goroutines + Channels + Mutex
- Rust Ergon: Uses Persistent Flows + Task Queue + Scatter/Gather
- Python Ergon: Same pattern as Rust, direct synchronous calls

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
- Python (direct call): ~1.0s

**Note on Free-Threading**:
This version uses direct synchronous calls (no to_thread), so free-threaded
Python (3.13t+) won't provide parallelism benefits. For true parallelism with
free-threading, use concurrent_segmented_sieve.py which uses to_thread().

**Rust Reference**: `/home/richinex/Documents/devs/rust_projects/ergon/ergon/examples/concurrent_segmented_sieve.rs`
"""

import asyncio
import time
from dataclasses import dataclass
from typing import List

from ergon import flow, flow_type, step, Worker, Scheduler, InMemoryExecutionLog

# =============================================================================
# GLOBAL CONFIG
# =============================================================================

LARGEST_PRIME = 10_000_000
SEGMENT_SIZE = 1_000_000  # 10 chunks of 1M each

# To track completion
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
@flow_type
class PrimeSegmentFlow:
    """
    Flow that processes one segment of the prime range.

    **Rust Reference**: lines 60-134
    """
    low: int
    high: int
    base_primes: List[int]  # Passed in so each worker is independent

    @step
    async def calculate(self) -> int:
        """
        Calculate primes in this segment.

        **Rust Reference**: lines 69-127

        Direct synchronous call - no asyncio.to_thread().
        """
        global completed_segments, total_primes_found, done_event, state_lock

        # Direct call (no thread offloading)
        count = calculate_segment(self.low, self.high, self.base_primes)

        # Update Global Stats (In a real app, we might write to DB)
        async with state_lock:
            total_primes_found += count
            completed_segments += 1
            finished = completed_segments

        # Notify orchestrator when all segments complete
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
    """
    global completed_segments, total_primes_found, done_event, state_lock

    # Initialize global state
    completed_segments = 0
    total_primes_found = 0
    done_event = asyncio.Event()
    state_lock = asyncio.Lock()

    start_time = time.time()

    # 1. Setup In-Memory Storage (Fastest for Benchmarking)
    storage = InMemoryExecutionLog()

    scheduler = Scheduler(storage).unversioned()

    # 2. Pre-calculate Base Primes (Sequential Step)
    sqrt_limit = int(LARGEST_PRIME ** 0.5)
    print(f"Calculating base primes up to {sqrt_limit}...")
    base_primes = await asyncio.to_thread(simple_sieve, sqrt_limit)
    print(f"Found {len(base_primes)} base primes")

    # 3. Scatter: Schedule Flows
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

    # 4. Start Workers
    print("Starting 8 workers...")
    workers = []
    worker_handles = []

    for i in range(8):
        worker = Worker(
            storage=storage,
            worker_id=f"worker-{i}",
            poll_interval=0.001  # 1ms
        )

        await worker.register(PrimeSegmentFlow)
        handle = await worker.start()

        workers.append(worker)
        worker_handles.append(handle)

    print("Workers started, processing segments...")

    # 5. Gather: Wait for completion
    await done_event.wait()

    # 6. Cleanup
    print("All segments complete, shutting down workers...")
    for handle in worker_handles:
        await handle.shutdown()

    elapsed = time.time() - start_time

    # 7. Print results
    print("\n" + "="*60)
    print("Python Ergon Concurrent Segmented Sieve (Direct)")
    print("="*60)
    print(f"Computation time: {elapsed:.3f}s")
    print(f"Number of primes: {total_primes_found:,}")
    print(f"Segments processed: {completed_segments}")
    print("="*60)


if __name__ == "__main__":
    asyncio.run(main())
