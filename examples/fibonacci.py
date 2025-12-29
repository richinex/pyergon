"""
Durable Fibonacci - demonstrates step-based checkpointing

**Rust Reference**: ergon_rust/ergon/examples/fibonacci.rs

This example demonstrates:
- Steps as "checkpoints" that save state to database
- Sequential computation broken into chunks
- State passing between steps via return values
- Durable execution: each step's result is cached

Flow:
1. Calculate F(0..25)  -> Save State (F24, F25) to DB
2. Calculate F(26..50) -> Load State -> Save State (F49, F50) to DB
3. Calculate F(51..75) -> Load State -> Save State (F74, F75) to DB
4. Calculate F(76..100) -> Final Result

Key Insight:
- Each @step saves its result to the database
- On retry/crash, steps that completed don't re-execute
- Flow resumes from last successful checkpoint

Run with:
    PYTHONPATH=src python3 examples/fibonacci.py
"""

import asyncio
from dataclasses import dataclass

from ergon import flow, step, Scheduler, Worker, InMemoryExecutionLog
from ergon.core import TaskStatus


# =============================================================================
# DOMAIN LOGIC
# =============================================================================

@dataclass
class FibCheckpoint:
    """
    The "Memoized" State saved to database between steps.

    **Rust Reference**: FibCheckpoint in fibonacci.rs lines 31-36

    Contains ONLY what is needed to resume calculation:
    - index: Current Fibonacci index (e.g., 25)
    - prev: F(n-1) value
    - curr: F(n) value
    """
    index: int
    prev: int  # F(n-1)
    curr: int  # F(n)


@flow
class FibonacciFlow:
    """
    Durable Fibonacci calculator using checkpointed steps.

    **Rust Reference**: FibonacciFlow in fibonacci.rs lines 38-140
    """

    @staticmethod
    def compute_next_chunk(start: FibCheckpoint, count: int) -> FibCheckpoint:
        """
        Pure logic helper: Computes `count` iterations from a starting point.

        **Rust Reference**: compute_next_chunk() lines 43-59
        """
        prev = start.prev
        curr = start.curr
        start_idx = start.index

        for _ in range(count):
            next_val = prev + curr
            prev = curr
            curr = next_val

        return FibCheckpoint(
            index=start_idx + count,
            prev=prev,
            curr=curr
        )

    @step
    async def chunk_1(self) -> FibCheckpoint:
        """
        Step 1: Calculate F(0..25).

        **Rust Reference**: chunk_1() lines 61-81

        Starts from F(0)=0, F(1)=1 and computes 24 more iterations.
        Result is saved to database as checkpoint.
        """
        print("   [Step 1] Starting from 0...")

        # Initial State: F(0)=0, F(1)=1. We start at index 1.
        start = FibCheckpoint(index=1, prev=0, curr=1)

        # Compute 24 more to get to index 25
        result = self.compute_next_chunk(start, 24)

        print(f"   [Step 1] Saved Checkpoint: F({result.index}) = {result.curr}")
        return result

    @step
    async def chunk_2(self, prev_state: FibCheckpoint) -> FibCheckpoint:
        """
        Step 2: Calculate F(26..50).

        **Rust Reference**: chunk_2() lines 83-95

        Receives checkpoint from chunk_1 (F25), continues calculation.
        """
        print(f"   [Step 2] Resuming from F({prev_state.index})...")

        result = self.compute_next_chunk(prev_state, 25)

        print(f"   [Step 2] Saved Checkpoint: F({result.index}) = {result.curr}")
        return result

    @step
    async def chunk_3(self, prev_state: FibCheckpoint) -> FibCheckpoint:
        """
        Step 3: Calculate F(51..75).

        **Rust Reference**: chunk_3() lines 97-109

        Receives checkpoint from chunk_2 (F50), continues calculation.
        """
        print(f"   [Step 3] Resuming from F({prev_state.index})...")

        result = self.compute_next_chunk(prev_state, 25)

        print(f"   [Step 3] Saved Checkpoint: F({result.index}) = {result.curr}")
        return result

    @step
    async def chunk_4(self, prev_state: FibCheckpoint) -> int:
        """
        Step 4: Calculate F(76..100) - final result.

        **Rust Reference**: chunk_4() lines 111-120

        Receives checkpoint from chunk_3 (F75), completes calculation.
        """
        print(f"   [Step 4] Resuming from F({prev_state.index})...")

        result = self.compute_next_chunk(prev_state, 25)

        print(f"   [Step 4] FINAL RESULT: F({result.index})")
        return result.curr

    async def run(self) -> int:
        """
        Main orchestrator - executes chunks sequentially.

        **Rust Reference**: run() lines 123-139

        Each step returns a checkpoint that feeds into the next step.
        This demonstrates:
        - Sequential execution
        - State passing via return values
        - Automatic caching (each step result saved to DB)
        """
        # 1. Run Chunk 1 -> Returns Checkpoint A
        cp_a = await self.chunk_1()

        # 2. Run Chunk 2 -> Takes Checkpoint A, Returns Checkpoint B
        cp_b = await self.chunk_2(cp_a)

        # 3. Run Chunk 3 -> Takes Checkpoint B, Returns Checkpoint C
        cp_c = await self.chunk_3(cp_b)

        # 4. Run Chunk 4 -> Takes Checkpoint C, Returns Final int
        final_val = await self.chunk_4(cp_c)

        return final_val


# =============================================================================
# MAIN
# =============================================================================

async def main():
    """
    Run Fibonacci example.

    **Rust Reference**: main() lines 146-166
    """
    # Use In-Memory for speed
    storage = InMemoryExecutionLog()
    scheduler = Scheduler(storage).unversioned()

    # Schedule the flow
    task_id = await scheduler.schedule(FibonacciFlow())

    # Start Worker
    # **Rust Reference**: worker.register(|f: Arc<FibonacciFlow>| f.run())
    worker = Worker(storage, "calc-worker", poll_interval=0.05)
    await worker.register(FibonacciFlow, lambda f: f.run())
    handle = await worker.start()

    # Wait for completion
    while True:
        scheduled = await storage.get_scheduled_flow(task_id)
        if scheduled and scheduled.status in (TaskStatus.COMPLETE, TaskStatus.FAILED):
            break
        await asyncio.sleep(0.05)

    await handle.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
