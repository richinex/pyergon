"""
Complex DAG: Parallel Execution

A computation DAG with multiple fan-out/fan-in points executed in parallel.
Independent steps at the same dependency level run concurrently.

```text
                          ┌─── mul_2 ────────────────────────────────────────────┐
                          │    (20)                                               │
        ┌── fetch_a ──────┤                                                       │
        │    (10)         │                      ┌── cross_mul ───┐               │
        │                 └─── mul_3 ────────────┤     (750)      │               │
        │                      (30)              │                │               │
        │                                        │                ├── aggregate ──┼── final
start ──┤                                        │                │    (1650)     │  (1670)
        │                 ┌─── square ───────────┤                │               │
        │                 │    (25)              │                │               │
        └── fetch_b ──────┤                      └── cross_add ───┘               │
             (5)          │                           (775)                       │
                          └─── cube ─────────────────────────────────────────────┘
                               (125)
```

Expected: final = 1670
Critical path: fetch → mul/square → cross_mul → cross_add → aggregate → final
Expected time: ~300ms (6 levels × 50ms)

Parallelism:
- Level 0: start (1 step)
- Level 1: fetch_a, fetch_b (2 steps in parallel)
- Level 2: mul_2, mul_3, square, cube (4 steps in parallel)
- Level 3: cross_mul (1 step)
- Level 4: cross_add (1 step)
- Level 5: aggregate (1 step)
- Level 6: final_result (1 step)

Run with:
```bash
PYTHONPATH=src python examples/dag_limit_parallel.py
```
"""

import asyncio
from dataclasses import dataclass

from pyergon import Executor, dag, flow, flow_type, step
from pyergon.executor.outcome import Completed
from pyergon.storage.memory import InMemoryExecutionLog


@dataclass
@flow_type
class ComplexDagParallel:
    """Complex DAG with parallel execution of independent steps."""

    id: str

    # =========================================================================
    # Level 0: Start
    # =========================================================================

    @step
    async def start(self) -> None:
        """Start the workflow."""
        print("[L0] start")
        await asyncio.sleep(0.05)  # 50ms

    # =========================================================================
    # Level 1: Fetch (parallel - both depend on start)
    # =========================================================================

    @step(depends_on=["start"])
    async def fetch_a(self) -> int:
        """Fetch value A."""
        print("[L1] fetch_a starting")
        await asyncio.sleep(0.05)
        print("[L1] fetch_a = 10")
        return 10

    @step(depends_on=["start"])
    async def fetch_b(self) -> int:
        """Fetch value B."""
        print("[L1] fetch_b starting")
        await asyncio.sleep(0.05)
        print("[L1] fetch_b = 5")
        return 5

    # =========================================================================
    # Level 2: Compute (parallel - mul_2/mul_3 depend on fetch_a, square/cube depend on fetch_b)
    # =========================================================================

    @step(depends_on=["fetch_a"], inputs={"a": "fetch_a"})
    async def mul_2(self, a: int) -> int:
        """Multiply A by 2."""
        print("[L2] mul_2 starting")
        await asyncio.sleep(0.05)
        result = a * 2
        print(f"[L2] mul_2 = {a} × 2 = {result}")
        return result

    @step(depends_on=["fetch_a"], inputs={"a": "fetch_a"})
    async def mul_3(self, a: int) -> int:
        """Multiply A by 3."""
        print("[L2] mul_3 starting")
        await asyncio.sleep(0.05)
        result = a * 3
        print(f"[L2] mul_3 = {a} × 3 = {result}")
        return result

    @step(depends_on=["fetch_b"], inputs={"b": "fetch_b"})
    async def square(self, b: int) -> int:
        """Square B."""
        print("[L2] square starting")
        await asyncio.sleep(0.05)
        result = b * b
        print(f"[L2] square = {b}² = {result}")
        return result

    @step(depends_on=["fetch_b"], inputs={"b": "fetch_b"})
    async def cube(self, b: int) -> int:
        """Cube B."""
        print("[L2] cube starting")
        await asyncio.sleep(0.05)
        result = b * b * b
        print(f"[L2] cube = {b}³ = {result}")
        return result

    # =========================================================================
    # Level 3: Cross-branch multiplication
    # =========================================================================

    @step(depends_on=["mul_3", "square"], inputs={"m": "mul_3", "s": "square"})
    async def cross_mul(self, m: int, s: int) -> int:
        """Cross multiply."""
        print("[L3] cross_mul starting")
        await asyncio.sleep(0.05)
        result = m * s
        print(f"[L3] cross_mul = {m} × {s} = {result}")
        return result

    # =========================================================================
    # Level 4: Cross-branch addition
    # =========================================================================

    @step(depends_on=["cross_mul", "square"], inputs={"cm": "cross_mul", "s": "square"})
    async def cross_add(self, cm: int, s: int) -> int:
        """Cross add."""
        print("[L4] cross_add starting")
        await asyncio.sleep(0.05)
        result = cm + s
        print(f"[L4] cross_add = {cm} + {s} = {result}")
        return result

    # =========================================================================
    # Level 5: Aggregate
    # =========================================================================

    @step(
        depends_on=["cross_mul", "cross_add", "cube"],
        inputs={"cm": "cross_mul", "ca": "cross_add", "c": "cube"},
    )
    async def aggregate(self, cm: int, ca: int, c: int) -> int:
        """Aggregate cross operations."""
        print("[L5] aggregate starting")
        await asyncio.sleep(0.05)
        result = cm + ca + c
        print(f"[L5] aggregate = {cm} + {ca} + {c} = {result}")
        return result

    # =========================================================================
    # Level 6: Final
    # =========================================================================

    @step(depends_on=["mul_2", "aggregate"], inputs={"m2": "mul_2", "agg": "aggregate"})
    async def final_result(self, m2: int, agg: int) -> int:
        """Final result."""
        print("[L6] final starting")
        await asyncio.sleep(0.05)
        result = m2 + agg
        print(f"[L6] final = {m2} + {agg} = {result}")
        return result

    @flow
    async def run(self) -> int:
        """
        Execute DAG with parallel execution using dag() function.

        In Rust: uses `dag!` macro
        In Python: uses dag() runtime function

        Both auto-wire dependencies and execute independent steps in parallel.
        """
        return await dag(self, final_step="final_result")


async def main():
    """Run the parallel DAG example."""
    import time

    print("=" * 70)
    print("COMPLEX DAG: PARALLEL EXECUTION")
    print("=" * 70)
    print()

    storage = InMemoryExecutionLog()
    await storage.reset()

    workflow = ComplexDagParallel(id="complex_parallel")

    # Match Rust pattern: Executor.new() + execute()
    executor = Executor(flow=workflow, storage=storage, flow_id="parallel-dag-001")

    start_time = time.time()
    outcome = await executor.execute(lambda w: w.run())
    elapsed = time.time() - start_time

    print()
    print("=" * 70)
    print("RESULT")
    print("=" * 70)

    if isinstance(outcome, Completed):
        result = outcome.result
        print(f"Final result: {result}")
        print("Expected:     1670")
        print(f"Match:        {'PASS' if result == 1670 else 'FAIL'}")
        print(f"Elapsed:      {elapsed:.2f}s (expected ~0.35s for 7 levels × 50ms)")
        print()
        print("Parallelism achieved:")
        print("  Level 0: 1 step  (start)")
        print("  Level 1: 2 steps (fetch_a, fetch_b)")
        print("  Level 2: 4 steps (mul_2, mul_3, square, cube)")
        print("  Level 3: 1 step  (cross_mul)")
        print("  Level 4: 1 step  (cross_add)")
        print("  Level 5: 1 step  (aggregate)")
        print("  Level 6: 1 step  (final_result)")
        print()
        print("vs. Sequential: ~0.55s (11 steps × 50ms)")
        print(f"Speedup: {0.55 / elapsed:.1f}x")

        assert result == 1670, f"Expected 1670, got {result}"
        print()
        print("[PASS] Parallel DAG executed correctly!")
    else:
        print(f"Unexpected outcome: {outcome}")


if __name__ == "__main__":
    asyncio.run(main())
