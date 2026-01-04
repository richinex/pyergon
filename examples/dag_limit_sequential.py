"""
Complex DAG: Sequential Execution

A computation DAG executed sequentially (one step at a time).
This demonstrates the difference between parallel DAG execution and sequential steps.

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
Sequential execution: 10 steps × 50ms = ~500ms

Run with:
```bash
PYTHONPATH=src python examples/dag_limit_sequential.py
```
"""

import asyncio
from dataclasses import dataclass

from pyergon import Executor, dag, flow, flow_type, step
from pyergon.executor.outcome import Completed
from pyergon.storage.memory import InMemoryExecutionLog


@dataclass
@flow_type
class ComplexDagSequential:
    """Complex DAG with sequential dependencies forcing one-at-a-time execution."""

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
    # Level 1: Fetch (sequential instead of parallel)
    # =========================================================================

    @step(depends_on=["start"])
    async def fetch_a(self) -> int:
        """Fetch value A."""
        print("[L1] fetch_a starting")
        await asyncio.sleep(0.05)
        print("[L1] fetch_a = 10")
        return 10

    @step(depends_on=["fetch_a"])
    async def fetch_b(self) -> int:
        """Fetch value B."""
        print("[L1] fetch_b starting")
        await asyncio.sleep(0.05)
        print("[L1] fetch_b = 5")
        return 5

    # =========================================================================
    # Level 2: Compute (sequential instead of parallel)
    # =========================================================================

    @step(depends_on=["fetch_a"], inputs={"a": "fetch_a"})
    async def mul_2(self, a: int) -> int:
        """Multiply A by 2."""
        print("[L2] mul_2 starting")
        await asyncio.sleep(0.05)
        result = a * 2
        print(f"[L2] mul_2 = {a} × 2 = {result}")
        return result

    @step(depends_on=["mul_2"], inputs={"a": "fetch_a"})
    async def mul_3(self, a: int) -> int:
        """Multiply A by 3."""
        print("[L2] mul_3 starting")
        await asyncio.sleep(0.05)
        result = a * 3
        print(f"[L2] mul_3 = {a} × 3 = {result}")
        return result

    @step(depends_on=["mul_3"], inputs={"b": "fetch_b"})
    async def square(self, b: int) -> int:
        """Square B."""
        print("[L2] square starting")
        await asyncio.sleep(0.05)
        result = b * b
        print(f"[L2] square = {b}² = {result}")
        return result

    @step(depends_on=["square"], inputs={"b": "fetch_b"})
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

    @step(depends_on=["cube"], inputs={"m": "mul_3", "s": "square"})
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

    @step(depends_on=["cross_mul"], inputs={"cm": "cross_mul", "s": "square"})
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

    @step(depends_on=["cross_add"], inputs={"cm": "cross_mul", "ca": "cross_add", "c": "cube"})
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

    @step(depends_on=["aggregate"], inputs={"m2": "mul_2", "agg": "aggregate"})
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
        Execute DAG sequentially using dag() function.

        In Rust: uses `dag!` macro
        In Python: uses dag() runtime function

        Both auto-wire dependencies and execute in topological order.
        """
        return await dag(self, final_step="final_result")


async def main():
    """Run the sequential DAG example."""
    import time

    print("=" * 70)
    print("COMPLEX DAG: SEQUENTIAL EXECUTION")
    print("=" * 70)
    print()

    storage = InMemoryExecutionLog()
    await storage.reset()

    workflow = ComplexDagSequential(id="complex_sequential")

    # Match Rust pattern: Executor.new() + execute()
    executor = Executor(flow=workflow, storage=storage, flow_id="seq-dag-001")

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
        print(f"Elapsed:      {elapsed:.2f}s (expected ~0.5s for 10 steps × 50ms)")

        assert result == 1670, f"Expected 1670, got {result}"
        print()
        print("[PASS] Sequential DAG executed correctly!")
    else:
        print(f"Unexpected outcome: {outcome}")


if __name__ == "__main__":
    asyncio.run(main())
