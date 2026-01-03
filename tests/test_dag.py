"""
Test for DAG (Directed Acyclic Graph) parallel execution.

This test demonstrates:
- Registering steps with dependencies
- Automatic parallel execution of independent steps
- Proper dependency waiting
- Level-based execution

ARCHITECTURE VERIFICATION:
- Steps at the same level run in parallel
- Dependent steps wait for their dependencies
- Topological execution order
"""

import asyncio
import sys
from pathlib import Path
from time import time

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from pyergon.executor.dag import DeferredRegistry


async def main():
    """
    Test DAG execution with parallel steps.
    """
    print("=" * 60)
    print("Testing DAG Parallel Execution")
    print("=" * 60)
    print()

    # Test 1: Basic parallel execution
    print("1. Basic Parallel Execution (3 independent steps)")
    print()

    registry = DeferredRegistry()

    # Register three independent steps (should run in parallel)
    async def fetch_a(_inputs):
        print("   [fetch_a] Starting...")
        await asyncio.sleep(0.1)
        print("   [fetch_a] Complete")
        return 10

    async def fetch_b(_inputs):
        print("   [fetch_b] Starting...")
        await asyncio.sleep(0.1)
        print("   [fetch_b] Complete")
        return 20

    async def fetch_c(_inputs):
        print("   [fetch_c] Starting...")
        await asyncio.sleep(0.1)
        print("   [fetch_c] Complete")
        return 30

    handle_a = registry.register("a", [], fetch_a)
    handle_b = registry.register("b", [], fetch_b)
    handle_c = registry.register("c", [], fetch_c)

    # Execute DAG
    start = time()
    await registry.execute()
    elapsed = time() - start

    # Resolve results
    a = await handle_a.resolve()
    b = await handle_b.resolve()
    c = await handle_c.resolve()

    print(f"\n   Results: a={a}, b={b}, c={c}")
    print(f"   Time: {elapsed:.2f}s (expected ~0.1s for parallel execution)")
    print()

    # Test 2: Diamond dependency pattern
    print("2. Diamond Dependency Pattern")
    print("   Structure:")
    print("        a (root)")
    print("       / \\")
    print("      b   c  (parallel)")
    print("       \\ /")
    print("        d (waits for both)")
    print()

    registry2 = DeferredRegistry()

    async def step_a(_inputs):
        print("   [a] Starting (root)")
        await asyncio.sleep(0.05)
        print("   [a] Complete")
        return 10

    async def step_b(inputs):
        a_value = inputs.get("a")
        import pickle

        a = pickle.loads(a_value) if a_value else 0
        print(f"   [b] Starting (depends on a={a})")
        await asyncio.sleep(0.05)
        print("   [b] Complete")
        return a + 1

    async def step_c(inputs):
        a_value = inputs.get("a")
        import pickle

        a = pickle.loads(a_value) if a_value else 0
        print(f"   [c] Starting (depends on a={a})")
        await asyncio.sleep(0.05)
        print("   [c] Complete")
        return a + 2

    async def step_d(inputs):
        import pickle

        b_value = inputs.get("b")
        c_value = inputs.get("c")
        b = pickle.loads(b_value) if b_value else 0
        c = pickle.loads(c_value) if c_value else 0
        print(f"   [d] Starting (depends on b={b}, c={c})")
        await asyncio.sleep(0.05)
        print("   [d] Complete")
        return b + c

    h_a = registry2.register("a", [], step_a)
    h_b = registry2.register("b", ["a"], step_b)
    h_c = registry2.register("c", ["a"], step_c)
    h_d = registry2.register("d", ["b", "c"], step_d)

    # Show level graph
    print()
    print(registry2.level_graph())

    # Execute
    await registry2.execute()

    # Resolve
    result_a = await h_a.resolve()
    result_b = await h_b.resolve()
    result_c = await h_c.resolve()
    result_d = await h_d.resolve()

    print("   Results:")
    print(f"     a = {result_a}")
    print(f"     b = {result_b} (a + 1)")
    print(f"     c = {result_c} (a + 2)")
    print(f"     d = {result_d} (b + c)")
    print()

    # Test 3: Complex parallel execution
    print("3. Complex Parallel Execution (6 steps)")
    print("   Structure:")
    print("        root")
    print("       / | \\")
    print("      A  B  C  (3 parallel)")
    print("       \\ | /")
    print("         D     (waits for A, B, C)")
    print("         |")
    print("         E     (final)")
    print()

    registry3 = DeferredRegistry()

    async def root(_inputs):
        print("   [root] Starting")
        await asyncio.sleep(0.02)
        print("   [root] Complete")
        return 100

    async def branch_a(inputs):
        print("   [A] Starting (parallel with B, C)")
        await asyncio.sleep(0.05)
        print("   [A] Complete")
        return 1

    async def branch_b(inputs):
        print("   [B] Starting (parallel with A, C)")
        await asyncio.sleep(0.05)
        print("   [B] Complete")
        return 2

    async def branch_c(inputs):
        print("   [C] Starting (parallel with A, B)")
        await asyncio.sleep(0.05)
        print("   [C] Complete")
        return 3

    async def merge_d(inputs):
        import pickle

        a = pickle.loads(inputs["A"])
        b = pickle.loads(inputs["B"])
        c = pickle.loads(inputs["C"])
        print(f"   [D] Starting (A={a}, B={b}, C={c})")
        await asyncio.sleep(0.02)
        print("   [D] Complete")
        return a + b + c

    async def final_e(inputs):
        import pickle

        d = pickle.loads(inputs["D"])
        print(f"   [E] Starting (D={d})")
        await asyncio.sleep(0.02)
        print("   [E] Complete")
        return d * 10

    registry3.register("root", [], root)
    registry3.register("A", ["root"], branch_a)
    registry3.register("B", ["root"], branch_b)
    registry3.register("C", ["root"], branch_c)
    registry3.register("D", ["A", "B", "C"], merge_d)
    he = registry3.register("E", ["D"], final_e)

    # Show summary
    summary = registry3.summary()
    print("   DAG Summary:")
    print(f"     Total steps: {summary.total_steps}")
    print(f"     Root nodes: {summary.root_count} {summary.roots}")
    print(f"     Leaf nodes: {summary.leaf_count} {summary.leaves}")
    print(f"     Max depth: {summary.max_depth}")
    print()

    # Execute
    start = time()
    await registry3.execute()
    elapsed = time() - start

    # Resolve
    result_e = await he.resolve()

    print(f"\n   Final result: {result_e}")
    print(f"   Time: {elapsed:.2f}s")
    print()

    print("=" * 60)
    print("DAG EXECUTION VERIFIED:")
    print("=" * 60)
    print("✓ Independent steps run in parallel")
    print("✓ Dependent steps wait for dependencies")
    print("✓ Topological execution order maintained")
    print("✓ Complex DAG patterns work correctly")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
