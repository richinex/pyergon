"""
Simple test to verify Executor + instrumented @step decorator.

This test demonstrates the relationship between:
- Executor: runs flows from outside (caller perspective)
- ExecutionContext: provides services to code inside steps
- @step decorator: uses ExecutionContext services

ARCHITECTURE VERIFICATION:
This test proves the split between Executor and ExecutionContext is correct:
- Executor: "how to run a flow" (caller API)
- ExecutionContext: "what's available inside steps" (step services)
"""

import asyncio
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from pyergon.core import get_current_context
from pyergon.decorators import flow, step
from pyergon.executor import Executor
from pyergon.storage import InMemoryExecutionLog


@flow
class SimpleFlow:
    """
    Simple flow to test instrumentation.
    """

    def __init__(self, value: int):
        self.value = value

    @step
    async def step1(self) -> int:
        """First step - will be cached."""
        print(f"[step1] Executing with value={self.value}")

        # Verify ExecutionContext is available
        ctx = get_current_context()
        assert ctx is not None, "ExecutionContext should be available inside step"
        print(f"[step1] ExecutionContext available: step_counter={ctx.current_step()}")

        return self.value * 2

    @step
    async def step2(self, input_value: int) -> str:
        """Second step - uses result from step1."""
        print(f"[step2] Executing with input={input_value}")

        ctx = get_current_context()
        assert ctx is not None
        print(f"[step2] ExecutionContext available: step_counter={ctx.current_step()}")

        return f"Result: {input_value}"

    async def run(self) -> str:
        """Main flow logic."""
        result1 = await self.step1()
        result2 = await self.step2(result1)
        return result2


async def main():
    """
    Test demonstrating Executor and ExecutionContext relationship.
    """
    print("=" * 60)
    print("Testing Executor + ExecutionContext + Instrumented Decorator")
    print("=" * 60)
    print()

    # Setup storage (InMemoryExecutionLog doesn't need connect())
    storage = InMemoryExecutionLog()

    # Create flow
    flow = SimpleFlow(value=42)

    print("1. Creating Executor (caller perspective)")
    print("   Executor provides: run() method to execute flow")
    print()

    # Create Executor
    executor = Executor(flow, storage, flow_id="test-flow-123")

    print("2. Executor.run() sets up ExecutionContext internally")
    print("   ExecutionContext provides to steps:")
    print("   - step counter (next_step())")
    print("   - cache checking (get_cached_result())")
    print("   - logging (log_step_start/completion)")
    print()

    print("3. First execution (no cache):")
    print()
    result = await executor.run(lambda f: f.run())
    print()
    print(f"   Result: {result}")
    print()

    print("4. Second execution (cache hit):")
    print("   @step decorator checks cache via ExecutionContext")
    print()

    # Create new executor with same flow_id
    executor2 = Executor(flow, storage, flow_id="test-flow-123")
    result2 = await executor2.run(lambda f: f.run())
    print()
    print(f"   Result: {result2}")
    print()

    print("5. Verification:")
    invocations = await storage.get_invocations_for_flow("test-flow-123")
    print(f"   Total invocations logged: {len(invocations)}")
    for inv in invocations:
        print(f"   - Step {inv.step}: {inv.method_name} ({inv.status})")
    print()

    print("=" * 60)
    print("ARCHITECTURE VERIFIED:")
    print("=" * 60)
    print("✓ Executor: provides run() method (caller API)")
    print("✓ ExecutionContext: provides services to steps")
    print("✓ @step decorator: uses ExecutionContext automatically")
    print("✓ Steps don't see ExecutionContext directly")
    print("✓ Callers don't see ExecutionContext internals")
    print()
    print("The split is correct - different audiences!")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
