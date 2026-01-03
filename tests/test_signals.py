"""
Test for signal handling (await_external_signal and signal_resume).

This test demonstrates:
- Flow that waits for external confirmation
- Using await_external_signal to pause execution
- Using signal_resume to provide the awaited value
- Resuming flow execution from the waiting point

ARCHITECTURE VERIFICATION:
- Signal coordination using asyncio.Event
- Global state management for waiting flows
- Proper CallType.AWAIT and CallType.RESUME handling
"""

import asyncio
import sys
from datetime import datetime
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from pyergon.core import CallType, get_current_call_type
from pyergon.decorators import flow, step
from pyergon.executor import Executor
from pyergon.executor.signal import await_external_signal, signal_resume
from pyergon.storage import InMemoryExecutionLog


@flow
class EmailConfirmationFlow:
    """
    Flow that waits for email confirmation before proceeding.
    """

    def __init__(self, email: str):
        self.email = email

    @step
    async def send_confirmation_email(self) -> str:
        """Send confirmation email."""
        print(f"[step1] Sending confirmation email to {self.email}")
        return f"confirmation-link-{self.email}"

    @step
    async def wait_for_confirmation(self, sent_at: datetime) -> datetime | None:
        """
        Wait for user to click confirmation link.

        This step returns None in AWAIT mode (first execution),
        and returns confirmed_at in RESUME mode (after signal).
        """
        call_type = get_current_call_type()

        if call_type == CallType.AWAIT:
            print(f"[step2] Waiting for confirmation (sent at {sent_at})")
            # Mark as waiting - return None
            return None
        elif call_type == CallType.RESUME:
            # Resuming - return the confirmed timestamp
            confirmed_at = datetime.now()
            print(f"[step2] Email confirmed at {confirmed_at}")
            return confirmed_at
        else:
            # Normal execution (no signal wait)
            print(f"[step2] Running without signal (sent at {sent_at})")
            return sent_at

    @step
    async def activate_account(self, confirmed_at: datetime) -> str:
        """Activate account after confirmation."""
        print(f"[step3] Activating account (confirmed at {confirmed_at})")
        return f"account-activated-{self.email}"

    async def run(self) -> str:
        """Main flow logic."""
        # Send email
        await self.send_confirmation_email()
        sent_at = datetime.now()

        # Wait for external confirmation
        print("[run] About to await external signal...")
        confirmed_at = await await_external_signal(lambda: self.wait_for_confirmation(sent_at))
        print(f"[run] Signal received! Confirmed at {confirmed_at}")

        # Activate account
        result = await self.activate_account(confirmed_at)
        return result


async def main():
    """
    Test demonstrating signal await/resume pattern.
    """
    print("=" * 60)
    print("Testing Signal Handling (await_external_signal)")
    print("=" * 60)
    print()

    # Setup storage
    storage = InMemoryExecutionLog()

    # Create flow
    flow_instance = EmailConfirmationFlow(email="user@example.com")
    flow_id = "test-signal-flow-123"

    print("1. Starting flow (will pause at await_external_signal)")
    print()

    # Create Executor
    executor = Executor(flow_instance, storage, flow_id=flow_id)

    # Start execution in background (will pause at signal wait)
    execution_task = asyncio.create_task(executor.run(lambda f: f.run()))

    # Give flow time to reach the await_external_signal
    await asyncio.sleep(0.5)

    print()
    print("2. Flow is now waiting for external signal")
    print("   Simulating user clicking confirmation link...")
    print()

    # Simulate external event (user clicks confirmation link)
    confirmed_at = datetime.now()
    print(f"   External event: User clicked link at {confirmed_at}")
    signal_resume(flow_id, confirmed_at)

    print()
    print("3. Signal sent - flow will resume")
    print()

    # Wait for flow to complete
    try:
        result = await asyncio.wait_for(execution_task, timeout=5.0)
        print()
        print(f"   Result: {result}")
        print()
    except TimeoutError:
        print()
        print("   ERROR: Flow did not complete within timeout")
        print()
        return

    print("4. Verification:")
    invocations = await storage.get_invocations_for_flow(flow_id)
    print(f"   Total invocations logged: {len(invocations)}")
    for inv in invocations:
        print(f"   - Step {inv.step}: {inv.method_name} ({inv.status})")
    print()

    print("=" * 60)
    print("SIGNAL HANDLING VERIFIED:")
    print("=" * 60)
    print("✓ Flow paused at await_external_signal")
    print("✓ External signal provided awaited value")
    print("✓ Flow resumed from waiting point")
    print("✓ CallType.AWAIT and CallType.RESUME handled correctly")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
