"""
Test FlowOutcome state machine implementation.

This test verifies Phase 2 of the implementation (Event-Driven Architecture):
- FlowOutcome state machine (Phase 2.1)
- Instant suspension detection in Executor (Phase 2.2)
- ExecutionContext suspend reason management (Phase 2.3)

**Rust Reference**: ergon_rust/ergon/src/executor/error.rs lines 69-92
**Rust Reference**: ergon_rust/ergon/src/executor/instance.rs lines 91-136
"""

from dataclasses import dataclass
from uuid import uuid4

import pytest

from pyergon.decorators import flow, flow_type
from pyergon.executor import Executor
from pyergon.executor.outcome import (
    Completed,
    Suspended,
    SuspendReason,
    is_completed,
    is_suspended,
)
from pyergon.executor.signal import await_external_signal
from pyergon.executor.timer import schedule_timer
from pyergon.storage.memory import InMemoryExecutionLog

# =============================================================================
# Test Flows
# =============================================================================


@dataclass
@flow_type
class SimpleFlow:
    """Flow that completes successfully."""

    value: int

    @flow
    async def run(self) -> int:
        """Return value multiplied by 2."""
        return self.value * 2


@dataclass
@flow_type
class ErrorFlow:
    """Flow that raises an error."""

    # Dataclass needs at least one field
    dummy: int = 0

    @flow
    async def run(self) -> int:
        """Raise a ValueError."""
        raise ValueError("Simulated error")


@dataclass
@flow_type
class TimerFlow:
    """Flow that suspends on a timer."""

    # Dataclass needs at least one field
    dummy: int = 0

    @flow
    async def run(self) -> str:
        """Schedule a timer and suspend."""
        # Schedule timer for 1 second (duration in seconds, not milliseconds)
        await schedule_timer(1.0)
        return "Timer completed"


@dataclass
@flow_type
class SignalFlow:
    """Flow that suspends on a signal."""

    # Dataclass needs at least one field
    dummy: int = 0

    @flow
    async def run(self) -> str:
        """Await an external signal and suspend."""
        # Await signal - pass signal name as string
        result_bytes = await await_external_signal("payment_confirmed")
        # In real usage, would deserialize: result = pickle.loads(result_bytes)
        return f"Signal received: {result_bytes}"


# =============================================================================
# Test Cases
# =============================================================================


@pytest.mark.asyncio
async def test_completed_success():
    """
    Test FlowOutcome for successful completion.

    **Rust Reference**: FlowOutcome::Completed(R) where R is success value
    """
    # Setup
    storage = InMemoryExecutionLog()

    flow = SimpleFlow(value=21)
    executor = Executor(flow, storage)

    # Execute
    outcome = await executor.run(lambda f: f.run())

    # Verify outcome type
    assert isinstance(outcome, Completed), f"Expected Completed, got {type(outcome)}"
    assert is_completed(outcome), "is_completed() should return True"
    assert not is_suspended(outcome), "is_suspended() should return False"

    # Verify result
    assert outcome.result == 42, f"Expected 42, got {outcome.result}"
    assert outcome.is_success(), "Should be a successful completion"
    assert not outcome.is_failure(), "Should not be a failure"


@pytest.mark.asyncio
async def test_completed_error():
    """
    Test FlowOutcome for error completion.

    **Rust Reference**: FlowOutcome::Completed(Err(e))

    In Python, errors are returned as Completed(exception) instead of raising.
    """
    # Setup
    storage = InMemoryExecutionLog()

    flow = ErrorFlow()
    executor = Executor(flow, storage)

    # Execute
    outcome = await executor.run(lambda f: f.run())

    # Verify outcome type
    assert isinstance(outcome, Completed), f"Expected Completed, got {type(outcome)}"
    assert is_completed(outcome), "is_completed() should return True"

    # Verify result is an exception
    assert isinstance(outcome.result, ValueError), (
        f"Expected ValueError, got {type(outcome.result)}"
    )
    assert str(outcome.result) == "Simulated error"
    assert not outcome.is_success(), "Should not be successful"
    assert outcome.is_failure(), "Should be a failure"


@pytest.mark.asyncio
async def test_suspended_timer():
    """
    Test FlowOutcome for timer suspension.

    **Rust Reference**: FlowOutcome::Suspended(SuspendReason::Timer { flow_id, step })

    When a flow calls schedule_timer(), it should:
    1. Set suspend_reason in ExecutionContext
    2. Return Suspended(reason) from Executor

    **Status**: This test will pass once Phase 5 (Timer Processing in Worker) is implemented.
    """
    # Setup
    storage = InMemoryExecutionLog()

    flow = TimerFlow()
    executor = Executor(flow, storage)

    # Execute
    outcome = await executor.run(lambda f: f.run())

    # Verify outcome type
    assert isinstance(outcome, Suspended), f"Expected Suspended, got {type(outcome)}"
    assert is_suspended(outcome), "is_suspended() should return True"
    assert not is_completed(outcome), "is_completed() should return False"

    # Verify suspend reason
    reason = outcome.reason
    assert isinstance(reason, SuspendReason)
    # Compare flow_ids as strings (reason.flow_id is always a string)
    assert reason.flow_id == executor.flow_id, (
        f"Expected flow_id={executor.flow_id}, got {reason.flow_id}"
    )
    assert reason.step == 0, f"Timer should be at step 0, got {reason.step}"
    assert reason.is_timer(), "Should be a timer suspension"
    assert not reason.is_signal(), "Should not be a signal suspension"
    assert reason.signal_name is None, "Timer should have no signal_name"


@pytest.mark.asyncio
async def test_suspended_signal():
    """
    Test FlowOutcome for signal suspension.

    **Rust Reference**: FlowOutcome::Suspended(SuspendReason::Signal { flow_id, step, signal_name })

    When a flow calls await_external_signal(), it should:
    1. Set suspend_reason in ExecutionContext with signal_name
    2. Return Suspended(reason) from Executor

    **Status**: This test will pass once signal suspension is fully integrated.
    """
    # Setup
    storage = InMemoryExecutionLog()

    flow = SignalFlow()
    executor = Executor(flow, storage)

    # Execute
    outcome = await executor.run(lambda f: f.run())

    # Verify outcome type
    assert isinstance(outcome, Suspended), f"Expected Suspended, got {type(outcome)}"
    assert is_suspended(outcome), "is_suspended() should return True"

    # Verify suspend reason
    reason = outcome.reason
    assert isinstance(reason, SuspendReason)
    assert reason.is_signal(), "Should be a signal suspension"
    assert not reason.is_timer(), "Should not be a timer suspension"
    assert reason.signal_name == "payment_confirmed", (
        f"Expected signal_name='payment_confirmed', got {reason.signal_name!r}"
    )


@pytest.mark.asyncio
async def test_suspend_reason_helpers():
    """
    Test SuspendReason helper methods.

    **Python Best Practice**: Test public API thoroughly
    Reference: https://docs.pytest.org/
    """
    flow_id = uuid4()

    # Test timer reason
    timer_reason = SuspendReason(flow_id=flow_id, step=5)
    assert timer_reason.is_timer()
    assert not timer_reason.is_signal()
    assert "Timer" in str(timer_reason)

    # Test signal reason
    signal_reason = SuspendReason(flow_id=flow_id, step=10, signal_name="order_confirmed")
    assert signal_reason.is_signal()
    assert not signal_reason.is_timer()
    assert "Signal" in str(signal_reason)
    assert "order_confirmed" in str(signal_reason)


@pytest.mark.asyncio
async def test_completed_helpers():
    """
    Test Completed helper methods.
    """
    # Test success completion
    success = Completed(result="Success value")
    assert success.is_success()
    assert not success.is_failure()
    assert "success" in str(success).lower()

    # Test error completion
    error = Completed(result=RuntimeError("Test error"))
    assert error.is_failure()
    assert not error.is_success()
    assert "error" in str(error).lower() or "runtimeerror" in str(error).lower()


# =============================================================================
# Run Tests
# =============================================================================

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
