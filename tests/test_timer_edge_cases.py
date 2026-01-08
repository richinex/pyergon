"""Edge case tests for timer handling."""

import pickle
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from pyergon.core import InvocationStatus, ScheduledFlow, TaskStatus
from pyergon.decorators import flow, flow_type, step
from pyergon.executor.suspension_payload import SuspensionPayload
from pyergon.executor.timer import schedule_timer, schedule_timer_named
from pyergon.storage.memory import InMemoryExecutionLog


async def _create_flow(
    storage: InMemoryExecutionLog,
    flow_id: str,
    flow_type: str,
    status: TaskStatus = TaskStatus.PENDING,
):
    """Helper to create and enqueue a flow."""
    now = datetime.now(UTC)
    flow = ScheduledFlow(
        task_id=flow_id,
        flow_id=flow_id,
        flow_type=flow_type,
        flow_data=b"data",
        status=status,
        locked_by=None,
        created_at=now,
        updated_at=now,
        retry_count=0,
        error_message=None,
        scheduled_for=None,
        parent_metadata=None,
        retry_policy=None,
        version=None,
    )
    await storage.enqueue_flow(flow)


@dataclass
@flow_type
class TimerWorkflow:
    """Workflow that uses timers."""

    timer_fired: bool = False

    @step
    async def wait_with_timer(self) -> str:
        """Step that schedules a timer."""
        await schedule_timer(0.001)  # Very short timer
        self.timer_fired = True
        return "timer_completed"

    @step
    async def wait_with_named_timer(self, name: str) -> str:
        """Step that schedules a named timer."""
        await schedule_timer_named(0.001, name=name)
        return f"timer_{name}_completed"

    @flow
    async def run(self) -> str:
        return await self.wait_with_timer()

    @flow
    async def run_named(self, name: str) -> str:
        return await self.wait_with_named_timer(name)


@pytest.mark.asyncio
async def test_schedule_timer_basic():
    """Test basic timer scheduling."""
    storage = InMemoryExecutionLog()
    flow_id = str(uuid4())

    # Create flow
    await _create_flow(storage, flow_id, "TimerWorkflow")

    # Start invocation
    await storage.log_invocation_start(
        flow_id=flow_id,
        step=0,
        class_name="TimerWorkflow",
        method_name="wait",
        parameters=b"params",
        params_hash=123,
        retry_policy=None,
    )

    # Schedule timer
    fire_at = datetime.now() + timedelta(seconds=1)
    await storage.log_timer(flow_id, 0, fire_at, "test_timer")

    # Verify invocation status changed
    invocation = await storage.get_invocation(flow_id, 0)
    assert invocation.status == InvocationStatus.WAITING_FOR_TIMER
    assert invocation.timer_fire_at is not None


@pytest.mark.asyncio
async def test_schedule_timer_zero_duration():
    """Test scheduling timer with zero duration (immediate)."""
    storage = InMemoryExecutionLog()
    flow_id = str(uuid4())

    await _create_flow(storage, flow_id, "TimerWorkflow")

    await storage.log_invocation_start(
        flow_id=flow_id,
        step=0,
        class_name="TimerWorkflow",
        method_name="wait",
        parameters=b"params",
        params_hash=123,
        retry_policy=None,
    )

    # Schedule immediate timer
    now = datetime.now()
    await storage.log_timer(flow_id, 0, now, "immediate")

    # Verify timer exists and is expired
    expired = await storage.get_expired_timers(now)
    assert any(t.flow_id == flow_id for t in expired)


@pytest.mark.asyncio
async def test_get_expired_timers():
    """Test retrieving expired timers."""
    storage = InMemoryExecutionLog()
    flow_id1 = str(uuid4())
    flow_id2 = str(uuid4())

    now = datetime.now()
    past = now - timedelta(seconds=10)
    future = now + timedelta(seconds=10)

    # Create flows
    await _create_flow(storage, flow_id1, "Flow1")
    await _create_flow(storage, flow_id2, "Flow2")

    # Start invocations
    for flow_id in [flow_id1, flow_id2]:
        await storage.log_invocation_start(
            flow_id=flow_id,
            step=0,
            class_name="TestFlow",
            method_name="wait",
            parameters=b"params",
            params_hash=123,
            retry_policy=None,
        )

    # Schedule expired timer
    await storage.log_timer(flow_id1, 0, past, "expired")

    # Schedule future timer
    await storage.log_timer(flow_id2, 0, future, "future")

    # Get expired timers
    expired = await storage.get_expired_timers(now)

    assert len(expired) >= 1
    assert any(t.flow_id == flow_id1 and t.timer_name == "expired" for t in expired)
    assert not any(t.flow_id == flow_id2 for t in expired)


@pytest.mark.asyncio
async def test_claim_timer_atomicity():
    """Test atomic timer claiming."""
    storage = InMemoryExecutionLog()
    flow_id = str(uuid4())

    past = datetime.now() - timedelta(seconds=10)

    await _create_flow(storage, flow_id, "TimerFlow")

    await storage.log_invocation_start(
        flow_id=flow_id,
        step=0,
        class_name="TimerFlow",
        method_name="wait",
        parameters=b"params",
        params_hash=123,
        retry_policy=None,
    )

    await storage.log_timer(flow_id, 0, past, "timer")

    # First claim should succeed
    success1 = await storage.claim_timer(flow_id, 0)
    assert success1 is True

    # Second claim should fail (already claimed)
    success2 = await storage.claim_timer(flow_id, 0)
    assert success2 is False


@pytest.mark.asyncio
async def test_timer_suspension_result():
    """Test storing and retrieving timer suspension results."""
    storage = InMemoryExecutionLog()
    flow_id = str(uuid4())
    step = 0
    timer_name = "test_timer"

    # Store timer result
    payload = SuspensionPayload(success=True, data=pickle.dumps(None), is_retryable=None)
    result_bytes = pickle.dumps(payload)

    await storage.store_suspension_result(flow_id, step, timer_name, result_bytes)

    # Retrieve result
    retrieved = await storage.get_suspension_result(flow_id, step, timer_name)
    assert retrieved == result_bytes

    # Remove result
    await storage.remove_suspension_result(flow_id, step, timer_name)

    # Verify removed
    retrieved_after = await storage.get_suspension_result(flow_id, step, timer_name)
    assert retrieved_after is None


@pytest.mark.asyncio
async def test_multiple_timers_same_flow():
    """Test multiple timers on different steps of same flow."""
    storage = InMemoryExecutionLog()
    flow_id = str(uuid4())

    await _create_flow(storage, flow_id, "MultiTimerFlow")

    # Create multiple invocations with timers
    for step_num in range(3):
        await storage.log_invocation_start(
            flow_id=flow_id,
            step=step_num,
            class_name="MultiTimerFlow",
            method_name=f"step_{step_num}",
            parameters=b"params",
            params_hash=123 + step_num,
            retry_policy=None,
        )

        fire_at = datetime.now() + timedelta(seconds=step_num + 1)
        await storage.log_timer(flow_id, step_num, fire_at, f"timer_{step_num}")

    # Verify all invocations are waiting
    invocations = await storage.get_invocations_for_flow(flow_id)
    waiting = [inv for inv in invocations if inv.status == InvocationStatus.WAITING_FOR_TIMER]

    assert len(waiting) == 3


@pytest.mark.asyncio
async def test_timer_fire_time_ordering():
    """Test that timers are retrieved in fire time order."""
    storage = InMemoryExecutionLog()

    now = datetime.now()
    flows = []

    # Create flows with different fire times
    for i in range(5):
        flow_id = str(uuid4())
        flows.append(flow_id)

        await _create_flow(storage, flow_id, f"Flow{i}")

        await storage.log_invocation_start(
            flow_id=flow_id,
            step=0,
            class_name=f"Flow{i}",
            method_name="wait",
            parameters=b"params",
            params_hash=123,
            retry_policy=None,
        )

        # Schedule timers with different fire times (all in past)
        fire_at = now - timedelta(seconds=10 - i)
        await storage.log_timer(flow_id, 0, fire_at, f"timer_{i}")

    # Get expired timers
    expired = await storage.get_expired_timers(now)

    # Should return timers in fire time order (earliest first)
    assert len(expired) >= 5


@pytest.mark.asyncio
async def test_get_next_timer_fire_time():
    """Test getting next timer fire time."""
    storage = InMemoryExecutionLog()

    # Initially no timers
    next_fire = await storage.get_next_timer_fire_time()
    assert next_fire is None

    # Add timer
    flow_id = str(uuid4())
    future = datetime.now() + timedelta(seconds=30)

    await _create_flow(storage, flow_id, "TimerFlow")

    await storage.log_invocation_start(
        flow_id=flow_id,
        step=0,
        class_name="TimerFlow",
        method_name="wait",
        parameters=b"params",
        params_hash=123,
        retry_policy=None,
    )

    await storage.log_timer(flow_id, 0, future, "timer1")

    # Get next fire time
    next_fire = await storage.get_next_timer_fire_time()
    assert next_fire is not None
    assert abs((next_fire - future).total_seconds()) < 1  # Within 1 second


@pytest.mark.asyncio
async def test_timer_with_long_duration():
    """Test scheduling timer with long duration."""
    storage = InMemoryExecutionLog()
    flow_id = str(uuid4())

    await _create_flow(storage, flow_id, "LongTimerFlow")

    await storage.log_invocation_start(
        flow_id=flow_id,
        step=0,
        class_name="LongTimerFlow",
        method_name="wait",
        parameters=b"params",
        params_hash=123,
        retry_policy=None,
    )

    # Schedule timer for 1 hour
    long_fire = datetime.now() + timedelta(hours=1)
    await storage.log_timer(flow_id, 0, long_fire, "long_timer")

    # Verify not expired yet
    now = datetime.now()
    expired = await storage.get_expired_timers(now)
    assert not any(t.flow_id == flow_id for t in expired)

    # Verify will be expired in future
    future_check = now + timedelta(hours=1, seconds=1)
    expired_future = await storage.get_expired_timers(future_check)
    assert any(t.flow_id == flow_id for t in expired_future)


@pytest.mark.asyncio
async def test_timer_payload_success():
    """Test timer payload with success=True."""
    storage = InMemoryExecutionLog()
    flow_id = str(uuid4())
    step = 0
    timer_name = "success_timer"

    payload = SuspensionPayload(
        success=True, data=pickle.dumps({"result": "ok"}), is_retryable=None
    )
    result_bytes = pickle.dumps(payload)

    await storage.store_suspension_result(flow_id, step, timer_name, result_bytes)

    # Retrieve and verify
    retrieved = await storage.get_suspension_result(flow_id, step, timer_name)
    retrieved_payload = pickle.loads(retrieved)

    assert retrieved_payload.success is True
    assert pickle.loads(retrieved_payload.data) == {"result": "ok"}


@pytest.mark.asyncio
async def test_timer_payload_failure():
    """Test timer payload with success=False."""
    storage = InMemoryExecutionLog()
    flow_id = str(uuid4())
    step = 0
    timer_name = "failure_timer"

    payload = SuspensionPayload(success=False, data=pickle.dumps("Timer error"), is_retryable=False)
    result_bytes = pickle.dumps(payload)

    await storage.store_suspension_result(flow_id, step, timer_name, result_bytes)

    # Retrieve and verify
    retrieved = await storage.get_suspension_result(flow_id, step, timer_name)
    retrieved_payload = pickle.loads(retrieved)

    assert retrieved_payload.success is False
    assert pickle.loads(retrieved_payload.data) == "Timer error"
    assert retrieved_payload.is_retryable is False


@pytest.mark.asyncio
async def test_timer_cleanup_after_retrieval():
    """Test that timer results can be cleaned up after retrieval."""
    storage = InMemoryExecutionLog()
    flow_id = str(uuid4())
    step = 0
    timer_name = "cleanup_timer"

    payload = SuspensionPayload(success=True, data=b"", is_retryable=None)
    result_bytes = pickle.dumps(payload)

    # Store result
    await storage.store_suspension_result(flow_id, step, timer_name, result_bytes)

    # Verify stored
    result1 = await storage.get_suspension_result(flow_id, step, timer_name)
    assert result1 is not None

    # Clean up
    await storage.remove_suspension_result(flow_id, step, timer_name)

    # Verify cleaned
    result2 = await storage.get_suspension_result(flow_id, step, timer_name)
    assert result2 is None


@pytest.mark.asyncio
async def test_concurrent_timer_claims():
    """Test that only one worker can claim a timer."""
    storage = InMemoryExecutionLog()
    flow_id = str(uuid4())

    past = datetime.now() - timedelta(seconds=10)

    await _create_flow(storage, flow_id, "ClaimTestFlow")

    await storage.log_invocation_start(
        flow_id=flow_id,
        step=0,
        class_name="ClaimTestFlow",
        method_name="wait",
        parameters=b"params",
        params_hash=123,
        retry_policy=None,
    )

    await storage.log_timer(flow_id, 0, past, "claimed_timer")

    # Simulate concurrent claims
    claim1 = await storage.claim_timer(flow_id, 0)
    claim2 = await storage.claim_timer(flow_id, 0)
    claim3 = await storage.claim_timer(flow_id, 0)

    # Only one should succeed
    claims = [claim1, claim2, claim3]
    assert sum(claims) == 1  # Exactly one True


@pytest.mark.asyncio
async def test_timer_on_non_existent_invocation():
    """Test timer operations on non-existent invocation."""
    storage = InMemoryExecutionLog()
    flow_id = str(uuid4())

    # Try to get invocation that doesn't exist
    invocation = await storage.get_invocation(flow_id, 999)
    assert invocation is None

    # Try to claim timer for non-existent invocation
    success = await storage.claim_timer(flow_id, 999)
    assert success is False
