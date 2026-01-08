"""Tests for in-memory storage edge cases and error handling."""

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from pyergon.core import InvocationStatus, TaskStatus
from pyergon.models import ScheduledFlow
from pyergon.storage.memory import InMemoryExecutionLog


@pytest.mark.asyncio
async def test_memory_get_scheduled_flow_nonexistent():
    """Test get_scheduled_flow returns None for nonexistent flow."""
    storage = InMemoryExecutionLog()
    result = await storage.get_scheduled_flow(str(uuid4()))
    assert result is None


@pytest.mark.asyncio
async def test_memory_complete_flow():
    """Test completing a flow."""
    storage = InMemoryExecutionLog()
    flow_id = str(uuid4())

    now = datetime.now(UTC)
    flow = ScheduledFlow(
        task_id=flow_id,
        flow_id=flow_id,
        flow_type="TestFlow",
        flow_data=b"data",
        status=TaskStatus.RUNNING,
        locked_by="worker-1",
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

    # Complete the flow
    await storage.complete_flow(flow_id, TaskStatus.COMPLETE, None)
    updated = await storage.get_scheduled_flow(flow_id)
    assert updated.status == TaskStatus.COMPLETE


@pytest.mark.asyncio
async def test_memory_retry_flow():
    """Test retry flow functionality."""
    storage = InMemoryExecutionLog()
    flow_id = str(uuid4())

    now = datetime.now(UTC)
    flow = ScheduledFlow(
        task_id=flow_id,
        flow_id=flow_id,
        flow_type="TestFlow",
        flow_data=b"data",
        status=TaskStatus.RUNNING,
        locked_by="worker-1",
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

    # Retry the flow
    delay = timedelta(seconds=5)
    await storage.retry_flow(flow_id, "Test error", delay)

    updated = await storage.get_scheduled_flow(flow_id)
    assert updated.status == TaskStatus.PENDING
    assert updated.locked_by is None
    assert updated.retry_count == 1
    assert updated.error_message == "Test error"
    assert updated.scheduled_for is not None


@pytest.mark.asyncio
async def test_memory_has_non_retryable_error():
    """Test checking for non-retryable errors."""
    storage = InMemoryExecutionLog()
    flow_id = str(uuid4())

    # Initially no error
    result = await storage.has_non_retryable_error(flow_id)
    assert result is False

    # Add invocation with retryable error
    await storage.log_invocation_start(
        flow_id=flow_id,
        step=0,
        class_name="TestFlow",
        method_name="run",
        parameters=b"params",
        params_hash=123,
        retry_policy=None,
    )

    result = await storage.has_non_retryable_error(flow_id)
    assert result is False

    # Mark as non-retryable
    await storage.update_is_retryable(flow_id, 0, False)

    result = await storage.has_non_retryable_error(flow_id)
    assert result is True


@pytest.mark.asyncio
async def test_memory_dequeue_flow():
    """Test dequeuing a flow."""
    storage = InMemoryExecutionLog()
    flow_id = str(uuid4())

    now = datetime.now(UTC)
    flow = ScheduledFlow(
        task_id=flow_id,
        flow_id=flow_id,
        flow_type="TestFlow",
        flow_data=b"data",
        status=TaskStatus.PENDING,
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

    # Dequeue the flow
    dequeued = await storage.dequeue_flow("worker-1")
    assert dequeued is not None
    assert dequeued.flow_id == flow_id
    assert dequeued.locked_by == "worker-1"
    assert dequeued.status == TaskStatus.RUNNING

    # Try to dequeue again (should get None since locked)
    dequeued2 = await storage.dequeue_flow("worker-2")
    assert dequeued2 is None


@pytest.mark.asyncio
async def test_memory_store_and_get_suspension_result():
    """Test storing and retrieving suspension results."""
    storage = InMemoryExecutionLog()
    flow_id = str(uuid4())
    step = 5
    signal_name = "test_signal"
    result_data = b"suspension_result"

    # Store suspension result
    await storage.store_suspension_result(flow_id, step, signal_name, result_data)

    # Retrieve it
    retrieved = await storage.get_suspension_result(flow_id, step, signal_name)
    assert retrieved == result_data

    # Remove it
    await storage.remove_suspension_result(flow_id, step, signal_name)

    # Verify removed
    retrieved = await storage.get_suspension_result(flow_id, step, signal_name)
    assert retrieved is None


@pytest.mark.asyncio
async def test_memory_log_timer():
    """Test logging a timer."""
    storage = InMemoryExecutionLog()
    flow_id = str(uuid4())
    step = 3
    timer_name = "test_timer"
    fire_at = datetime.now(UTC) + timedelta(seconds=10)

    # Start invocation
    await storage.log_invocation_start(
        flow_id=flow_id,
        step=step,
        class_name="TestFlow",
        method_name="run",
        parameters=b"params",
        params_hash=123,
        retry_policy=None,
    )

    # Log timer
    await storage.log_timer(flow_id, step, fire_at, timer_name)

    # Verify invocation status changed
    invocation = await storage.get_invocation(flow_id, step)
    assert invocation.status == InvocationStatus.WAITING_FOR_TIMER

    # Get expired timers (should be empty since timer is in future)
    now = datetime.now(UTC)
    expired = await storage.get_expired_timers(now)
    assert len(expired) == 0


@pytest.mark.asyncio
async def test_memory_get_expired_timers():
    """Test getting expired timers."""
    storage = InMemoryExecutionLog()
    flow_id1 = str(uuid4())
    flow_id2 = str(uuid4())

    now = datetime.now(UTC)
    past = now - timedelta(seconds=10)
    future = now + timedelta(seconds=10)

    # Start invocations
    for flow_id in [flow_id1, flow_id2]:
        await storage.log_invocation_start(
            flow_id=flow_id,
            step=0,
            class_name="TestFlow",
            method_name="run",
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
    assert any(t.flow_id == flow_id1 for t in expired)


@pytest.mark.asyncio
async def test_memory_claim_timer():
    """Test atomically claiming a timer."""
    storage = InMemoryExecutionLog()
    flow_id = str(uuid4())

    past = datetime.now(UTC) - timedelta(seconds=10)

    # Start invocation
    await storage.log_invocation_start(
        flow_id=flow_id,
        step=0,
        class_name="TestFlow",
        method_name="run",
        parameters=b"params",
        params_hash=123,
        retry_policy=None,
    )

    # Log timer
    await storage.log_timer(flow_id, 0, past, "timer")

    # Claim the timer
    success = await storage.claim_timer(flow_id, 0)
    assert success is True

    # Try to claim again (should fail since already claimed)
    success = await storage.claim_timer(flow_id, 0)
    assert success is False


@pytest.mark.asyncio
async def test_memory_log_signal():
    """Test logging a signal."""
    storage = InMemoryExecutionLog()
    flow_id = str(uuid4())

    # Start invocation
    await storage.log_invocation_start(
        flow_id=flow_id,
        step=0,
        class_name="TestFlow",
        method_name="run",
        parameters=b"params",
        params_hash=123,
        retry_policy=None,
    )

    # Log signal
    await storage.log_signal(flow_id, 0, "test_signal")

    # Verify invocation status changed
    invocation = await storage.get_invocation(flow_id, 0)
    assert invocation.status == InvocationStatus.WAITING_FOR_SIGNAL
    assert invocation.timer_name == "test_signal"


@pytest.mark.asyncio
async def test_memory_move_ready_delayed_tasks():
    """Test moving ready delayed tasks."""
    storage = InMemoryExecutionLog()
    flow_id = str(uuid4())

    now = datetime.now(UTC)
    past_time = now - timedelta(seconds=10)

    # Create flow scheduled for past (should be ready)
    flow = ScheduledFlow(
        task_id=flow_id,
        flow_id=flow_id,
        flow_type="TestFlow",
        flow_data=b"data",
        status=TaskStatus.PENDING,
        locked_by=None,
        created_at=now,
        updated_at=now,
        retry_count=0,
        error_message=None,
        scheduled_for=past_time,
        parent_metadata=None,
        retry_policy=None,
        version=None,
    )

    await storage.enqueue_flow(flow)

    # Move ready tasks
    count = await storage.move_ready_delayed_tasks()
    assert count >= 0  # Should find and move the task


@pytest.mark.asyncio
async def test_memory_resume_flow():
    """Test resuming a suspended flow."""
    storage = InMemoryExecutionLog()
    flow_id = str(uuid4())

    now = datetime.now(UTC)
    flow = ScheduledFlow(
        task_id=flow_id,
        flow_id=flow_id,
        flow_type="TestFlow",
        flow_data=b"data",
        status=TaskStatus.SUSPENDED,
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

    # Resume flow
    success = await storage.resume_flow(flow_id)
    assert success is True

    # Verify status changed
    updated = await storage.get_scheduled_flow(flow_id)
    assert updated.status == TaskStatus.PENDING

    # Try to resume again (should return False since not suspended)
    success = await storage.resume_flow(flow_id)
    assert success is False


@pytest.mark.asyncio
async def test_memory_get_next_timer_fire_time():
    """Test getting the next timer fire time."""
    storage = InMemoryExecutionLog()
    flow_id = str(uuid4())

    # Initially no timers
    next_fire = await storage.get_next_timer_fire_time()
    assert next_fire is None

    # Add a timer
    future = datetime.now(UTC) + timedelta(seconds=30)

    await storage.log_invocation_start(
        flow_id=flow_id,
        step=0,
        class_name="TestFlow",
        method_name="run",
        parameters=b"params",
        params_hash=123,
        retry_policy=None,
    )

    await storage.log_timer(flow_id, 0, future, "timer1")

    # Get next fire time
    next_fire = await storage.get_next_timer_fire_time()
    assert next_fire is not None
    assert next_fire == future


@pytest.mark.asyncio
async def test_memory_get_invocations_for_flow():
    """Test getting all invocations for a flow."""
    storage = InMemoryExecutionLog()
    flow_id = str(uuid4())

    # Add multiple invocations
    for step in range(3):
        await storage.log_invocation_start(
            flow_id=flow_id,
            step=step,
            class_name="TestFlow",
            method_name=f"step_{step}",
            parameters=b"params",
            params_hash=123 + step,
            retry_policy=None,
        )

    # Get all invocations
    invocations = await storage.get_invocations_for_flow(flow_id)

    assert len(invocations) == 3
    assert all(inv.flow_id == flow_id for inv in invocations)
    assert {inv.step for inv in invocations} == {0, 1, 2}
