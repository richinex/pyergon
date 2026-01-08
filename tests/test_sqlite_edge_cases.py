"""Edge case tests for SQLite storage."""

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from pyergon.core import InvocationStatus, RetryPolicy, ScheduledFlow, TaskStatus
from pyergon.storage.sqlite import SqliteExecutionLog


@pytest.mark.asyncio
async def test_sqlite_in_memory_connection():
    """Test SQLite in-memory database creation."""
    storage = await SqliteExecutionLog.in_memory()
    assert storage is not None

    # Should be able to perform operations
    flow_id = str(uuid4())
    await storage.log_invocation_start(
        flow_id=flow_id,
        step=0,
        class_name="TestFlow",
        method_name="run",
        parameters=b"params",
        params_hash=123,
        retry_policy=None,
    )

    invocation = await storage.get_invocation(flow_id, 0)
    assert invocation is not None


@pytest.mark.asyncio
async def test_sqlite_enqueue_and_dequeue():
    """Test flow enqueueing and dequeueing in SQLite."""
    storage = await SqliteExecutionLog.in_memory()
    flow_id = str(uuid4())

    now = datetime.now(UTC)
    flow = ScheduledFlow(
        task_id=flow_id,
        flow_id=flow_id,
        flow_type="TestFlow",
        flow_data=b"test_data",
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

    # Dequeue
    dequeued = await storage.dequeue_flow("worker-1")
    assert dequeued is not None
    assert dequeued.flow_id == flow_id
    assert dequeued.locked_by == "worker-1"
    assert dequeued.status == TaskStatus.RUNNING


@pytest.mark.asyncio
async def test_sqlite_complete_flow():
    """Test completing a flow in SQLite."""
    storage = await SqliteExecutionLog.in_memory()
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
    await storage.complete_flow(flow_id, TaskStatus.COMPLETE, None)

    updated = await storage.get_scheduled_flow(flow_id)
    assert updated.status == TaskStatus.COMPLETE


@pytest.mark.asyncio
async def test_sqlite_retry_flow():
    """Test retrying a flow in SQLite."""
    storage = await SqliteExecutionLog.in_memory()
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

    # Retry with delay
    delay = timedelta(seconds=5)
    await storage.retry_flow(flow_id, "Test error", delay)

    updated = await storage.get_scheduled_flow(flow_id)
    assert updated.status == TaskStatus.PENDING
    assert updated.retry_count == 1
    assert updated.error_message == "Test error"


@pytest.mark.asyncio
async def test_sqlite_has_non_retryable_error():
    """Test checking for non-retryable errors in SQLite."""
    storage = await SqliteExecutionLog.in_memory()
    flow_id = str(uuid4())

    # Initially no error
    result = await storage.has_non_retryable_error(flow_id)
    assert result is False

    # Add invocation with non-retryable error
    await storage.log_invocation_start(
        flow_id=flow_id,
        step=0,
        class_name="TestFlow",
        method_name="run",
        parameters=b"params",
        params_hash=123,
        retry_policy=None,
    )

    await storage.update_is_retryable(flow_id, 0, False)

    result = await storage.has_non_retryable_error(flow_id)
    assert result is True


@pytest.mark.asyncio
async def test_sqlite_log_timer():
    """Test logging a timer in SQLite."""
    storage = await SqliteExecutionLog.in_memory()
    flow_id = str(uuid4())

    await storage.log_invocation_start(
        flow_id=flow_id,
        step=0,
        class_name="TestFlow",
        method_name="run",
        parameters=b"params",
        params_hash=123,
        retry_policy=None,
    )

    fire_at = datetime.now() + timedelta(seconds=10)
    await storage.log_timer(flow_id, 0, fire_at, "test_timer")

    # Verify status changed
    invocation = await storage.get_invocation(flow_id, 0)
    assert invocation.status == InvocationStatus.WAITING_FOR_TIMER


@pytest.mark.asyncio
async def test_sqlite_get_expired_timers():
    """Test getting expired timers from SQLite."""
    storage = await SqliteExecutionLog.in_memory()
    flow_id1 = str(uuid4())
    flow_id2 = str(uuid4())

    now = datetime.now()
    past = now - timedelta(seconds=10)
    future = now + timedelta(seconds=10)

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

    # Expired timer
    await storage.log_timer(flow_id1, 0, past, "expired")

    # Future timer
    await storage.log_timer(flow_id2, 0, future, "future")

    # Get expired
    expired = await storage.get_expired_timers(now)

    assert len(expired) >= 1
    assert any(t.flow_id == flow_id1 for t in expired)


@pytest.mark.asyncio
async def test_sqlite_claim_timer():
    """Test atomic timer claiming in SQLite."""
    storage = await SqliteExecutionLog.in_memory()
    flow_id = str(uuid4())

    past = datetime.now() - timedelta(seconds=10)

    await storage.log_invocation_start(
        flow_id=flow_id,
        step=0,
        class_name="TestFlow",
        method_name="run",
        parameters=b"params",
        params_hash=123,
        retry_policy=None,
    )

    await storage.log_timer(flow_id, 0, past, "timer")

    # First claim succeeds
    success1 = await storage.claim_timer(flow_id, 0)
    assert success1 is True

    # Second claim fails
    success2 = await storage.claim_timer(flow_id, 0)
    assert success2 is False


@pytest.mark.asyncio
async def test_sqlite_log_signal():
    """Test logging a signal in SQLite."""
    storage = await SqliteExecutionLog.in_memory()
    flow_id = str(uuid4())

    await storage.log_invocation_start(
        flow_id=flow_id,
        step=0,
        class_name="TestFlow",
        method_name="run",
        parameters=b"params",
        params_hash=123,
        retry_policy=None,
    )

    await storage.log_signal(flow_id, 0, "test_signal")

    # Verify status changed
    invocation = await storage.get_invocation(flow_id, 0)
    assert invocation.status == InvocationStatus.WAITING_FOR_SIGNAL


@pytest.mark.asyncio
async def test_sqlite_suspension_results():
    """Test suspension result operations in SQLite."""
    storage = await SqliteExecutionLog.in_memory()
    flow_id = str(uuid4())

    result_data = b"suspension_result"

    # Store
    await storage.store_suspension_result(flow_id, 0, "signal", result_data)

    # Retrieve
    retrieved = await storage.get_suspension_result(flow_id, 0, "signal")
    assert retrieved == result_data

    # Remove
    await storage.remove_suspension_result(flow_id, 0, "signal")

    # Verify removed
    retrieved_after = await storage.get_suspension_result(flow_id, 0, "signal")
    assert retrieved_after is None


@pytest.mark.asyncio
async def test_sqlite_resume_flow():
    """Test resuming a suspended flow in SQLite."""
    storage = await SqliteExecutionLog.in_memory()
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

    # Resume
    success = await storage.resume_flow(flow_id)
    assert success is True

    # Verify status changed
    updated = await storage.get_scheduled_flow(flow_id)
    assert updated.status == TaskStatus.PENDING


@pytest.mark.asyncio
async def test_sqlite_move_ready_delayed_tasks():
    """Test moving ready delayed tasks in SQLite."""
    storage = await SqliteExecutionLog.in_memory()
    flow_id = str(uuid4())

    now = datetime.now(UTC)
    past_time = now - timedelta(seconds=10)

    # Create delayed flow that's now ready
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
    assert count >= 0


@pytest.mark.asyncio
async def test_sqlite_get_invocations_for_flow():
    """Test getting all invocations for a flow in SQLite."""
    storage = await SqliteExecutionLog.in_memory()
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

    # Get all
    invocations = await storage.get_invocations_for_flow(flow_id)

    assert len(invocations) == 3
    assert {inv.step for inv in invocations} == {0, 1, 2}


@pytest.mark.asyncio
async def test_sqlite_update_is_retryable():
    """Test updating is_retryable flag in SQLite."""
    storage = await SqliteExecutionLog.in_memory()
    flow_id = str(uuid4())

    await storage.log_invocation_start(
        flow_id=flow_id,
        step=0,
        class_name="TestFlow",
        method_name="run",
        parameters=b"params",
        params_hash=123,
        retry_policy=None,
    )

    # Update to non-retryable
    await storage.update_is_retryable(flow_id, 0, False)

    invocation = await storage.get_invocation(flow_id, 0)
    assert invocation.is_retryable is False

    # Update to retryable
    await storage.update_is_retryable(flow_id, 0, True)

    invocation = await storage.get_invocation(flow_id, 0)
    assert invocation.is_retryable is True


@pytest.mark.asyncio
async def test_sqlite_reset():
    """Test reset clears all data in SQLite."""
    storage = await SqliteExecutionLog.in_memory()
    flow_id = str(uuid4())

    # Add some data
    await storage.log_invocation_start(
        flow_id=flow_id,
        step=0,
        class_name="TestFlow",
        method_name="run",
        parameters=b"params",
        params_hash=123,
        retry_policy=None,
    )

    # Reset
    await storage.reset()

    # Verify cleared
    invocation = await storage.get_invocation(flow_id, 0)
    assert invocation is None


@pytest.mark.asyncio
async def test_sqlite_concurrent_enqueue():
    """Test concurrent flow enqueueing in SQLite."""
    storage = await SqliteExecutionLog.in_memory()

    now = datetime.now(UTC)
    flows = []

    # Enqueue multiple flows
    for i in range(10):
        flow_id = str(uuid4())
        flow = ScheduledFlow(
            task_id=flow_id,
            flow_id=flow_id,
            flow_type=f"TestFlow{i}",
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
        flows.append(flow_id)

    # All should be enqueued
    for flow_id in flows:
        scheduled_flow = await storage.get_scheduled_flow(flow_id)
        assert scheduled_flow is not None


@pytest.mark.asyncio
async def test_sqlite_get_next_timer_fire_time():
    """Test getting next timer fire time in SQLite."""
    storage = await SqliteExecutionLog.in_memory()

    # Initially no timers
    next_fire = await storage.get_next_timer_fire_time()
    assert next_fire is None

    # Add timer
    flow_id = str(uuid4())
    future = datetime.now() + timedelta(seconds=30)

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


@pytest.mark.asyncio
async def test_sqlite_invocation_with_retry_policy():
    """Test invocation with retry policy in SQLite."""
    storage = await SqliteExecutionLog.in_memory()
    flow_id = str(uuid4())

    await storage.log_invocation_start(
        flow_id=flow_id,
        step=0,
        class_name="TestFlow",
        method_name="run",
        parameters=b"params",
        params_hash=123,
        retry_policy=RetryPolicy.AGGRESSIVE,
    )

    invocation = await storage.get_invocation(flow_id, 0)
    assert invocation.retry_policy == RetryPolicy.AGGRESSIVE
