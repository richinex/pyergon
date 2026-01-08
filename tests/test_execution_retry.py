"""Tests for execution flow completion, error handling, and retry logic."""

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from pyergon.core import RetryPolicy, ScheduledFlow, TaskStatus
from pyergon.executor.execution import (
    check_should_retry,
    handle_flow_completion,
    handle_flow_error,
    handle_suspended_flow,
)
from pyergon.executor.outcome import SuspendReason
from pyergon.storage.memory import InMemoryExecutionLog


@pytest.mark.asyncio
async def test_handle_flow_completion_no_parent():
    """Test handle_flow_completion with no parent flow."""
    storage = InMemoryExecutionLog()
    flow_id = str(uuid4())
    task_id = str(uuid4())

    # Create flow
    await _create_flow(storage, task_id, flow_id)

    # Handle completion (no parent)
    await handle_flow_completion(
        storage=storage,
        worker_id="worker-1",
        flow_task_id=task_id,
        flow_id=flow_id,
        parent_metadata=None,
    )

    # Verify flow marked complete
    flow = await storage.get_scheduled_flow(task_id)
    assert flow.status == TaskStatus.COMPLETE


@pytest.mark.asyncio
async def test_handle_flow_error_no_retry():
    """Test handle_flow_error when retry is not needed."""
    storage = InMemoryExecutionLog()
    flow_id = str(uuid4())
    task_id = str(uuid4())

    # Create flow
    flow = await _create_flow(storage, task_id, flow_id)

    # Create invocation with no retry policy
    await storage.log_invocation_start(
        flow_id=flow_id,
        step=0,
        class_name="TestFlow",
        method_name="run",
        parameters=b"params",
        params_hash=123,
        retry_policy=None,
    )

    # Mark error as non-retryable
    await storage.update_is_retryable(flow_id, 0, False)

    # Handle error
    error = RuntimeError("Test error")
    await handle_flow_error(
        storage=storage,
        worker_id="worker-1",
        flow=flow,
        flow_task_id=task_id,
        error=error,
        parent_metadata=None,
    )

    # Verify flow marked failed
    updated_flow = await storage.get_scheduled_flow(task_id)
    assert updated_flow.status == TaskStatus.FAILED
    assert updated_flow.error_message == "Test error"


@pytest.mark.asyncio
async def test_handle_flow_error_with_retry():
    """Test handle_flow_error schedules retry."""
    storage = InMemoryExecutionLog()
    flow_id = str(uuid4())
    task_id = str(uuid4())

    # Create flow with retry policy
    flow = await _create_flow(storage, task_id, flow_id, retry_count=0)

    # Create invocation with retry policy
    await storage.log_invocation_start(
        flow_id=flow_id,
        step=0,
        class_name="TestFlow",
        method_name="run",
        parameters=b"params",
        params_hash=123,
        retry_policy=RetryPolicy.STANDARD,  # 3 attempts
    )

    # Handle error
    error = RuntimeError("Retryable error")
    await handle_flow_error(
        storage=storage,
        worker_id="worker-1",
        flow=flow,
        flow_task_id=task_id,
        error=error,
        parent_metadata=None,
    )

    # Verify retry scheduled
    updated_flow = await storage.get_scheduled_flow(task_id)
    assert updated_flow.status == TaskStatus.PENDING
    assert updated_flow.retry_count == 1
    assert updated_flow.error_message == "Retryable error"
    assert updated_flow.scheduled_for is not None


@pytest.mark.asyncio
async def test_handle_flow_error_max_retries_exceeded():
    """Test handle_flow_error when max retries exceeded."""
    storage = InMemoryExecutionLog()
    flow_id = str(uuid4())
    task_id = str(uuid4())

    # Create flow at max retries
    flow = await _create_flow(storage, task_id, flow_id, retry_count=2)

    # Create invocation with retry policy (max 3 attempts)
    await storage.log_invocation_start(
        flow_id=flow_id,
        step=0,
        class_name="TestFlow",
        method_name="run",
        parameters=b"params",
        params_hash=123,
        retry_policy=RetryPolicy.STANDARD,
    )

    # Handle error (should not retry since at max attempts)
    error = RuntimeError("Final error")
    await handle_flow_error(
        storage=storage,
        worker_id="worker-1",
        flow=flow,
        flow_task_id=task_id,
        error=error,
        parent_metadata=None,
    )

    # Verify flow marked failed (no more retries)
    updated_flow = await storage.get_scheduled_flow(task_id)
    assert updated_flow.status == TaskStatus.FAILED
    assert updated_flow.error_message == "Final error"


@pytest.mark.asyncio
async def test_handle_suspended_flow():
    """Test handle_suspended_flow."""
    storage = InMemoryExecutionLog()
    flow_id = str(uuid4())
    task_id = str(uuid4())

    # Create flow
    await _create_flow(storage, task_id, flow_id, status=TaskStatus.RUNNING)

    # Create invocation
    await storage.log_invocation_start(
        flow_id=flow_id,
        step=0,
        class_name="TestFlow",
        method_name="run",
        parameters=b"params",
        params_hash=123,
        retry_policy=None,
    )

    # Suspend flow
    reason = SuspendReason(flow_id=flow_id, step=0, signal_name="test_signal")
    await handle_suspended_flow(
        storage=storage,
        worker_id="worker-1",
        flow_task_id=task_id,
        flow_id=flow_id,
        reason=reason,
    )

    # Verify flow marked suspended
    updated_flow = await storage.get_scheduled_flow(task_id)
    assert updated_flow.status == TaskStatus.SUSPENDED


@pytest.mark.asyncio
async def test_check_should_retry_no_policy():
    """Test check_should_retry with no retry policy."""
    storage = InMemoryExecutionLog()
    flow_id = str(uuid4())
    task_id = str(uuid4())

    flow = await _create_flow(storage, task_id, flow_id, retry_count=0)

    # Create invocation without retry policy
    await storage.log_invocation_start(
        flow_id=flow_id,
        step=0,
        class_name="TestFlow",
        method_name="run",
        parameters=b"params",
        params_hash=123,
        retry_policy=None,
    )

    # Check retry (should use default policy since error is retryable)
    delay = await check_should_retry(storage, flow)
    assert delay is not None  # Default STANDARD policy


@pytest.mark.asyncio
async def test_check_should_retry_with_policy():
    """Test check_should_retry with explicit retry policy."""
    storage = InMemoryExecutionLog()
    flow_id = str(uuid4())
    task_id = str(uuid4())

    flow = await _create_flow(storage, task_id, flow_id, retry_count=0)

    # Create invocation with AGGRESSIVE retry policy
    await storage.log_invocation_start(
        flow_id=flow_id,
        step=0,
        class_name="TestFlow",
        method_name="run",
        parameters=b"params",
        params_hash=123,
        retry_policy=RetryPolicy.AGGRESSIVE,  # 5 attempts
    )

    # Check retry
    delay = await check_should_retry(storage, flow)
    assert delay is not None
    assert isinstance(delay, timedelta)


@pytest.mark.asyncio
async def test_check_should_retry_exponential_backoff():
    """Test check_should_retry calculates exponential backoff correctly."""
    storage = InMemoryExecutionLog()
    flow_id = str(uuid4())
    task_id = str(uuid4())

    # Test first retry (attempt 1)
    flow1 = await _create_flow(storage, task_id + "_1", flow_id + "_1", retry_count=0)
    await storage.log_invocation_start(
        flow_id=flow_id + "_1",
        step=0,
        class_name="TestFlow",
        method_name="run",
        parameters=b"params",
        params_hash=123,
        retry_policy=RetryPolicy.STANDARD,
    )

    delay1 = await check_should_retry(storage, flow1)
    assert delay1 is not None

    # Test second retry (attempt 2) - should have longer delay
    flow2 = await _create_flow(storage, task_id + "_2", flow_id + "_2", retry_count=1)
    await storage.log_invocation_start(
        flow_id=flow_id + "_2",
        step=0,
        class_name="TestFlow",
        method_name="run",
        parameters=b"params",
        params_hash=123,
        retry_policy=RetryPolicy.STANDARD,
    )

    delay2 = await check_should_retry(storage, flow2)
    assert delay2 is not None
    assert delay2 > delay1  # Exponential backoff


@pytest.mark.asyncio
async def test_check_should_retry_max_delay():
    """Test check_should_retry respects max_delay."""
    storage = InMemoryExecutionLog()
    flow_id = str(uuid4())
    task_id = str(uuid4())

    # Create flow with 1 retry (still within AGGRESSIVE policy limit of 5)
    flow = await _create_flow(storage, task_id, flow_id, retry_count=1)

    await storage.log_invocation_start(
        flow_id=flow_id,
        step=0,
        class_name="TestFlow",
        method_name="run",
        parameters=b"params",
        params_hash=123,
        retry_policy=RetryPolicy.AGGRESSIVE,  # 5 attempts, higher backoff
    )

    delay = await check_should_retry(storage, flow)

    # Should be capped at max_delay
    assert delay is not None
    assert delay <= timedelta(seconds=RetryPolicy.AGGRESSIVE.max_delay_ms / 1000.0)


@pytest.mark.asyncio
async def test_check_should_retry_non_retryable_error():
    """Test check_should_retry returns None for non-retryable errors."""
    storage = InMemoryExecutionLog()
    flow_id = str(uuid4())
    task_id = str(uuid4())

    flow = await _create_flow(storage, task_id, flow_id, retry_count=0)

    await storage.log_invocation_start(
        flow_id=flow_id,
        step=0,
        class_name="TestFlow",
        method_name="run",
        parameters=b"params",
        params_hash=123,
        retry_policy=RetryPolicy.STANDARD,
    )

    # Mark error as non-retryable
    await storage.update_is_retryable(flow_id, 0, False)

    # Check retry (should return None)
    delay = await check_should_retry(storage, flow)
    assert delay is None


@pytest.mark.asyncio
async def test_check_should_retry_missing_invocation():
    """Test check_should_retry handles missing invocation."""
    storage = InMemoryExecutionLog()
    flow_id = str(uuid4())
    task_id = str(uuid4())

    flow = await _create_flow(storage, task_id, flow_id, retry_count=0)

    # Don't create invocation

    # Check retry (should return None since invocation missing)
    delay = await check_should_retry(storage, flow)
    assert delay is None


# Helper function
async def _create_flow(
    storage: InMemoryExecutionLog,
    task_id: str,
    flow_id: str,
    retry_count: int = 0,
    status: TaskStatus = TaskStatus.RUNNING,
) -> ScheduledFlow:
    """Helper to create a flow for testing."""
    now = datetime.now(UTC)
    flow = ScheduledFlow(
        task_id=task_id,
        flow_id=flow_id,
        flow_type="TestFlow",
        flow_data=b"data",
        status=status,
        locked_by="worker-1" if status == TaskStatus.RUNNING else None,
        created_at=now,
        updated_at=now,
        retry_count=retry_count,
        error_message=None,
        scheduled_for=None,
        parent_metadata=None,
        retry_policy=None,
        version=None,
    )
    await storage.enqueue_flow(flow)
    return flow
