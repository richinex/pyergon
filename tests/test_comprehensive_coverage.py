"""Comprehensive tests to reach 80% coverage target."""

import pickle
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from pyergon.core import InvocationStatus, RetryPolicy, ScheduledFlow, TaskStatus
from pyergon.decorators import flow, flow_type, step
from pyergon.executor import Executor
from pyergon.executor.execution import check_should_retry, handle_flow_error
from pyergon.executor.instance import Executor as InstanceExecutor
from pyergon.executor.suspension_payload import SuspensionPayload
from pyergon.storage.memory import InMemoryExecutionLog
from pyergon.storage.sqlite import SqliteExecutionLog


@pytest.mark.asyncio
async def test_suspension_payload_serialization():
    """Test SuspensionPayload serialization."""
    payload = SuspensionPayload(
        success=True, data=pickle.dumps({"result": "value"}), is_retryable=True
    )

    # Serialize
    serialized = pickle.dumps(payload)
    assert serialized is not None

    # Deserialize
    deserialized = pickle.loads(serialized)
    assert deserialized.success is True
    assert pickle.loads(deserialized.data) == {"result": "value"}
    assert deserialized.is_retryable is True


@pytest.mark.asyncio
async def test_suspension_payload_with_none():
    """Test SuspensionPayload with None values."""
    payload = SuspensionPayload(success=False, data=pickle.dumps(None), is_retryable=None)

    serialized = pickle.dumps(payload)
    deserialized = pickle.loads(serialized)

    assert deserialized.success is False
    assert pickle.loads(deserialized.data) is None
    assert deserialized.is_retryable is None


@pytest.mark.asyncio
async def test_check_should_retry_with_max_attempts():
    """Test check_should_retry respects max attempts."""
    storage = InMemoryExecutionLog()
    flow_id = str(uuid4())
    task_id = str(uuid4())

    now = datetime.now(UTC)
    # Flow already at max retries for STANDARD policy (3 attempts = retry_count 2)
    flow = ScheduledFlow(
        task_id=task_id,
        flow_id=flow_id,
        flow_type="TestFlow",
        flow_data=b"data",
        status=TaskStatus.RUNNING,
        locked_by="worker-1",
        created_at=now,
        updated_at=now,
        retry_count=2,  # Max for STANDARD
        error_message=None,
        scheduled_for=None,
        parent_metadata=None,
        retry_policy=None,
        version=None,
    )
    await storage.enqueue_flow(flow)

    # Add invocation
    await storage.log_invocation_start(
        flow_id=flow_id,
        step=0,
        class_name="TestFlow",
        method_name="run",
        parameters=b"params",
        params_hash=123,
        retry_policy=RetryPolicy.STANDARD,
    )

    # Should not retry (at max attempts)
    delay = await check_should_retry(storage, flow)
    assert delay is None


@pytest.mark.asyncio
async def test_check_should_retry_exponential_backoff():
    """Test exponential backoff calculation."""
    storage = InMemoryExecutionLog()
    flow_id = str(uuid4())
    task_id = str(uuid4())

    now = datetime.now(UTC)

    # Create flow at retry_count=0 (first retry)
    flow = ScheduledFlow(
        task_id=task_id,
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

    await storage.log_invocation_start(
        flow_id=flow_id,
        step=0,
        class_name="TestFlow",
        method_name="run",
        parameters=b"params",
        params_hash=123,
        retry_policy=RetryPolicy.STANDARD,
    )

    delay = await check_should_retry(storage, flow)
    assert delay is not None
    assert isinstance(delay, timedelta)


@pytest.mark.asyncio
async def test_handle_flow_error_with_parent():
    """Test handle_flow_error with parent metadata."""
    storage = InMemoryExecutionLog()
    flow_id = str(uuid4())
    task_id = str(uuid4())
    parent_id = str(uuid4())

    now = datetime.now(UTC)
    parent_metadata = (parent_id, "signal_token")

    flow = ScheduledFlow(
        task_id=task_id,
        flow_id=flow_id,
        flow_type="ChildFlow",
        flow_data=b"data",
        status=TaskStatus.RUNNING,
        locked_by="worker-1",
        created_at=now,
        updated_at=now,
        retry_count=0,
        error_message=None,
        scheduled_for=None,
        parent_metadata=parent_metadata,
        retry_policy=None,
        version=None,
    )
    await storage.enqueue_flow(flow)

    await storage.log_invocation_start(
        flow_id=flow_id,
        step=0,
        class_name="ChildFlow",
        method_name="run",
        parameters=b"params",
        params_hash=123,
        retry_policy=None,
    )

    # Mark as non-retryable
    await storage.update_is_retryable(flow_id, 0, False)

    # Handle error
    error = RuntimeError("Child flow error")
    await handle_flow_error(
        storage=storage,
        worker_id="worker-1",
        flow=flow,
        flow_task_id=task_id,
        error=error,
        parent_metadata=parent_metadata,
    )

    # Verify flow marked failed
    updated = await storage.get_scheduled_flow(task_id)
    assert updated.status == TaskStatus.FAILED


@pytest.mark.asyncio
async def test_executor_instance_flow_id():
    """Test Executor instance flow_id generation."""

    @dataclass
    @flow_type
    class SimpleFlow:
        @step
        async def do_work(self) -> str:
            return "work_done"

        @flow
        async def run(self) -> str:
            return await self.do_work()

    storage = InMemoryExecutionLog()
    workflow = SimpleFlow()

    # With explicit flow_id
    executor1 = InstanceExecutor(workflow, storage, "custom-flow-id")
    assert executor1.flow_id == "custom-flow-id"

    # Without flow_id (auto-generated)
    executor2 = InstanceExecutor(workflow, storage)
    assert executor2.flow_id is not None
    assert len(executor2.flow_id) > 0


@pytest.mark.asyncio
async def test_flow_with_all_retry_policies():
    """Test flow with different retry policies."""

    @dataclass
    @flow_type
    class RetryFlow:
        @step
        async def step_standard(self) -> str:
            return "standard"

        @step
        async def step_aggressive(self) -> str:
            return "aggressive"

        @flow
        async def run(self) -> str:
            s1 = await self.step_standard()
            s2 = await self.step_aggressive()
            return f"{s1}+{s2}"

    storage = InMemoryExecutionLog()
    workflow = RetryFlow()

    executor = Executor(workflow, storage, "retry-policy-test")
    outcome = await executor.run(lambda w: w.run())

    assert outcome.result == "standard+aggressive"


@pytest.mark.asyncio
async def test_memory_storage_complete_with_error_message():
    """Test completing flow with error message."""
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

    # Complete with error
    await storage.complete_flow(flow_id, TaskStatus.FAILED, "Test error message")

    updated = await storage.get_scheduled_flow(flow_id)
    assert updated.status == TaskStatus.FAILED
    assert updated.error_message == "Test error message"


@pytest.mark.asyncio
async def test_sqlite_storage_complete_with_error_message():
    """Test completing flow with error message in SQLite."""
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

    # Complete with error
    await storage.complete_flow(flow_id, TaskStatus.FAILED, "SQLite error message")

    updated = await storage.get_scheduled_flow(flow_id)
    assert updated.status == TaskStatus.FAILED
    assert updated.error_message == "SQLite error message"


@pytest.mark.asyncio
async def test_invocation_with_all_statuses():
    """Test invocations with all possible statuses."""
    storage = InMemoryExecutionLog()
    flow_id = str(uuid4())

    statuses = [
        InvocationStatus.PENDING,
        InvocationStatus.WAITING_FOR_SIGNAL,
        InvocationStatus.COMPLETE,
    ]

    for i, status in enumerate(statuses):
        await storage.log_invocation_start(
            flow_id=flow_id,
            step=i,
            class_name="TestFlow",
            method_name=f"step_{i}",
            parameters=b"params",
            params_hash=123 + i,
            retry_policy=None,
        )

        # Update status
        invocation = await storage.get_invocation(flow_id, i)
        if status == InvocationStatus.WAITING_FOR_SIGNAL:
            await storage.log_signal(flow_id, i, f"signal_{i}")
        elif status == InvocationStatus.COMPLETE:
            invocation.set_status(InvocationStatus.COMPLETE)

    # Verify all invocations
    invocations = await storage.get_invocations_for_flow(flow_id)
    assert len(invocations) == len(statuses)


@pytest.mark.asyncio
async def test_retry_policy_all_variants():
    """Test all retry policy variants."""
    policies = [RetryPolicy.STANDARD, RetryPolicy.AGGRESSIVE, RetryPolicy.NONE]

    for policy in policies:
        assert policy.max_attempts >= 1
        assert policy.initial_delay_ms >= 0
        assert policy.max_delay_ms >= 0
        assert policy.backoff_multiplier >= 1.0


@pytest.mark.asyncio
async def test_flow_data_persistence():
    """Test that flow data is persisted correctly."""

    @dataclass
    @flow_type
    class DataFlow:
        value: int = 0

        @step
        async def increment(self) -> int:
            self.value += 1
            return self.value

        @flow
        async def run(self) -> int:
            return await self.increment()

    storage = InMemoryExecutionLog()
    workflow = DataFlow(value=10)

    executor = Executor(workflow, storage, "data-persist-test")
    outcome = await executor.run(lambda w: w.run())

    assert outcome.result == 11
    assert workflow.value == 11


@pytest.mark.asyncio
async def test_multiple_invocations_same_step():
    """Test multiple invocations don't interfere."""
    storage = InMemoryExecutionLog()
    flow_id1 = str(uuid4())
    flow_id2 = str(uuid4())

    # Same step number, different flows
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

    # Both should exist independently
    inv1 = await storage.get_invocation(flow_id1, 0)
    inv2 = await storage.get_invocation(flow_id2, 0)

    assert inv1 is not None
    assert inv2 is not None
    assert inv1.flow_id != inv2.flow_id
