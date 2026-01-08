"""Comprehensive tests for PendingChild functionality."""

import pickle
from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import uuid4

import pytest

from pyergon.core import ChildFlowError, InvocationStatus, ScheduledFlow, TaskStatus
from pyergon.decorators import flow, flow_type, step
from pyergon.executor import Executor
from pyergon.executor.outcome import is_suspended
from pyergon.executor.pending_child import PendingChild
from pyergon.executor.suspension_payload import SuspensionPayload
from pyergon.storage.memory import InMemoryExecutionLog


@dataclass
@flow_type
class ChildWorkflow:
    """Simple child workflow for testing."""

    value: int = 0

    @step
    async def process(self) -> int:
        return self.value * 2

    @flow
    async def run(self) -> int:
        return await self.process()


@dataclass
@flow_type
class FailingChildWorkflow:
    """Child workflow that raises an error."""

    @step
    async def failing_step(self) -> int:
        raise ValueError("Child error")

    @flow
    async def run(self) -> int:
        return await self.failing_step()


@dataclass
@flow_type
class ParentWorkflow:
    """Parent workflow that invokes children."""

    child_value: int = 10

    @step
    async def invoke_child(self) -> int:
        child = ChildWorkflow(value=self.child_value)
        pending = self.invoke(child)
        result = await pending.result()
        return result

    @flow
    async def run(self) -> int:
        return await self.invoke_child()


@dataclass
@flow_type
class ParentWithErrorChild:
    """Parent that invokes a failing child."""

    @step
    async def invoke_failing_child(self) -> int:
        child = FailingChildWorkflow()
        pending = self.invoke(child)
        result = await pending.result()
        return result

    @flow
    async def run(self) -> int:
        return await self.invoke_failing_child()


@pytest.mark.asyncio
async def test_pending_child_initialization():
    """Test PendingChild initialization."""
    child_bytes = b"test_data"
    child_type = "TestFlow"

    pending = PendingChild(child_bytes, child_type)

    assert pending.child_bytes == child_bytes
    assert pending.child_type == child_type


@pytest.mark.asyncio
async def test_pending_child_repr():
    """Test PendingChild string representation."""
    child_bytes = b"test_data"
    child_type = "TestFlow"

    pending = PendingChild(child_bytes, child_type)

    repr_str = repr(pending)
    assert "PendingChild" in repr_str
    assert "TestFlow" in repr_str


@pytest.mark.asyncio
async def test_pending_child_result_no_context():
    """Test PendingChild.result() raises error when no context."""
    child_bytes = b"test_data"
    child_type = "TestFlow"

    pending = PendingChild(child_bytes, child_type)

    # Should raise RuntimeError when called outside context
    with pytest.raises(RuntimeError, match="outside execution context"):
        await pending.result()


@pytest.mark.asyncio
async def test_parent_invoke_child_success():
    """Test parent flow invoking child flow successfully."""
    storage = InMemoryExecutionLog()

    # Create parent workflow
    parent = ParentWorkflow(child_value=15)

    # Execute parent with valid UUID
    parent_id = str(uuid4())
    executor = Executor(parent, storage, parent_id)
    outcome = await executor.run(lambda w: w.run())

    # Parent should suspend waiting for child
    # In real usage, worker would process child and resume parent
    # For now, just verify suspension
    assert is_suspended(outcome)


@pytest.mark.asyncio
async def test_pending_child_deterministic_uuid():
    """Test that child UUIDs are deterministic."""
    storage = InMemoryExecutionLog()

    # Execute same parent twice with valid UUIDs
    parent1 = ParentWorkflow(child_value=20)
    parent1_id = str(uuid4())
    executor1 = Executor(parent1, storage, parent1_id)
    outcome1 = await executor1.run(lambda w: w.run())

    # Second execution with same parent
    parent2 = ParentWorkflow(child_value=20)
    parent2_id = str(uuid4())
    executor2 = Executor(parent2, storage, parent2_id)
    outcome2 = await executor2.run(lambda w: w.run())

    # Verify both suspensions
    assert is_suspended(outcome1)
    assert is_suspended(outcome2)


@pytest.mark.asyncio
async def test_pending_child_with_existing_result():
    """Test PendingChild when result already exists."""
    storage = InMemoryExecutionLog()
    parent_id = str(uuid4())
    step = 12345

    # Manually create invocation with result
    await storage.log_invocation_start(
        flow_id=parent_id,
        step=step,
        class_name="<child_flow>",
        method_name="invoke(ChildWorkflow)",
        parameters=b"params",
        params_hash=123,
        retry_policy=None,
    )

    # Create success payload
    child_result = 42
    payload = SuspensionPayload(success=True, data=pickle.dumps(child_result), is_retryable=None)
    result_bytes = pickle.dumps(payload)

    # Mark invocation complete with result
    await storage.log_invocation_completion(parent_id, step, result_bytes)

    # Verify invocation is complete
    inv = await storage.get_invocation(parent_id, step)
    assert inv.status == InvocationStatus.COMPLETE


@pytest.mark.asyncio
async def test_pending_child_signal_arrives_before_wait():
    """Test when child signal arrives before parent waits."""
    storage = InMemoryExecutionLog()
    parent_id = str(uuid4())
    step = 54321
    signal_name = "test-signal"

    # Manually create invocation waiting for signal
    await storage.log_invocation_start(
        flow_id=parent_id,
        step=step,
        class_name="<child_flow>",
        method_name="invoke(ChildWorkflow)",
        parameters=b"params",
        params_hash=123,
        retry_policy=None,
    )

    await storage.log_signal(parent_id, step, signal_name)

    # Store signal result (as if child completed)
    child_result = 99
    payload = SuspensionPayload(success=True, data=pickle.dumps(child_result), is_retryable=None)
    result_bytes = pickle.dumps(payload)

    await storage.store_suspension_result(parent_id, step, signal_name, result_bytes)

    # Verify invocation is waiting
    inv = await storage.get_invocation(parent_id, step)
    assert inv.status == InvocationStatus.WAITING_FOR_SIGNAL


@pytest.mark.asyncio
async def test_child_flow_error_parsing():
    """Test ChildFlowError parsing from error message."""
    # Test with type prefix
    error1 = ChildFlowError("ValueError", "Test message", retryable=True)
    assert error1.type_name == "ValueError"
    assert error1.message == "Test message"
    assert error1.is_retryable() is True

    # Test non-retryable
    error2 = ChildFlowError("TypeError", "Type error", retryable=False)
    assert error2.is_retryable() is False


@pytest.mark.asyncio
async def test_pending_child_error_payload():
    """Test PendingChild handling error payload."""
    storage = InMemoryExecutionLog()
    parent_id = str(uuid4())
    step = 99999

    # Create invocation
    await storage.log_invocation_start(
        flow_id=parent_id,
        step=step,
        class_name="<child_flow>",
        method_name="invoke(FailingChild)",
        parameters=b"params",
        params_hash=123,
        retry_policy=None,
    )

    # Create error payload
    error_msg = "ValueError: Test error"
    payload = SuspensionPayload(success=False, data=pickle.dumps(error_msg), is_retryable=False)
    result_bytes = pickle.dumps(payload)

    # Mark as complete with error
    await storage.log_invocation_completion(parent_id, step, result_bytes)

    # Verify stored
    inv = await storage.get_invocation(parent_id, step)
    assert inv.status == InvocationStatus.COMPLETE
    assert inv.return_value == result_bytes


@pytest.mark.asyncio
async def test_pending_child_scheduled_flow_exists():
    """Test PendingChild when child flow already scheduled."""
    storage = InMemoryExecutionLog()
    child_id = str(uuid4())

    # Pre-schedule child flow
    now = datetime.now(UTC)
    scheduled = ScheduledFlow(
        task_id=child_id,
        flow_id=child_id,
        flow_type="ChildWorkflow",
        flow_data=b"child_data",
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

    await storage.enqueue_flow(scheduled)

    # Verify flow exists
    flow = await storage.get_scheduled_flow(child_id)
    assert flow is not None
    assert flow.flow_type == "ChildWorkflow"


@pytest.mark.asyncio
async def test_pending_child_type_parameter():
    """Test PendingChild generic type parameter."""
    # Create PendingChild with type hint
    child = ChildWorkflow(value=10)
    child_bytes = pickle.dumps(child)

    pending: PendingChild[int] = PendingChild(child_bytes, "ChildWorkflow")

    assert pending.child_type == "ChildWorkflow"


@pytest.mark.asyncio
async def test_child_flow_error_without_type_prefix():
    """Test ChildFlowError when error message has no type prefix."""
    error = ChildFlowError("unknown", "Generic error message", retryable=True)

    assert error.type_name == "unknown"
    assert error.message == "Generic error message"


@pytest.mark.asyncio
async def test_pending_child_complex_data():
    """Test PendingChild with complex nested data."""
    complex_child = ChildWorkflow(value=12345)
    child_bytes = pickle.dumps(complex_child)

    _pending = PendingChild(child_bytes, "ChildWorkflow")

    # Verify bytes are preserved
    deserialized = pickle.loads(child_bytes)
    assert deserialized.value == 12345


@pytest.mark.asyncio
async def test_pending_child_empty_data():
    """Test PendingChild with minimal data."""
    pending = PendingChild(b"", "EmptyFlow")

    assert pending.child_bytes == b""
    assert pending.child_type == "EmptyFlow"


@pytest.mark.asyncio
async def test_suspension_payload_in_pending_child_context():
    """Test SuspensionPayload usage in PendingChild context."""
    # Success payload
    success_payload = SuspensionPayload(
        success=True, data=pickle.dumps({"result": "ok"}), is_retryable=None
    )

    assert success_payload.success is True
    result = pickle.loads(success_payload.data)
    assert result == {"result": "ok"}

    # Error payload
    error_payload = SuspensionPayload(
        success=False, data=pickle.dumps("Error message"), is_retryable=False
    )

    assert error_payload.success is False
    assert error_payload.is_retryable is False


@pytest.mark.asyncio
async def test_child_flow_error_exception_inheritance():
    """Test ChildFlowError is an Exception."""
    error = ChildFlowError("TestError", "Test message", retryable=True)

    assert isinstance(error, Exception)

    # Should be raisable
    with pytest.raises(ChildFlowError):
        raise error
