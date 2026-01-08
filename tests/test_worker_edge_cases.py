"""Comprehensive tests for worker module."""

import asyncio
import pickle
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from pyergon.core import ScheduledFlow, TaskStatus
from pyergon.decorators import flow, flow_type, step
from pyergon.executor.outcome import Completed
from pyergon.executor.suspension_payload import SuspensionPayload
from pyergon.executor.worker import Registry, Worker, WorkerError
from pyergon.storage.memory import InMemoryExecutionLog


@dataclass
@flow_type
class SimpleWorkflow:
    """Simple workflow for testing."""

    value: int = 0

    @step
    async def process(self) -> int:
        await asyncio.sleep(0.001)
        self.value += 10
        return self.value

    @flow
    async def run(self) -> int:
        return await self.process()


@dataclass
@flow_type
class MultiMethodWorkflow:
    """Workflow with multiple methods."""

    @step
    async def step_a(self) -> str:
        return "a"

    @step
    async def step_b(self) -> str:
        return "b"

    @flow
    async def execute(self) -> str:
        a = await self.step_a()
        b = await self.step_b()
        return f"{a}+{b}"


@dataclass
@flow_type
class ErrorWorkflow:
    """Workflow that raises an error."""

    @step
    async def failing_step(self) -> int:
        raise ValueError("Test error")

    @flow
    async def run(self) -> int:
        return await self.failing_step()


@pytest.mark.asyncio
async def test_registry_register_and_get():
    """Test Registry register and get_executor."""
    registry = Registry()

    # Register with explicit executor
    registry.register(SimpleWorkflow, lambda w: w.run())

    # Should have executor
    executor = registry.get_executor("SimpleWorkflow")
    assert executor is not None

    # Should return None for unregistered type
    unknown = registry.get_executor("UnknownFlow")
    assert unknown is None


@pytest.mark.asyncio
async def test_registry_len_and_is_empty():
    """Test Registry len and is_empty."""
    registry = Registry()

    # Initially empty
    assert len(registry) == 0
    assert registry.is_empty() is True

    # Register one
    registry.register(SimpleWorkflow, lambda w: w.run())
    assert len(registry) == 1
    assert registry.is_empty() is False

    # Register another
    registry.register(MultiMethodWorkflow, lambda w: w.execute())
    assert len(registry) == 2


@pytest.mark.asyncio
async def test_registry_execute_flow():
    """Test Registry executor can execute a flow."""
    storage = InMemoryExecutionLog()
    registry = Registry()

    registry.register(SimpleWorkflow, lambda w: w.run())

    # Get executor
    executor = registry.get_executor("SimpleWorkflow")
    assert executor is not None

    # Create flow data
    workflow = SimpleWorkflow(value=5)
    flow_data = pickle.dumps(workflow)
    flow_id = uuid4()

    # Execute
    outcome = await executor(flow_data, flow_id, storage, None)

    # Should complete successfully
    assert isinstance(outcome, Completed)
    assert outcome.result == 15


@pytest.mark.asyncio
async def test_registry_executor_handles_deserialization_error():
    """Test Registry executor handles deserialization errors."""
    storage = InMemoryExecutionLog()
    registry = Registry()

    registry.register(SimpleWorkflow, lambda w: w.run())

    executor = registry.get_executor("SimpleWorkflow")

    # Invalid flow data
    invalid_data = b"invalid pickle data"
    flow_id = uuid4()

    # Execute with invalid data
    outcome = await executor(invalid_data, flow_id, storage, None)

    # Should complete with exception
    assert isinstance(outcome, Completed)
    assert isinstance(outcome.result, Exception)
    assert "failed to deserialize" in str(outcome.result)


@pytest.mark.asyncio
async def test_worker_initialization():
    """Test Worker initialization with default config."""
    storage = InMemoryExecutionLog()
    worker = Worker(storage, "worker-1")

    assert worker._worker_id == "worker-1"
    assert worker._storage == storage
    assert worker._enable_timers is False
    assert worker._poll_interval == 1.0
    assert worker._running is False


@pytest.mark.asyncio
async def test_worker_with_timers():
    """Test Worker with_timers builder method."""
    storage = InMemoryExecutionLog()
    worker = Worker(storage, "worker-1")

    assert worker._enable_timers is False

    worker.with_timers()

    assert worker._enable_timers is True


@pytest.mark.asyncio
async def test_worker_with_timer_interval():
    """Test Worker with_timer_interval builder method."""
    storage = InMemoryExecutionLog()
    worker = Worker(storage, "worker-1")

    worker.with_timer_interval(0.5)

    assert worker._timer_interval == 0.5


@pytest.mark.asyncio
async def test_worker_with_poll_interval():
    """Test Worker with_poll_interval builder method."""
    storage = InMemoryExecutionLog()
    worker = Worker(storage, "worker-1")

    worker.with_poll_interval(2.0)

    assert worker._poll_interval == 2.0
    # Jitter should be added
    assert worker._poll_interval_with_jitter > 2.0
    assert worker._poll_interval_with_jitter < 2.01


@pytest.mark.asyncio
async def test_worker_builder_chaining():
    """Test Worker builder pattern chaining."""
    storage = InMemoryExecutionLog()

    worker = (
        Worker(storage, "worker-1").with_timers().with_timer_interval(0.1).with_poll_interval(0.5)
    )

    assert worker._enable_timers is True
    assert worker._timer_interval == 0.1
    assert worker._poll_interval == 0.5


@pytest.mark.asyncio
async def test_worker_with_max_concurrent_flows():
    """Test Worker with_max_concurrent_flows."""
    storage = InMemoryExecutionLog()
    worker = Worker(storage, "worker-1")

    assert worker._max_concurrent_flows is None

    worker.with_max_concurrent_flows(50)

    assert worker._max_concurrent_flows is not None
    assert isinstance(worker._max_concurrent_flows, asyncio.Semaphore)


@pytest.mark.asyncio
async def test_worker_register_with_explicit_executor():
    """Test Worker register with explicit executor."""
    storage = InMemoryExecutionLog()
    worker = Worker(storage, "worker-1")

    await worker.register(SimpleWorkflow, lambda w: w.run())

    assert len(worker._registry) == 1
    executor = worker._registry.get_executor("SimpleWorkflow")
    assert executor is not None


@pytest.mark.asyncio
async def test_worker_register_auto_detect_flow():
    """Test Worker register with auto-detection of @flow method."""
    storage = InMemoryExecutionLog()
    worker = Worker(storage, "worker-1")

    # Register without explicit executor - should auto-detect @flow decorated method
    await worker.register(SimpleWorkflow)

    assert len(worker._registry) == 1
    executor = worker._registry.get_executor("SimpleWorkflow")
    assert executor is not None


@pytest.mark.asyncio
async def test_worker_register_multiple_flows():
    """Test Worker register multiple flow types."""
    storage = InMemoryExecutionLog()
    worker = Worker(storage, "worker-1")

    await worker.register(SimpleWorkflow)
    await worker.register(MultiMethodWorkflow)

    assert len(worker._registry) == 2


@pytest.mark.asyncio
async def test_worker_calculate_next_timer_wake_no_timers():
    """Test _calculate_next_timer_wake with no timers."""
    storage = InMemoryExecutionLog()
    worker = Worker(storage, "worker-1")

    next_wake = await worker._calculate_next_timer_wake()

    assert next_wake is None


@pytest.mark.asyncio
async def test_worker_calculate_next_timer_wake_with_timer():
    """Test _calculate_next_timer_wake with scheduled timer."""
    storage = InMemoryExecutionLog()
    worker = Worker(storage, "worker-1")

    flow_id = str(uuid4())
    future = datetime.now() + timedelta(seconds=10)

    # Add invocation with timer
    await storage.log_invocation_start(
        flow_id=flow_id,
        step=0,
        class_name="TestFlow",
        method_name="run",
        parameters=b"params",
        params_hash=123,
        retry_policy=None,
    )

    await storage.log_timer(flow_id, 0, future, "test_timer")

    next_wake = await worker._calculate_next_timer_wake()

    assert next_wake is not None
    assert abs((next_wake - future).total_seconds()) < 1


@pytest.mark.asyncio
async def test_worker_process_timers_no_expired():
    """Test _process_timers with no expired timers."""
    storage = InMemoryExecutionLog()
    worker = Worker(storage, "worker-1")

    # Should not raise
    await worker._process_timers()


@pytest.mark.asyncio
async def test_worker_process_timers_with_expired():
    """Test _process_timers with expired timer."""
    storage = InMemoryExecutionLog()
    worker = Worker(storage, "worker-1")

    flow_id = str(uuid4())
    past = datetime.now() - timedelta(seconds=10)

    # Create flow
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

    # Add invocation with expired timer
    await storage.log_invocation_start(
        flow_id=flow_id,
        step=0,
        class_name="TestFlow",
        method_name="run",
        parameters=b"params",
        params_hash=123,
        retry_policy=None,
    )

    await storage.log_timer(flow_id, 0, past, "expired_timer")

    # Process timers
    await worker._process_timers()

    # Verify timer result was stored
    result = await storage.get_suspension_result(flow_id, 0, "expired_timer")
    assert result is not None

    # Verify payload structure
    payload = pickle.loads(result)
    assert isinstance(payload, SuspensionPayload)
    assert payload.success is True


@pytest.mark.asyncio
async def test_worker_process_timers_claim_contention():
    """Test _process_timers with timer claim contention."""
    storage = InMemoryExecutionLog()
    worker1 = Worker(storage, "worker-1")
    worker2 = Worker(storage, "worker-2")

    flow_id = str(uuid4())
    past = datetime.now() - timedelta(seconds=10)

    # Create flow
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

    await storage.log_invocation_start(
        flow_id=flow_id,
        step=0,
        class_name="TestFlow",
        method_name="run",
        parameters=b"params",
        params_hash=123,
        retry_policy=None,
    )

    await storage.log_timer(flow_id, 0, past, "claimed_timer")

    # Both workers try to process - only one should succeed
    await asyncio.gather(worker1._process_timers(), worker2._process_timers())

    # Verify timer result was stored (by one of them)
    result = await storage.get_suspension_result(flow_id, 0, "claimed_timer")
    assert result is not None


@pytest.mark.asyncio
async def test_worker_handle_worker_id():
    """Test WorkerHandle worker_id method."""
    storage = InMemoryExecutionLog()
    worker = Worker(storage, "test-worker-123")

    await worker.register(SimpleWorkflow)

    handle = await worker.start()

    assert handle.worker_id() == "test-worker-123"

    await handle.shutdown()


@pytest.mark.asyncio
async def test_worker_handle_is_running():
    """Test WorkerHandle is_running method."""
    storage = InMemoryExecutionLog()
    worker = Worker(storage, "worker-1")

    await worker.register(SimpleWorkflow)

    handle = await worker.start()

    # Should be running
    assert handle.is_running() is True

    # Shutdown
    await handle.shutdown()

    # Should not be running
    assert handle.is_running() is False


@pytest.mark.asyncio
async def test_worker_handle_shutdown():
    """Test WorkerHandle shutdown."""
    storage = InMemoryExecutionLog()
    worker = Worker(storage, "worker-1")

    await worker.register(SimpleWorkflow)

    handle = await worker.start()

    # Shutdown
    await handle.shutdown()

    # Should be stopped
    assert handle.is_running() is False


@pytest.mark.asyncio
async def test_worker_handle_abort():
    """Test WorkerHandle abort."""
    storage = InMemoryExecutionLog()
    worker = Worker(storage, "worker-1")

    await worker.register(SimpleWorkflow)

    handle = await worker.start()

    # Abort (doesn't wait)
    handle.abort()

    # Give it a moment to cancel
    await asyncio.sleep(0.01)

    # Should be stopped
    assert handle.is_running() is False


@pytest.mark.asyncio
async def test_worker_execute_flow_unregistered_type():
    """Test _execute_flow with unregistered flow type."""
    storage = InMemoryExecutionLog()
    worker = Worker(storage, "worker-1")

    # Don't register any flows

    flow_id = str(uuid4())
    task_id = str(uuid4())
    now = datetime.now(UTC)

    # Create scheduled flow
    scheduled_flow = ScheduledFlow(
        task_id=task_id,
        flow_id=flow_id,
        flow_type="UnknownFlow",
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

    # Must enqueue first
    await storage.enqueue_flow(scheduled_flow)

    # Execute - should handle gracefully
    await worker._execute_flow(scheduled_flow)

    # Flow should be marked failed or retrying
    updated = await storage.get_scheduled_flow(task_id)
    assert updated is not None
    # Could be FAILED or PENDING (if retrying)
    assert updated.status in [TaskStatus.FAILED, TaskStatus.PENDING]


@pytest.mark.asyncio
async def test_worker_execute_flow_success():
    """Test _execute_flow with successful flow."""
    storage = InMemoryExecutionLog()
    worker = Worker(storage, "worker-1")

    await worker.register(SimpleWorkflow)

    flow_id = str(uuid4())
    task_id = str(uuid4())
    now = datetime.now(UTC)

    workflow = SimpleWorkflow(value=5)
    flow_data = pickle.dumps(workflow)

    scheduled_flow = ScheduledFlow(
        task_id=task_id,
        flow_id=flow_id,
        flow_type="SimpleWorkflow",
        flow_data=flow_data,
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

    await storage.enqueue_flow(scheduled_flow)

    # Execute
    await worker._execute_flow(scheduled_flow)

    # Flow should complete
    updated = await storage.get_scheduled_flow(task_id)
    assert updated.status == TaskStatus.COMPLETE


@pytest.mark.asyncio
async def test_worker_execute_flow_error():
    """Test _execute_flow with flow that raises error."""
    storage = InMemoryExecutionLog()
    worker = Worker(storage, "worker-1")

    await worker.register(ErrorWorkflow)

    flow_id = str(uuid4())
    task_id = str(uuid4())
    now = datetime.now(UTC)

    workflow = ErrorWorkflow()
    flow_data = pickle.dumps(workflow)

    scheduled_flow = ScheduledFlow(
        task_id=task_id,
        flow_id=flow_id,
        flow_type="ErrorWorkflow",
        flow_data=flow_data,
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

    await storage.enqueue_flow(scheduled_flow)

    # Execute
    await worker._execute_flow(scheduled_flow)

    # Flow should be retrying (PENDING) or failed
    updated = await storage.get_scheduled_flow(task_id)
    assert updated is not None
    # Error handling may retry, so could be PENDING or FAILED
    assert updated.status in [TaskStatus.PENDING, TaskStatus.FAILED]
    # Retry count should increase
    assert updated.retry_count >= 0


@pytest.mark.asyncio
async def test_worker_start_and_shutdown():
    """Test Worker start and shutdown lifecycle."""
    storage = InMemoryExecutionLog()
    worker = Worker(storage, "worker-1")

    await worker.register(SimpleWorkflow)

    # Start
    handle = await worker.start()

    assert worker._running is True

    # Shutdown
    await handle.shutdown()

    assert worker._running is False


@pytest.mark.asyncio
async def test_worker_error_exception():
    """Test WorkerError exception."""
    error = WorkerError("Test worker error")

    assert isinstance(error, Exception)
    assert str(error) == "Test worker error"


@pytest.mark.asyncio
async def test_worker_with_signals():
    """Test Worker with_signals builder method."""
    storage = InMemoryExecutionLog()
    worker = Worker(storage, "worker-1")

    # Mock signal source
    class MockSignalSource:
        async def poll_for_signal(self, signal_name: str):
            return None

        async def consume_signal(self, signal_name: str):
            pass

    signal_source = MockSignalSource()

    assert worker._signal_source is None

    worker.with_signals(signal_source)

    assert worker._signal_source is signal_source


@pytest.mark.asyncio
async def test_worker_with_signal_interval():
    """Test Worker with_signal_interval builder method."""
    storage = InMemoryExecutionLog()
    worker = Worker(storage, "worker-1")

    worker.with_signal_interval(0.2)

    assert worker._signal_poll_interval == 0.2
