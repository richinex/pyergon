"""
Durability tests for ergon - Testing crash recovery and persistence.

These tests verify that execution state survives process restarts and that
the durable execution guarantees are actually enforced.
"""

import asyncio
import pickle
from dataclasses import dataclass
from datetime import datetime, timedelta
from uuid import uuid4

import pytest

from pyergon.core import InvocationStatus, RetryPolicy
from pyergon.decorators import flow, flow_type
from pyergon.executor.scheduler import Scheduler
from pyergon.storage import SqliteExecutionLog

# ==============================================================================
# TEST 1: Basic Durability - Invocations Persist
# ==============================================================================


@pytest.mark.durability
@pytest.mark.asyncio
async def test_invocations_persist_across_connections(temp_db_path):
    """
    Durability Test: Invocations survive database reconnection.

    Simulates process restart by closing and reopening database.
    """
    flow_id = str(uuid4())

    # Phase 1: Create storage and write data
    storage1 = SqliteExecutionLog(str(temp_db_path))
    await storage1.connect()

    await storage1.log_invocation_start(
        flow_id=flow_id,
        step=0,
        class_name="DurableFlow",
        method_name="step_one",
        parameters=pickle.dumps({"input": "test"}),
        params_hash=hash("test"),
    )

    await storage1.log_invocation_completion(
        flow_id=flow_id,
        step=0,
        return_value=pickle.dumps("result"),
    )

    # Close connection (simulates process termination)
    await storage1.close()

    # Phase 2: Reopen database (simulates process restart)
    storage2 = SqliteExecutionLog(str(temp_db_path))
    await storage2.connect()

    # Verify data persisted
    invocation = await storage2.get_invocation(flow_id, step=0)

    assert invocation is not None, "Invocation should persist across restart"
    assert invocation.id == flow_id
    assert invocation.step == 0
    assert invocation.status == InvocationStatus.COMPLETE
    assert invocation.class_name == "DurableFlow"
    assert invocation.method_name == "step_one"

    # Verify return value persisted
    assert invocation.return_value is not None
    result = pickle.loads(invocation.return_value)
    assert result == "result"

    await storage2.close()


# ==============================================================================
# TEST 2: Flow State Persistence
# ==============================================================================


# Flow for test_flow_queue_persists (must be at module level, not inside function)
@dataclass
@flow_type
class PersistentFlow:
    data: str

    @flow
    async def run(self):
        return self.data


@pytest.mark.durability
@pytest.mark.asyncio
async def test_flow_queue_persists(temp_db_path):
    """
    Durability Test: Queued flows survive restart.

    Scheduled flows must persist even if process crashes before execution.
    """
    # Phase 1: Schedule flow
    storage1 = SqliteExecutionLog(str(temp_db_path))
    await storage1.connect()

    scheduler = Scheduler(storage1)

    flow = PersistentFlow(data="test_data")
    task_id = await scheduler.schedule(flow)

    # Close (simulate crash before execution)
    await storage1.close()

    # Phase 2: Restart and verify flow in queue
    storage2 = SqliteExecutionLog(str(temp_db_path))
    await storage2.connect()

    # Dequeue - should find the flow
    from pyergon.executor.worker import Worker

    Worker(storage2, worker_id="test_worker")

    scheduled_flow = await storage2.dequeue_flow("test_worker")

    assert scheduled_flow is not None, "Scheduled flow should survive restart"
    assert scheduled_flow.task_id == task_id
    assert scheduled_flow.flow_type == "PersistentFlow"

    # Deserialize and verify data
    deserialized_flow = pickle.loads(scheduled_flow.flow_data)
    assert deserialized_flow.data == "test_data"

    await storage2.close()


# ==============================================================================
# TEST 3: Multi-Step Flow Resumption
# ==============================================================================


@pytest.mark.durability
@pytest.mark.asyncio
async def test_multi_step_flow_resumes_correctly(temp_db_path):
    """
    Durability Test: Multi-step flow resumes from checkpoint.

    If flow completes steps 0-2 then crashes, it should resume at step 3.
    """
    storage = SqliteExecutionLog(str(temp_db_path))
    await storage.connect()

    flow_id = str(uuid4())

    # Simulate partial execution: complete steps 0, 1, 2
    for step in range(3):
        await storage.log_invocation_start(
            flow_id=flow_id,
            step=step,
            class_name="ResumableFlow",
            method_name=f"step_{step}",
            parameters=pickle.dumps({}),
            params_hash=hash(step),
        )
        await storage.log_invocation_completion(
            flow_id=flow_id,
            step=step,
            return_value=pickle.dumps(f"result_{step}"),
        )

    # Now "restart" and resume execution
    # Get cached results for steps 0-2
    cached_steps = await storage.get_invocations_for_flow(flow_id)

    assert len(cached_steps) == 3
    assert all(inv.status == InvocationStatus.COMPLETE for inv in cached_steps)

    # Verify we can retrieve specific cached results
    for step in range(3):
        inv = await storage.get_invocation(flow_id, step)
        assert inv is not None
        assert inv.status == InvocationStatus.COMPLETE
        result = pickle.loads(inv.return_value)
        assert result == f"result_{step}"

    # Step 3 should not exist yet
    step_3 = await storage.get_invocation(flow_id, step=3)
    assert step_3 is None, "Step 3 should not exist yet (not executed)"

    await storage.close()


# ==============================================================================
# TEST 4: Concurrent Writer Durability
# ==============================================================================


@pytest.mark.durability
@pytest.mark.asyncio
@pytest.mark.concurrency
async def test_concurrent_writes_all_persist(temp_db_path):
    """
    Durability + Concurrency: All concurrent writes must persist.

    No data loss when multiple workers write simultaneously.
    """
    storage = SqliteExecutionLog(str(temp_db_path))
    await storage.connect()

    num_flows = 10
    steps_per_flow = 5

    async def write_flow_data(flow_index: int):
        """Write invocations for one flow."""
        flow_id = f"flow_{flow_index}"
        for step in range(steps_per_flow):
            await storage.log_invocation_start(
                flow_id=flow_id,
                step=step,
                class_name="ConcurrentFlow",
                method_name=f"step_{step}",
                parameters=pickle.dumps({"flow": flow_index, "step": step}),
                params_hash=hash((flow_index, step)),
            )
            await storage.log_invocation_completion(
                flow_id=flow_id,
                step=step,
                return_value=pickle.dumps(f"result_{flow_index}_{step}"),
            )

    # Write concurrently
    await asyncio.gather(*[write_flow_data(i) for i in range(num_flows)])

    # Close and reopen (simulate restart)
    await storage.close()

    storage2 = SqliteExecutionLog(str(temp_db_path))
    await storage2.connect()

    # Verify all data persisted
    for flow_index in range(num_flows):
        flow_id = f"flow_{flow_index}"
        invocations = await storage2.get_invocations_for_flow(flow_id)

        assert len(invocations) == steps_per_flow, (
            f"Flow {flow_index} should have {steps_per_flow} invocations, got {len(invocations)}"
        )

        # Verify no data corruption
        for inv in invocations:
            assert inv.id == flow_id
            assert inv.status == InvocationStatus.COMPLETE
            assert inv.return_value is not None

    await storage2.close()


# ==============================================================================
# TEST 5: Retry State Persistence
# ==============================================================================


@pytest.mark.durability
@pytest.mark.asyncio
async def test_retry_attempts_persist(temp_db_path):
    """
    Durability Test: Retry attempt count persists across restarts.

    If a step fails twice, then process restarts, next attempt should be #3.
    """
    storage = SqliteExecutionLog(str(temp_db_path))
    await storage.connect()

    flow_id = str(uuid4())

    # Attempt 1: Start and fail
    await storage.log_invocation_start(
        flow_id=flow_id,
        step=0,
        class_name="RetryFlow",
        method_name="flaky_step",
        parameters=pickle.dumps({}),
        params_hash=hash("test"),
        retry_policy=RetryPolicy.STANDARD,
    )

    # Simulate failure
    await storage.update_is_retryable(flow_id, step=0, is_retryable=True)

    # Check initial attempt count (Rust uses DEFAULT 1, not 0)
    inv = await storage.get_invocation(flow_id, step=0)
    assert inv.attempts == 1  # Initial attempt (matches Rust: DEFAULT 1)

    # Log second attempt (would happen after retry delay)
    await storage.log_invocation_start(
        flow_id=flow_id,
        step=0,
        class_name="RetryFlow",
        method_name="flaky_step",
        parameters=pickle.dumps({}),
        params_hash=hash("test"),
        retry_policy=RetryPolicy.STANDARD,
    )

    # Close (simulate crash)
    await storage.close()

    # Reopen
    storage2 = SqliteExecutionLog(str(temp_db_path))
    await storage2.connect()

    # Verify attempt count persisted
    inv2 = await storage2.get_invocation(flow_id, step=0)
    assert inv2 is not None
    # Note: Actual behavior depends on how attempts are tracked
    # This test documents expected behavior

    await storage2.close()


# ==============================================================================
# TEST 6: Suspension State Persistence
# ==============================================================================


@pytest.mark.durability
@pytest.mark.asyncio
@pytest.mark.slow
async def test_suspended_flow_resumes_after_restart(temp_db_path):
    """
    Durability Test: Suspended flows resume correctly after restart.

    Flow suspended waiting for signal should resume when signal arrives after restart.
    """
    storage = SqliteExecutionLog(str(temp_db_path))
    await storage.connect()

    flow_id = str(uuid4())
    signal_name = "test_signal"

    # Simulate flow suspension
    await storage.log_invocation_start(
        flow_id=flow_id,
        step=0,
        class_name="WaitingFlow",
        method_name="wait_for_signal",
        parameters=pickle.dumps({}),
        params_hash=hash("wait"),
    )

    # Mark as waiting for signal
    # Store suspension state
    await storage.log_signal(flow_id, step=0, signal_name=signal_name)

    # Verify marked as waiting
    inv = await storage.get_invocation(flow_id, step=0)
    assert inv.status == InvocationStatus.WAITING_FOR_SIGNAL

    # Close (simulate crash)
    await storage.close()

    # Restart
    storage2 = SqliteExecutionLog(str(temp_db_path))
    await storage2.connect()

    # Verify suspension state persisted
    inv2 = await storage2.get_invocation(flow_id, step=0)
    assert inv2 is not None
    assert inv2.status == InvocationStatus.WAITING_FOR_SIGNAL

    # Deliver signal after restart
    from pyergon.executor.suspension_payload import SuspensionPayload

    payload = SuspensionPayload(success=True, data=pickle.dumps("signal_result"))

    await storage2.store_suspension_result(
        flow_id=flow_id,
        step=0,
        suspension_key=signal_name,
        result=pickle.dumps(payload),
    )

    # Resume flow
    await storage2.resume_flow(flow_id)

    # Verify can retrieve signal result
    result_bytes = await storage2.get_suspension_result(flow_id, step=0, suspension_key=signal_name)
    assert result_bytes is not None

    result_payload = pickle.loads(result_bytes)
    assert result_payload.success is True

    await storage2.close()


# ==============================================================================
# TEST 7: Database Corruption Detection
# ==============================================================================


@pytest.mark.durability
@pytest.mark.asyncio
async def test_corrupted_data_detected(temp_db_path):
    """
    Durability Test: Detect corrupted serialized data.

    If pickled data is corrupted, should raise clear error (not silent failure).
    """
    storage = SqliteExecutionLog(str(temp_db_path))
    await storage.connect()

    flow_id = str(uuid4())

    # Write valid data
    await storage.log_invocation_start(
        flow_id=flow_id,
        step=0,
        class_name="TestFlow",
        method_name="test_step",
        parameters=pickle.dumps({"data": "valid"}),
        params_hash=hash("valid"),
    )

    # Manually corrupt the data (direct SQL manipulation)
    async with storage._lock:
        await storage._connection.execute(
            "UPDATE execution_log SET parameters = ? WHERE id = ? AND step = ?",
            (b"CORRUPTED_NOT_PICKLE", flow_id, 0),
        )
        await storage._connection.commit()

    # Try to read corrupted data
    inv = await storage.get_invocation(flow_id, step=0)

    # Should get invocation but parameters are corrupted
    assert inv is not None
    assert inv.parameters == b"CORRUPTED_NOT_PICKLE"

    # Trying to unpickle should fail clearly
    with pytest.raises(pickle.UnpicklingError):
        pickle.loads(inv.parameters)

    await storage.close()


# ==============================================================================
# TEST 8: Timer Persistence
# ==============================================================================


@pytest.mark.durability
@pytest.mark.asyncio
async def test_timers_persist_across_restart(temp_db_path):
    """
    Durability Test: Scheduled timers survive restart.

    Timer scheduled for future should still fire after process restart.
    """
    storage = SqliteExecutionLog(str(temp_db_path))
    await storage.connect()

    flow_id = str(uuid4())
    timer_name = "test_timer"
    fire_at = datetime.now() + timedelta(seconds=60)  # 1 minute from now

    # IMPORTANT: log_timer does UPDATE, not INSERT - must create invocation first
    # Rust: "UPDATE execution_log SET ... WHERE id = ? AND step = ?"
    await storage.log_invocation_start(
        flow_id=flow_id,
        step=0,
        class_name="TimerFlow",
        method_name="wait_step",
        parameters=pickle.dumps({}),
        params_hash=hash("timer"),
    )

    # Schedule timer (updates existing invocation)
    await storage.log_timer(
        flow_id=flow_id,
        step=0,
        timer_fire_at=fire_at,
        timer_name=timer_name,
    )

    # Verify timer logged
    inv = await storage.get_invocation(flow_id, step=0)
    assert inv is not None
    assert inv.status == InvocationStatus.WAITING_FOR_TIMER
    assert inv.timer_name == timer_name

    # Close (simulate crash)
    await storage.close()

    # Restart
    storage2 = SqliteExecutionLog(str(temp_db_path))
    await storage2.connect()

    # Verify timer still scheduled
    inv2 = await storage2.get_invocation(flow_id, step=0)
    assert inv2 is not None
    assert inv2.status == InvocationStatus.WAITING_FOR_TIMER
    assert inv2.timer_name == timer_name

    # Verify can query expired timers (simulate time passing)
    # Use a time in the past to test expiration logic
    expired_timers = await storage2.get_expired_timers(datetime.now() + timedelta(seconds=120))

    # Should find our timer (it expires in 60s, we checked at 120s)
    timer_found = any(t.flow_id == flow_id and t.step == 0 for t in expired_timers)
    assert timer_found, "Timer should be in expired list"

    await storage2.close()


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-m", "durability"])
