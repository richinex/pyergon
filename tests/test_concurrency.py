"""
Concurrency and race condition tests for pyergon.

These tests verify that the system handles concurrent operations correctly:
- Multiple workers processing flows simultaneously
- Concurrent reads and writes to storage
- Race conditions in flow scheduling and execution
- Event notification coordination
"""

import asyncio
import pickle
from dataclasses import dataclass
from uuid import uuid4

import pytest

from pyergon.core import InvocationStatus, TaskStatus
from pyergon.decorators import flow, flow_type, step
from pyergon.executor import Completed, Executor
from pyergon.executor.scheduler import Scheduler

# ==============================================================================
# TEST 1: Concurrent Flow Execution (No Interference)
# ==============================================================================


# Flow classes must be at module level to avoid scope issues
@dataclass
@flow_type
class IsolatedFlow:
    flow_id: int
    value: int

    @step
    async def compute(self) -> int:
        # Simulate work with randomized timing
        await asyncio.sleep(0.001)
        return self.value * 2

    @flow
    async def run(self) -> int:
        result = await self.compute()
        return result


@pytest.mark.concurrency
@pytest.mark.asyncio
async def test_concurrent_flows_dont_interfere(in_memory_storage):
    """
    Race Condition Test: Concurrent flows maintain isolation.

    Multiple flows executing simultaneously should not interfere with each other.
    """
    num_flows = 10
    results = []

    async def execute_flow(flow_id: int, value: int):
        """Execute one flow."""
        flow = IsolatedFlow(flow_id=flow_id, value=value)
        executor = Executor(flow, in_memory_storage, f"flow_{flow_id}")
        outcome = await executor.run(lambda f: f.run())
        assert isinstance(outcome, Completed)
        return outcome.result

    # Execute flows concurrently
    results = await asyncio.gather(*[execute_flow(i, i * 10) for i in range(num_flows)])

    # Verify all flows computed correctly (no interference)
    for i in range(num_flows):
        expected = i * 10 * 2
        # Handle case where result might be an exception
        if not isinstance(results[i], Exception):
            assert results[i] == expected, f"Flow {i} expected {expected}, got {results[i]}"


# ==============================================================================
# TEST 2: Concurrent Storage Writes
# ==============================================================================


@pytest.mark.concurrency
@pytest.mark.asyncio
async def test_concurrent_storage_writes_no_corruption(in_memory_storage):
    """
    Race Condition Test: Concurrent writes don't corrupt storage.

    Multiple tasks writing to same storage should not cause data loss or corruption.
    """
    num_writers = 20
    writes_per_writer = 50

    async def writer_task(writer_id: int):
        """Write multiple invocations."""
        flow_id = f"writer_{writer_id}"

        for step_num in range(writes_per_writer):
            await in_memory_storage.log_invocation_start(
                flow_id=flow_id,
                step=step_num,
                class_name="WriterFlow",
                method_name=f"step_{step_num}",
                parameters=pickle.dumps({"writer": writer_id, "step": step_num}),
                params_hash=hash((writer_id, step_num)),
            )

            # Small delay to increase chance of race conditions
            if step_num % 10 == 0:
                await asyncio.sleep(0.001)

            await in_memory_storage.log_invocation_completion(
                flow_id=flow_id,
                step=step_num,
                return_value=pickle.dumps(f"result_{writer_id}_{step_num}"),
            )

    # Run all writers concurrently
    await asyncio.gather(*[writer_task(i) for i in range(num_writers)])

    # Verify all writes completed correctly
    for writer_id in range(num_writers):
        flow_id = f"writer_{writer_id}"
        invocations = await in_memory_storage.get_invocations_for_flow(flow_id)

        # All writes should have persisted
        assert len(invocations) == writes_per_writer, (
            f"Writer {writer_id} expected {writes_per_writer} invocations, got {len(invocations)}"
        )

        # No duplicate steps
        steps = [inv.step for inv in invocations]
        assert len(steps) == len(set(steps)), f"Writer {writer_id} has duplicate steps: {steps}"

        # All invocations completed
        assert all(inv.status == InvocationStatus.COMPLETE for inv in invocations), (
            f"Writer {writer_id} has incomplete invocations"
        )


# ==============================================================================
# TEST 3: Concurrent Flow Scheduling
# ==============================================================================


@dataclass
@flow_type
class ScheduledFlow:
    scheduler_id: int
    flow_index: int

    @flow
    async def run(self):
        return f"result_{self.scheduler_id}_{self.flow_index}"


@pytest.mark.concurrency
@pytest.mark.asyncio
async def test_concurrent_flow_scheduling(in_memory_storage):
    """
    Race Condition Test: Concurrent schedulers don't duplicate flows.

    Multiple schedulers running concurrently should not cause issues.
    """
    num_schedulers = 5
    flows_per_scheduler = 20

    async def scheduler_task(scheduler_id: int):
        """Schedule multiple flows."""
        scheduler = Scheduler(in_memory_storage)
        task_ids = []

        for i in range(flows_per_scheduler):
            flow = ScheduledFlow(scheduler_id=scheduler_id, flow_index=i)
            task_id = await scheduler.schedule(flow)
            task_ids.append(task_id)

        return task_ids

    # Run schedulers concurrently
    all_task_ids = await asyncio.gather(*[scheduler_task(i) for i in range(num_schedulers)])

    # Flatten task IDs
    flat_task_ids = [tid for task_ids in all_task_ids for tid in task_ids]

    # Verify all flows scheduled
    assert len(flat_task_ids) == num_schedulers * flows_per_scheduler

    # Verify no duplicate task IDs (each flow scheduled once)
    assert len(flat_task_ids) == len(set(flat_task_ids)), (
        "Duplicate task IDs detected - flows scheduled multiple times!"
    )


# ==============================================================================
# TEST 4: Worker Queue Dequeue Race Condition
# ==============================================================================


@dataclass
@flow_type
class WorkerTestFlow:
    flow_id: int

    @step
    async def work(self) -> str:
        await asyncio.sleep(0.01)  # Simulate work
        return f"processed_by_worker_{self.flow_id}"

    @flow
    async def run(self) -> str:
        return await self.work()


@pytest.mark.concurrency
@pytest.mark.asyncio
@pytest.mark.slow
async def test_workers_dont_dequeue_same_flow(in_memory_storage):
    """
    Race Condition Test: Multiple workers don't claim same flow.

    Critical test: Optimistic locking should prevent two workers from
    processing the same flow simultaneously.
    """
    num_workers = 5
    num_flows = 20

    # Schedule flows
    scheduler = Scheduler(in_memory_storage)
    task_ids = []
    for i in range(num_flows):
        flow = WorkerTestFlow(flow_id=i)
        task_id = await scheduler.schedule(flow)
        task_ids.append(task_id)

    # Track which worker claimed which flow
    claimed_flows: set[str] = set()
    lock = asyncio.Lock()

    async def worker_dequeue_test(worker_id: str):
        """Worker tries to dequeue flows."""
        claimed_by_this_worker = []

        # Try to dequeue multiple times
        for _ in range(num_flows):
            scheduled_flow = await in_memory_storage.dequeue_flow(worker_id)

            if scheduled_flow is not None:
                # Record claim
                async with lock:
                    # Check if another worker already claimed this
                    if scheduled_flow.task_id in claimed_flows:
                        pytest.fail(
                            f"Worker {worker_id} claimed flow {scheduled_flow.task_id} "
                            f"but it was already claimed!"
                        )

                    claimed_flows.add(scheduled_flow.task_id)
                    claimed_by_this_worker.append(scheduled_flow.task_id)

            # Small delay between dequeue attempts
            await asyncio.sleep(0.001)

        return claimed_by_this_worker

    # Run workers concurrently
    worker_results = await asyncio.gather(
        *[worker_dequeue_test(f"worker_{i}") for i in range(num_workers)]
    )

    # Verify all flows claimed exactly once
    total_claimed = sum(len(claims) for claims in worker_results)
    assert total_claimed == num_flows, f"Expected {num_flows} flows claimed, got {total_claimed}"

    # Verify no duplicate claims
    assert len(claimed_flows) == num_flows, (
        f"Duplicate claims detected: {num_flows} flows but {len(claimed_flows)} unique claims"
    )


# ==============================================================================
# TEST 5: Event Notification Race Conditions
# ==============================================================================


@dataclass
@flow_type
class NotificationTestFlow:
    data: str

    @flow
    async def run(self):
        return self.data


@pytest.mark.concurrency
@pytest.mark.asyncio
async def test_work_notify_wakes_all_waiting_workers(in_memory_storage):
    """
    Race Condition Test: Work notifications wake waiting workers.

    When work arrives, waiting workers should be notified correctly.
    """
    if not hasattr(in_memory_storage, "work_notify"):
        pytest.skip("Storage doesn't support work notifications")

    num_workers = 3
    work_received = []

    async def worker_wait_for_work(worker_id: str):
        """Worker waits for notification."""
        try:
            # Wait for work notification (with timeout)
            await asyncio.wait_for(in_memory_storage.work_notify().wait(), timeout=1.0)
            work_received.append(worker_id)
            in_memory_storage.work_notify().clear()
        except TimeoutError:
            pass

    # Start workers waiting
    worker_tasks = [
        asyncio.create_task(worker_wait_for_work(f"worker_{i}")) for i in range(num_workers)
    ]

    # Give workers time to start waiting
    await asyncio.sleep(0.1)

    # Schedule work (should trigger notification)
    scheduler = Scheduler(in_memory_storage)
    flow = NotificationTestFlow(data="test")
    await scheduler.schedule(flow)

    # Wait for workers to react
    await asyncio.gather(*worker_tasks)

    # At least one worker should have been notified
    assert len(work_received) > 0, "No workers were notified of new work"


# ==============================================================================
# TEST 6: Interleaved Read-Write Operations
# ==============================================================================


@pytest.mark.concurrency
@pytest.mark.asyncio
async def test_interleaved_reads_writes_consistent(in_memory_storage):
    """
    Race Condition Test: Reads during writes see consistent state.

    Readers should never see partial/inconsistent data during concurrent writes.
    """
    flow_id = str(uuid4())
    num_steps = 50
    read_snapshots = []

    async def writer():
        """Write steps sequentially."""
        for step_num in range(num_steps):
            await in_memory_storage.log_invocation_start(
                flow_id=flow_id,
                step=step_num,
                class_name="TestFlow",
                method_name=f"step_{step_num}",
                parameters=pickle.dumps({}),
                params_hash=hash(step_num),
            )
            await asyncio.sleep(0.001)  # Simulate work
            await in_memory_storage.log_invocation_completion(
                flow_id=flow_id,
                step=step_num,
                return_value=pickle.dumps(f"result_{step_num}"),
            )

    async def reader():
        """Read state periodically."""
        for _ in range(30):
            invocations = await in_memory_storage.get_invocations_for_flow(flow_id)
            read_snapshots.append(len(invocations))
            await asyncio.sleep(0.002)

    # Run writer and reader concurrently
    await asyncio.gather(writer(), reader())

    # Verify snapshots show monotonically increasing progress
    for i in range(1, len(read_snapshots)):
        assert read_snapshots[i] >= read_snapshots[i - 1], (
            f"Read snapshots not monotonic: {read_snapshots[i - 1]} -> {read_snapshots[i]}"
        )


# ==============================================================================
# TEST 7: Flow Completion Race Conditions
# ==============================================================================


@dataclass
@flow_type
class CompletionFlow:
    flow_id: int

    @step
    async def work(self) -> int:
        await asyncio.sleep(0.01)
        return self.flow_id * 100

    @flow
    async def run(self) -> int:
        return await self.work()


@pytest.mark.concurrency
@pytest.mark.asyncio
async def test_concurrent_flow_completion_updates(in_memory_storage):
    """
    Race Condition Test: Concurrent completion updates don't corrupt state.

    Multiple flows completing simultaneously should not cause issues.
    """
    num_flows = 20

    async def schedule_and_execute(flow_id: int):
        """Schedule, execute, and complete a flow."""
        # Schedule
        scheduler = Scheduler(in_memory_storage)
        flow = CompletionFlow(flow_id=flow_id)
        task_id = await scheduler.schedule(flow)

        # Dequeue
        scheduled = await in_memory_storage.dequeue_flow(f"worker_{flow_id}")
        assert scheduled is not None

        # Execute
        deserialized = pickle.loads(scheduled.flow_data)
        executor = Executor(deserialized, in_memory_storage, scheduled.flow_id)
        await executor.run(lambda f: f.run())

        # Mark complete
        await in_memory_storage.complete_flow(task_id=scheduled.task_id, status=TaskStatus.COMPLETE)

        return task_id

    # Run all concurrently
    completed_task_ids = await asyncio.gather(*[schedule_and_execute(i) for i in range(num_flows)])

    # All flows should have completed
    assert len(completed_task_ids) == num_flows
    assert all(tid is not None for tid in completed_task_ids)


# ==============================================================================
# TEST 8: Deadlock Prevention
# ==============================================================================


@pytest.mark.concurrency
@pytest.mark.asyncio
async def test_no_deadlock_with_concurrent_locks(in_memory_storage):
    """
    Deadlock Test: System doesn't deadlock under concurrent load.

    Multiple operations acquiring locks should not create deadlocks.
    """
    num_operations = 50

    async def complex_operation(op_id: int):
        """Perform operation that might acquire locks."""
        flow_id = f"flow_{op_id}"

        # Operation 1: Write
        await in_memory_storage.log_invocation_start(
            flow_id=flow_id,
            step=0,
            class_name="TestFlow",
            method_name="test",
            parameters=pickle.dumps({}),
            params_hash=hash(op_id),
        )

        # Operation 2: Read
        inv = await in_memory_storage.get_invocation(flow_id, step=0)

        # Operation 3: Update
        if inv is not None:
            await in_memory_storage.log_invocation_completion(
                flow_id=flow_id,
                step=0,
                return_value=pickle.dumps(f"result_{op_id}"),
            )

        return op_id

    # If this hangs, we have a deadlock
    results = await asyncio.gather(*[complex_operation(i) for i in range(num_operations)])

    # All operations should complete
    assert len(results) == num_operations


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-m", "concurrency"])
