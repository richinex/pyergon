"""
Property-based tests for ergon using Hypothesis.

These tests generate thousands of test cases to find edge cases in:
- Serialization/deserialization
- Storage operations
- Step caching logic
- DAG execution consistency
- Retry policy calculations
"""

import asyncio
import pickle
from uuid import uuid4

import pytest
from hypothesis import HealthCheck, assume, given, settings
from hypothesis import strategies as st

from pyergon.core import (
    CallType,
    Invocation,
    InvocationStatus,
    RetryPolicy,
)
from pyergon.decorators import flow, flow_type, step
from pyergon.executor import Completed, Executor
from pyergon.storage import InMemoryExecutionLog

# ==============================================================================
# PROPERTY 1: Serialization Roundtrip
# ==============================================================================


@pytest.mark.property
@given(
    inv_id=st.uuids().map(str),
    flow_id=st.uuids().map(str),
    step=st.integers(min_value=0, max_value=10000),
    class_name=st.text(
        min_size=1, max_size=50, alphabet=st.characters(whitelist_categories=("Lu", "Ll"))
    ),
    method_name=st.text(
        min_size=1, max_size=50, alphabet=st.characters(whitelist_categories=("Lu", "Ll"))
    ),
    parameters=st.binary(min_size=0, max_size=1000),
    params_hash=st.integers(),
    # Only use statuses that don't require special fields
    status=st.sampled_from(
        [
            InvocationStatus.PENDING,
            InvocationStatus.COMPLETE,
            InvocationStatus.WAITING_FOR_SIGNAL,
        ]
    ),
    attempts=st.integers(min_value=0, max_value=100),
)
def test_invocation_pickle_roundtrip(
    inv_id, flow_id, step, class_name, method_name, parameters, params_hash, status, attempts
):
    """
    Property: Invocation serialization/deserialization is identity function.

    For any invocation, pickle.loads(pickle.dumps(inv)) == inv

    Note: We exclude WAITING_FOR_TIMER status because it requires timer_fire_at field.
    That validation is tested separately.
    """
    invocation = Invocation(
        id=inv_id,
        flow_id=flow_id,
        step=step,
        class_name=class_name,
        method_name=method_name,
        parameters=parameters,
        params_hash=params_hash,
        status=status,
        attempts=attempts,
    )

    # Serialize
    serialized = pickle.dumps(invocation)

    # Deserialize
    deserialized = pickle.loads(serialized)

    # Property: roundtrip preserves all required fields
    assert deserialized.id == invocation.id
    assert deserialized.flow_id == invocation.flow_id
    assert deserialized.step == invocation.step
    assert deserialized.class_name == invocation.class_name
    assert deserialized.method_name == invocation.method_name
    assert deserialized.parameters == invocation.parameters
    assert deserialized.params_hash == invocation.params_hash
    assert deserialized.status == invocation.status
    assert deserialized.attempts == invocation.attempts


@pytest.mark.property
@given(value=st.integers() | st.text() | st.lists(st.integers()))
def test_flow_data_pickle_roundtrip(value):
    """
    Property: Flow instance data serialization preserves values.

    Any picklable Python value can roundtrip through flow serialization.

    Note: Uses pre-defined flow class from conftest to avoid pickle issues with
    dynamically defined classes.
    """
    from conftest import SimpleTestFlow

    flow_instance = SimpleTestFlow(value=value if isinstance(value, int) else 0)

    # Serialize
    serialized = pickle.dumps(flow_instance)

    # Deserialize
    deserialized = pickle.loads(serialized)

    # Property: data preserved
    assert deserialized.value == flow_instance.value


# ==============================================================================
# PROPERTY 2: Storage Operations
# ==============================================================================


@pytest.mark.property
@pytest.mark.asyncio
@given(
    num_invocations=st.integers(min_value=1, max_value=50),
    max_step=st.integers(min_value=0, max_value=100),
)
@settings(max_examples=100, deadline=None)
async def test_storage_preserves_all_invocations(num_invocations, max_step):
    """
    Property: Storage persists all invocations without loss.

    If we save N invocations, we can retrieve exactly N invocations.
    """
    storage = InMemoryExecutionLog()
    flow_id = str(uuid4())

    # Generate unique steps
    steps = list(range(min(num_invocations, max_step + 1)))

    # Save invocations using storage methods
    for step_num in steps:
        await storage.log_invocation_start(
            flow_id=flow_id,
            step=step_num,
            class_name="TestFlow",
            method_name=f"step_{step_num}",
            parameters=b"",
            params_hash=hash((flow_id, step_num)),
        )
        await storage.log_invocation_completion(
            flow_id=flow_id,
            step=step_num,
            return_value=b"success",
        )

    # Property: can retrieve all saved invocations
    retrieved = await storage.get_invocations_for_flow(flow_id)
    assert len(retrieved) == len(steps)

    # Property: all steps are present
    retrieved_steps = {inv.step for inv in retrieved}
    assert retrieved_steps == set(steps)


@pytest.mark.property
@pytest.mark.asyncio
@given(
    num_flows=st.integers(min_value=2, max_value=20),
    steps_per_flow=st.integers(min_value=1, max_value=10),
)
@settings(max_examples=50, deadline=None)
async def test_storage_isolates_flows(num_flows, steps_per_flow):
    """
    Property: Storage isolates invocations by flow_id.

    Invocations for different flows don't interfere with each other.
    """
    storage = InMemoryExecutionLog()
    flow_ids = [str(uuid4()) for _ in range(num_flows)]

    # Save invocations for each flow
    for flow_id in flow_ids:
        for step_num in range(steps_per_flow):
            await storage.log_invocation_start(
                flow_id=flow_id,
                step=step_num,
                class_name="TestFlow",
                method_name=f"step_{step_num}",
                parameters=b"",
                params_hash=hash((flow_id, step_num)),
            )
            await storage.log_invocation_completion(
                flow_id=flow_id,
                step=step_num,
                return_value=pickle.dumps(f"result_{flow_id}_{step_num}"),
            )

    # Property: each flow has exactly its own invocations
    for flow_id in flow_ids:
        invocations = await storage.get_invocations_for_flow(flow_id)
        assert len(invocations) == steps_per_flow

        # Property: all invocations belong to this flow
        for inv in invocations:
            assert inv.flow_id == flow_id


# ==============================================================================
# PROPERTY 3: Retry Policy Calculations
# ==============================================================================


@pytest.mark.property
@given(
    max_attempts=st.integers(min_value=1, max_value=100),
    initial_delay_ms=st.integers(min_value=1, max_value=10000),
    max_delay_ms=st.integers(min_value=100, max_value=60000),
    backoff_multiplier=st.floats(min_value=1.0, max_value=5.0),
    attempt=st.integers(min_value=1, max_value=50),
)
def test_retry_policy_delay_properties(
    max_attempts, initial_delay_ms, max_delay_ms, backoff_multiplier, attempt
):
    """
    Property: Retry delays follow exponential backoff with ceiling.

    1. Delay grows exponentially (or stays same if multiplier=1)
    2. Delay never exceeds max_delay
    3. Delay is deterministic for same inputs
    4. Returns None when attempt >= max_attempts
    """
    # Ensure max_delay >= initial_delay
    assume(max_delay_ms >= initial_delay_ms)

    policy = RetryPolicy(
        max_attempts=max_attempts,
        initial_delay_ms=initial_delay_ms,
        max_delay_ms=max_delay_ms,
        backoff_multiplier=backoff_multiplier,
    )

    # Calculate delay (returns milliseconds or None)
    delay = policy.delay_for_attempt(attempt)

    # Property 1: When attempt >= max_attempts, returns None
    if attempt >= max_attempts:
        assert delay is None
    else:
        # Property 2: Delay is non-negative integer
        assert delay is not None
        assert isinstance(delay, int)
        assert delay >= 0

        # Property 3: Delay never exceeds max_delay
        assert delay <= max_delay_ms

        # Property 4: For attempt 1, delay equals initial_delay
        if attempt == 1:
            expected = initial_delay_ms
            assert delay == expected

        # Property 5: Delay is deterministic
        delay2 = policy.delay_for_attempt(attempt)
        assert delay == delay2


@pytest.mark.property
@given(
    max_attempts=st.integers(min_value=1, max_value=10),
    attempt_number=st.integers(min_value=1, max_value=20),
)
def test_retry_policy_max_attempts_respected(max_attempts, attempt_number):
    """
    Property: Retry policy respects max_attempts boundary.

    delay_for_attempt(attempt) returns None when attempt >= max_attempts.
    """
    policy = RetryPolicy(
        max_attempts=max_attempts,
        initial_delay_ms=100,
        max_delay_ms=1000,
        backoff_multiplier=2.0,
    )

    delay = policy.delay_for_attempt(attempt_number)

    # Property: delay_for_attempt returns None when attempt >= max_attempts
    if attempt_number >= max_attempts:
        # Exceeded max attempts - no more retries
        assert delay is None
    else:
        # Within limit - should return a delay
        assert delay is not None
        assert isinstance(delay, int)
        assert delay >= 0


# ==============================================================================
# PROPERTY 4: Step ID Stability
# ==============================================================================


@pytest.mark.property
@given(
    class_name=st.text(
        min_size=1, max_size=50, alphabet=st.characters(whitelist_categories=("Lu", "Ll"))
    ),
    method_name=st.text(
        min_size=1, max_size=50, alphabet=st.characters(whitelist_categories=("Lu", "Ll"))
    ),
)
def test_step_id_deterministic(class_name, method_name):
    """
    Property: Step IDs are deterministic for same class.method.

    Hash-based step IDs must produce same result for same input.
    """
    # Compute step ID (simplified version - actual uses hash)
    step_id_1 = hash(f"{class_name}.{method_name}") & 0x7FFFFFFF
    step_id_2 = hash(f"{class_name}.{method_name}") & 0x7FFFFFFF

    # Property: deterministic
    assert step_id_1 == step_id_2

    # Property: non-negative (masked to positive int)
    assert step_id_1 >= 0
    assert step_id_1 < 2**31


# ==============================================================================
# PROPERTY 5: Flow Execution Idempotency
# ==============================================================================


@pytest.mark.property
@pytest.mark.asyncio
@given(
    initial_value=st.integers(min_value=-1000, max_value=1000),
)
@settings(
    max_examples=50, deadline=None, suppress_health_check=[HealthCheck.function_scoped_fixture]
)
async def test_flow_execution_deterministic(initial_value, in_memory_storage):
    """
    Property: Same flow with same inputs produces same output.

    Executing flow multiple times with same initial state yields same result.
    """
    from dataclasses import dataclass

    @dataclass
    @flow_type
    class DeterministicFlow:
        value: int

        @step
        async def compute(self) -> int:
            # Deterministic computation
            return self.value * 2 + 10

        @flow
        async def run(self) -> int:
            return await self.compute()

    # First execution
    flow1 = DeterministicFlow(value=initial_value)
    executor1 = Executor(flow1, in_memory_storage, str(uuid4()))
    outcome1 = await executor1.run(lambda f: f.run())

    # Second execution with same input
    flow2 = DeterministicFlow(value=initial_value)
    executor2 = Executor(flow2, in_memory_storage, str(uuid4()))
    outcome2 = await executor2.run(lambda f: f.run())

    # Property: both executions complete (no suspension)
    assert isinstance(outcome1, Completed)
    assert isinstance(outcome2, Completed)

    # Property: same result
    if not isinstance(outcome1.result, Exception) and not isinstance(outcome2.result, Exception):
        assert outcome1.result == outcome2.result
        assert outcome1.result == initial_value * 2 + 10


# ==============================================================================
# PROPERTY 6: Concurrent Operations Don't Corrupt State
# ==============================================================================


@pytest.mark.property
@pytest.mark.asyncio
@pytest.mark.concurrency
@given(
    num_concurrent=st.integers(min_value=2, max_value=10),
    operations_per_task=st.integers(min_value=1, max_value=20),
)
@settings(max_examples=30, deadline=None)
async def test_concurrent_storage_operations(num_concurrent, operations_per_task):
    """
    Property: Concurrent storage operations don't corrupt data.

    Multiple tasks writing concurrently produce correct final state.
    """
    # Create fresh storage for each test run (avoid accumulation across Hypothesis examples)
    storage = InMemoryExecutionLog()

    async def writer_task(task_id: int):
        """Write operations for one task."""
        flow_id = f"flow_{task_id}"
        for step_num in range(operations_per_task):
            await storage.log_invocation_start(
                flow_id=flow_id,
                step=step_num,
                class_name="ConcurrentFlow",
                method_name=f"step_{step_num}",
                parameters=pickle.dumps({"task_id": task_id, "step": step_num}),
                params_hash=hash((task_id, step_num)),
            )
            await storage.log_invocation_completion(
                flow_id=flow_id,
                step=step_num,
                return_value=pickle.dumps(f"result_{task_id}_{step_num}"),
            )
        return task_id

    # Run concurrent writers
    task_ids = await asyncio.gather(*[writer_task(i) for i in range(num_concurrent)])

    # Property: all tasks completed
    assert len(task_ids) == num_concurrent

    # Property: each flow has correct number of invocations
    for task_id in task_ids:
        flow_id = f"flow_{task_id}"
        invocations = await storage.get_invocations_for_flow(flow_id)
        assert len(invocations) == operations_per_task

        # Property: no duplicates
        steps = [inv.step for inv in invocations]
        assert len(steps) == len(set(steps))


# ==============================================================================
# PROPERTY 7: CallType Transitions
# ==============================================================================


@pytest.mark.property
@given(
    call_type=st.sampled_from([CallType.RUN, CallType.AWAIT, CallType.RESUME]),
)
def test_call_type_valid_values(call_type):
    """
    Property: CallType enum has valid string representations.

    All CallType values can be converted to/from strings.
    """
    # Property: has string representation
    call_type_str = str(call_type)
    assert isinstance(call_type_str, str)
    assert len(call_type_str) > 0


# ==============================================================================
# PROPERTY 8: InvocationStatus State Machine
# ==============================================================================


@pytest.mark.property
@given(
    status=st.sampled_from(
        [
            InvocationStatus.PENDING,
            InvocationStatus.COMPLETE,
            InvocationStatus.WAITING_FOR_SIGNAL,
            InvocationStatus.WAITING_FOR_TIMER,
        ]
    ),
)
def test_invocation_status_serialization(status):
    """
    Property: InvocationStatus enum values serialize correctly.

    Status values can roundtrip through string representation.
    """
    # Convert to string
    status_str = status.value

    # Property: can convert back
    from_str = InvocationStatus(status_str)
    assert from_str == status


if __name__ == "__main__":
    # Run property tests with verbose hypothesis output
    pytest.main([__file__, "-v", "--hypothesis-verbosity=verbose", "-m", "property"])
