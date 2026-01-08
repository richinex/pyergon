"""Tests for Invocation model methods and properties."""

import pickle
from datetime import UTC, datetime, timedelta
from uuid import uuid4

from pyergon.core import InvocationStatus, RetryPolicy
from pyergon.models.invocation import Invocation


def test_invocation_is_flow():
    """Test is_flow() method."""
    # Step 0 is the flow run
    flow_inv = Invocation(
        id=str(uuid4()),
        flow_id=str(uuid4()),
        step=0,
        class_name="TestFlow",
        method_name="run",
        parameters=b"params",
        params_hash=123,
        status=InvocationStatus.COMPLETE,
        attempts=1,
    )
    assert flow_inv.is_flow() is True

    # Other steps are not flows
    step_inv = Invocation(
        id=str(uuid4()),
        flow_id=str(uuid4()),
        step=5,
        class_name="TestFlow",
        method_name="process",
        parameters=b"params",
        params_hash=456,
        status=InvocationStatus.COMPLETE,
        attempts=1,
    )
    assert step_inv.is_flow() is False


def test_invocation_get_params_hash():
    """Test get_params_hash() method."""
    inv = Invocation(
        id=str(uuid4()),
        flow_id=str(uuid4()),
        step=1,
        class_name="TestFlow",
        method_name="test",
        parameters=b"params",
        params_hash=12345,
        status=InvocationStatus.COMPLETE,
        attempts=1,
    )
    assert inv.get_params_hash() == 12345


def test_invocation_get_retry_policy():
    """Test get_retry_policy() method."""
    inv = Invocation(
        id=str(uuid4()),
        flow_id=str(uuid4()),
        step=0,
        class_name="TestFlow",
        method_name="run",
        parameters=b"params",
        params_hash=123,
        status=InvocationStatus.COMPLETE,
        attempts=1,
        retry_policy=RetryPolicy.STANDARD,
    )
    assert inv.get_retry_policy() == RetryPolicy.STANDARD

    # None case
    inv_none = Invocation(
        id=str(uuid4()),
        flow_id=str(uuid4()),
        step=0,
        class_name="TestFlow",
        method_name="run",
        parameters=b"params",
        params_hash=123,
        status=InvocationStatus.COMPLETE,
        attempts=1,
    )
    assert inv_none.get_retry_policy() is None


def test_invocation_get_is_retryable():
    """Test get_is_retryable() method."""
    # True case
    inv_true = Invocation(
        id=str(uuid4()),
        flow_id=str(uuid4()),
        step=1,
        class_name="TestFlow",
        method_name="test",
        parameters=b"params",
        params_hash=123,
        status=InvocationStatus.COMPLETE,
        attempts=1,
        is_retryable=True,
    )
    assert inv_true.get_is_retryable() is True

    # False case
    inv_false = Invocation(
        id=str(uuid4()),
        flow_id=str(uuid4()),
        step=1,
        class_name="TestFlow",
        method_name="test",
        parameters=b"params",
        params_hash=123,
        status=InvocationStatus.COMPLETE,
        attempts=1,
        is_retryable=False,
    )
    assert inv_false.get_is_retryable() is False

    # None case
    inv_none = Invocation(
        id=str(uuid4()),
        flow_id=str(uuid4()),
        step=1,
        class_name="TestFlow",
        method_name="test",
        parameters=b"params",
        params_hash=123,
        status=InvocationStatus.COMPLETE,
        attempts=1,
    )
    assert inv_none.get_is_retryable() is None


def test_invocation_increment_attempts():
    """Test increment_attempts() method."""
    inv = Invocation(
        id=str(uuid4()),
        flow_id=str(uuid4()),
        step=1,
        class_name="TestFlow",
        method_name="test",
        parameters=b"params",
        params_hash=123,
        status=InvocationStatus.COMPLETE,
        attempts=1,
    )

    assert inv.attempts == 1
    inv.increment_attempts()
    assert inv.attempts == 2
    inv.increment_attempts()
    assert inv.attempts == 3


def test_invocation_set_status():
    """Test set_status() method."""
    inv = Invocation(
        id=str(uuid4()),
        flow_id=str(uuid4()),
        step=1,
        class_name="TestFlow",
        method_name="test",
        parameters=b"params",
        params_hash=123,
        status=InvocationStatus.PENDING,
        attempts=1,
    )

    assert inv.status == InvocationStatus.PENDING
    inv.set_status(InvocationStatus.COMPLETE)
    assert inv.status == InvocationStatus.COMPLETE


def test_invocation_set_return_value():
    """Test set_return_value() method."""
    inv = Invocation(
        id=str(uuid4()),
        flow_id=str(uuid4()),
        step=1,
        class_name="TestFlow",
        method_name="test",
        parameters=b"params",
        params_hash=123,
        status=InvocationStatus.COMPLETE,
        attempts=1,
    )

    assert inv.return_value is None

    result = {"key": "value"}
    result_bytes = pickle.dumps(result)
    inv.set_return_value(result_bytes)

    assert inv.return_value == result_bytes


def test_invocation_set_is_retryable():
    """Test set_is_retryable() method."""
    inv = Invocation(
        id=str(uuid4()),
        flow_id=str(uuid4()),
        step=1,
        class_name="TestFlow",
        method_name="test",
        parameters=b"params",
        params_hash=123,
        status=InvocationStatus.COMPLETE,
        attempts=1,
    )

    assert inv.is_retryable is None

    inv.set_is_retryable(True)
    assert inv.is_retryable is True

    inv.set_is_retryable(False)
    assert inv.is_retryable is False

    inv.set_is_retryable(None)
    assert inv.is_retryable is None


def test_invocation_set_timer_fire_at():
    """Test set_timer_fire_at() method."""
    fire_time = datetime.now(UTC) + timedelta(seconds=30)

    # Must provide timer_fire_at when status is WAITING_FOR_TIMER
    inv = Invocation(
        id=str(uuid4()),
        flow_id=str(uuid4()),
        step=1,
        class_name="TestFlow",
        method_name="test",
        parameters=b"params",
        params_hash=123,
        status=InvocationStatus.WAITING_FOR_TIMER,
        attempts=1,
        timer_fire_at=fire_time,
    )

    assert inv.timer_fire_at == fire_time

    # Update to new time
    new_time = datetime.now(UTC) + timedelta(seconds=60)
    inv.set_timer_fire_at(new_time)
    assert inv.timer_fire_at == new_time


def test_invocation_set_timer_name():
    """Test set_timer_name() method."""
    fire_time = datetime.now(UTC) + timedelta(seconds=30)

    inv = Invocation(
        id=str(uuid4()),
        flow_id=str(uuid4()),
        step=1,
        class_name="TestFlow",
        method_name="test",
        parameters=b"params",
        params_hash=123,
        status=InvocationStatus.WAITING_FOR_TIMER,
        attempts=1,
        timer_fire_at=fire_time,
    )

    assert inv.timer_name is None

    inv.set_timer_name("my_timer")
    assert inv.timer_name == "my_timer"

    inv.set_timer_name(None)
    assert inv.timer_name is None


def test_invocation_deserialize_parameters():
    """Test deserialize_parameters() method."""
    # Create simple dict params
    params = {"value": 42}
    params_bytes = pickle.dumps(params)

    inv = Invocation(
        id=str(uuid4()),
        flow_id=str(uuid4()),
        step=1,
        class_name="TestFlow",
        method_name="test",
        parameters=params_bytes,
        params_hash=123,
        status=InvocationStatus.COMPLETE,
        attempts=1,
    )

    deserialized = inv.deserialize_parameters(dict)
    assert deserialized["value"] == 42


def test_invocation_deserialize_return_value():
    """Test deserialize_return_value() method."""
    result = {"result": "success"}
    result_bytes = pickle.dumps(result)

    inv = Invocation(
        id=str(uuid4()),
        flow_id=str(uuid4()),
        step=1,
        class_name="TestFlow",
        method_name="test",
        parameters=b"params",
        params_hash=123,
        status=InvocationStatus.COMPLETE,
        attempts=1,
        return_value=result_bytes,
    )

    deserialized = inv.deserialize_return_value(dict)
    assert deserialized == result

    # None case
    inv_none = Invocation(
        id=str(uuid4()),
        flow_id=str(uuid4()),
        step=1,
        class_name="TestFlow",
        method_name="test",
        parameters=b"params",
        params_hash=123,
        status=InvocationStatus.COMPLETE,
        attempts=1,
    )
    assert inv_none.deserialize_return_value(dict) is None


def test_invocation_is_timer_expired():
    """Test is_timer_expired() method."""
    # Expired timer (use naive datetime to match is_timer_expired() implementation)
    past = datetime.now() - timedelta(seconds=10)
    inv_expired = Invocation(
        id=str(uuid4()),
        flow_id=str(uuid4()),
        step=1,
        class_name="TestFlow",
        method_name="test",
        parameters=b"params",
        params_hash=123,
        status=InvocationStatus.WAITING_FOR_TIMER,
        attempts=1,
        timer_fire_at=past,
    )
    assert inv_expired.is_timer_expired() is True

    # Future timer
    future = datetime.now() + timedelta(seconds=30)
    inv_future = Invocation(
        id=str(uuid4()),
        flow_id=str(uuid4()),
        step=1,
        class_name="TestFlow",
        method_name="test",
        parameters=b"params",
        params_hash=123,
        status=InvocationStatus.WAITING_FOR_TIMER,
        attempts=1,
        timer_fire_at=future,
    )
    assert inv_future.is_timer_expired() is False

    # No timer
    inv_no_timer = Invocation(
        id=str(uuid4()),
        flow_id=str(uuid4()),
        step=1,
        class_name="TestFlow",
        method_name="test",
        parameters=b"params",
        params_hash=123,
        status=InvocationStatus.COMPLETE,
        attempts=1,
    )
    assert inv_no_timer.is_timer_expired() is False


def test_invocation_is_complete():
    """Test is_complete property."""
    # Complete invocation
    inv_complete = Invocation(
        id=str(uuid4()),
        flow_id=str(uuid4()),
        step=1,
        class_name="TestFlow",
        method_name="test",
        parameters=b"params",
        params_hash=123,
        status=InvocationStatus.COMPLETE,
        attempts=1,
    )
    assert inv_complete.is_complete is True

    # Not complete
    inv_pending = Invocation(
        id=str(uuid4()),
        flow_id=str(uuid4()),
        step=1,
        class_name="TestFlow",
        method_name="test",
        parameters=b"params",
        params_hash=123,
        status=InvocationStatus.PENDING,
        attempts=1,
    )
    assert inv_pending.is_complete is False


def test_invocation_is_waiting():
    """Test is_waiting property."""
    # Waiting for signal
    inv_signal = Invocation(
        id=str(uuid4()),
        flow_id=str(uuid4()),
        step=1,
        class_name="TestFlow",
        method_name="test",
        parameters=b"params",
        params_hash=123,
        status=InvocationStatus.WAITING_FOR_SIGNAL,
        attempts=1,
    )
    assert inv_signal.is_waiting is True

    # Waiting for timer
    fire_time = datetime.now(UTC) + timedelta(seconds=30)
    inv_timer = Invocation(
        id=str(uuid4()),
        flow_id=str(uuid4()),
        step=1,
        class_name="TestFlow",
        method_name="test",
        parameters=b"params",
        params_hash=123,
        status=InvocationStatus.WAITING_FOR_TIMER,
        attempts=1,
        timer_fire_at=fire_time,
    )
    assert inv_timer.is_waiting is True

    # Not waiting
    inv_complete = Invocation(
        id=str(uuid4()),
        flow_id=str(uuid4()),
        step=1,
        class_name="TestFlow",
        method_name="test",
        parameters=b"params",
        params_hash=123,
        status=InvocationStatus.COMPLETE,
        attempts=1,
    )
    assert inv_complete.is_waiting is False


def test_invocation_repr():
    """Test __repr__() method."""
    inv = Invocation(
        id=str(uuid4()),
        flow_id=str(uuid4()),
        step=5,
        class_name="TestFlow",
        method_name="test_method",
        parameters=b"params",
        params_hash=123,
        status=InvocationStatus.COMPLETE,
        attempts=2,
    )

    repr_str = repr(inv)
    assert "Invocation" in repr_str
    assert "step=5" in repr_str
    assert "test_method" in repr_str
    assert "COMPLETE" in repr_str


def test_invocation_all_fields():
    """Test invocation with all fields populated."""
    inv_id = str(uuid4())
    flow_id = str(uuid4())
    now = datetime.now(UTC)
    fire_at = now + timedelta(seconds=60)
    params = {"test": "data"}
    result = {"output": "value"}

    inv = Invocation(
        id=inv_id,
        flow_id=flow_id,
        step=3,
        class_name="ComplexFlow",
        method_name="complex_step",
        parameters=pickle.dumps(params),
        params_hash=999,
        status=InvocationStatus.WAITING_FOR_TIMER,
        attempts=2,
        return_value=pickle.dumps(result),
        is_retryable=True,
        retry_policy=RetryPolicy.AGGRESSIVE,
        timer_fire_at=fire_at,
        timer_name="test_timer",
    )

    assert inv.id == inv_id
    assert inv.flow_id == flow_id
    assert inv.step == 3
    assert inv.class_name == "ComplexFlow"
    assert inv.method_name == "complex_step"
    assert inv.parameters == pickle.dumps(params)
    assert inv.params_hash == 999
    assert inv.return_value == pickle.dumps(result)
    assert inv.is_retryable is True
    assert inv.retry_policy == RetryPolicy.AGGRESSIVE
    assert inv.timer_fire_at == fire_at
    assert inv.timer_name == "test_timer"


def test_invocation_minimal_fields():
    """Test invocation with minimal required fields."""
    inv = Invocation(
        id=str(uuid4()),
        flow_id=str(uuid4()),
        step=0,
        class_name="TestFlow",
        method_name="run",
        parameters=b"params",
        params_hash=123,
        status=InvocationStatus.PENDING,
        attempts=1,
    )

    # All optional fields should be None
    assert inv.return_value is None
    assert inv.is_retryable is None
    assert inv.retry_policy is None
    assert inv.timer_fire_at is None
    assert inv.timer_name is None
