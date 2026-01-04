"""Single step execution record with metadata.

Tracks identity, parameters, results, status, retry configuration,
and timer information for a durable workflow step.

Design: Value Object Pattern
    All fields are basic types or serializable for easy persistence.
    Mutable for status updates but represents execution state snapshot.
"""

import pickle
from dataclasses import dataclass, field
from datetime import datetime
from typing import TypeVar

from pyergon.models.retry import RetryPolicy
from pyergon.models.status import InvocationStatus

T = TypeVar("T")


@dataclass
class Invocation:
    """Record of a single workflow step execution.

    Tracks:
    - Identity: flow_id, step number, class_name, method_name
    - Parameters: serialized bytes with params_hash for non-determinism detection
    - Result: serialized return value (or exception if cached)
    - Status: pending, waiting (timer/signal), or complete
    - Retry: attempts, retry_policy, is_retryable flag
    - Timing: delays, timer fire times, timestamps

    The params_hash enables detection of non-deterministic behavior by
    comparing parameters across replay executions.
    """

    # Identity fields
    id: str
    """Unique identifier for this invocation (UUID string)."""

    flow_id: str
    """Flow instance identifier (links to parent flow)."""

    step: int
    """Step number within the flow (0-indexed, step 0 is flow entry point)."""

    class_name: str
    """Name of the flow class."""

    method_name: str
    """Name of the step method."""

    # Execution data
    parameters: bytes
    """Serialized parameters (pickle format)."""

    params_hash: int
    """Hash of parameters for detecting non-deterministic replay behavior.

    When replaying a flow, if a step is called with different parameters
    than the cached execution, this hash mismatch indicates non-determinism.
    """

    return_value: bytes | None = None
    """Serialized result or exception (pickle format), None if not complete.

    Both successful results and cached errors are stored here as
    serialized bytes. Use is_retryable to distinguish error types.
    """

    # Status tracking
    timestamp: datetime = field(default_factory=datetime.now)
    """When this invocation was created."""

    status: InvocationStatus = InvocationStatus.PENDING
    """Current execution status (PENDING, WAITING_*, COMPLETE)."""

    attempts: int = 0
    """Number of execution attempts (for retry tracking)."""

    updated_at: datetime = field(default_factory=datetime.now)
    """When this invocation was last updated."""

    # Retry configuration
    delay: int | None = None
    """Optional delay before execution in milliseconds.

    Used for: @step(delay=1000)
    """

    retry_policy: RetryPolicy | None = None
    """Retry policy for this step (None means no automatic retries)."""

    is_retryable: bool | None = None
    """Whether the cached error can be retried (three-state logic).

    Values:
    - None: not an error (successful result)
    - True: retryable error (transient failure)
    - False: non-retryable error (permanent failure)

    This three-state semantic is critical for retry logic.
    """

    # Timer support
    timer_fire_at: datetime | None = None
    """When the timer should fire (for WAITING_FOR_TIMER status)."""

    timer_name: str | None = None
    """Optional timer name for debugging and deduplication."""

    def __post_init__(self):
        """Validate invariants after creation."""
        if self.step < 0:
            raise ValueError(f"Step must be non-negative, got {self.step}")

        if self.attempts < 0:
            raise ValueError(f"Attempts must be non-negative, got {self.attempts}")

        if self.status == InvocationStatus.WAITING_FOR_TIMER and self.timer_fire_at is None:
            raise ValueError("timer_fire_at must be set when status is WAITING_FOR_TIMER")

    # Methods
    def is_flow(self) -> bool:
        """Check if this is a flow entry point (step == 0)."""
        return self.step == 0

    def get_params_hash(self) -> int:
        """Return the parameters hash for non-determinism detection."""
        return self.params_hash

    def get_delay(self) -> int | None:
        """Return the delay in milliseconds, or None if no delay."""
        return self.delay

    def get_retry_policy(self) -> RetryPolicy | None:
        """Return the retry policy, or None if no retries configured."""
        return self.retry_policy

    def get_is_retryable(self) -> bool | None:
        """Return whether this error is retryable (None/True/False)."""
        return self.is_retryable

    def increment_attempts(self) -> None:
        """Increment the execution attempt counter."""
        self.attempts += 1

    def set_status(self, status: InvocationStatus) -> None:
        """Update status and timestamp."""
        self.status = status
        self.updated_at = datetime.now()

    def set_return_value(self, return_value: bytes) -> None:
        """Update return value and timestamp."""
        self.return_value = return_value
        self.updated_at = datetime.now()

    def set_is_retryable(self, is_retryable: bool | None) -> None:
        """Update retryable flag and timestamp."""
        self.is_retryable = is_retryable
        self.updated_at = datetime.now()

    def set_timer_fire_at(self, fire_at: datetime | None) -> None:
        """Update timer fire time and timestamp."""
        self.timer_fire_at = fire_at
        self.updated_at = datetime.now()

    def set_timer_name(self, name: str | None) -> None:
        """Update timer name and timestamp."""
        self.timer_name = name
        self.updated_at = datetime.now()

    def deserialize_parameters(self, type_: type[T]) -> T:
        """Deserialize parameters from pickle format.

        Args:
            type_: Target type (for type hints only)

        Returns:
            Deserialized parameters

        Raises:
            pickle.UnpicklingError: If deserialization fails
        """
        return pickle.loads(self.parameters)

    def deserialize_return_value(self, type_: type[T]) -> T | None:
        """Deserialize return value from pickle format.

        Args:
            type_: Target type (for type hints only)

        Returns:
            Deserialized return value, or None if not set

        Raises:
            pickle.UnpicklingError: If deserialization fails
        """
        if self.return_value is None:
            return None
        return pickle.loads(self.return_value)

    def is_timer_expired(self) -> bool:
        """Check if timer has passed its fire time."""
        if self.timer_fire_at is None:
            return False
        return datetime.now() >= self.timer_fire_at

    @property
    def is_complete(self) -> bool:
        """Check if this invocation is complete."""
        return self.status.is_complete

    @property
    def is_waiting(self) -> bool:
        """Check if this invocation is in a waiting state."""
        return self.status.is_waiting

    def __repr__(self) -> str:
        """Readable representation for debugging."""
        return (
            f"Invocation(id={self.id!r}, flow_id={self.flow_id!r}, step={self.step}, "
            f"method_name={self.method_name!r}, status={self.status}, "
            f"attempts={self.attempts})"
        )
