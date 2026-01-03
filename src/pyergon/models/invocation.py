"""
Invocation represents a single step execution with all its metadata.

Design principles:
- Immutable after creation (use dataclass with frozen=False for updates)
- All fields have sensible defaults
- Serialization-friendly (all fields are basic types or serializable)

COMPLIANCE: Matches Rust ergon src/core/invocation.rs exactly
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
    """
    Represents a single durable step execution.

    An Invocation tracks everything about a step's execution:
    - Identity (id, flow_id, step number, class_name, method_name)
    - Parameters (serialized bytes + params_hash)
    - Result (serialized bytes)
    - Status (pending, waiting, complete)
    - Retry metadata (attempts, is_retryable, retry_policy)
    - Timer metadata (fire_at for timer-based steps)
    - Delay metadata (delay for step delays)

    Design: Value object pattern - represents a snapshot of execution state

    RUST COMPLIANCE:
    Matches Rust's Invocation struct (src/core/invocation.rs:51-75) exactly:
    - All fields present with correct types
    - Field names match (except snake_case conventions)
    - Semantics preserved (e.g., is_retryable as Optional[bool])
    """

    # ==========================================================================
    # Identity (matches Rust lines 52-56)
    # ==========================================================================

    id: str
    """Unique identifier for this invocation (UUID).

    Rust: id: Uuid
    Python: str (UUID as string for simplicity)
    """

    flow_id: str
    """Unique identifier for the flow instance (matches parent flow's ID).

    Note: In Rust this is part of the identity. In Python we keep it
    for compatibility but id is the primary identifier.
    """

    step: int
    """Step number within the flow (0-indexed)"""

    class_name: str
    """Name of the flow class"""

    method_name: str
    """Name of the step method.

    Rust: method_name: String
    Python: method_name (not step_name!)
    """

    # ==========================================================================
    # Execution data (matches Rust lines 57-61)
    # ==========================================================================

    parameters: bytes
    """Serialized parameters (using pickle).

    Rust: parameters: Vec<u8>
    Python: bytes
    """

    params_hash: int
    """Hash of parameters for non-determinism detection.

    Rust: params_hash: u64
    Python: int (Python int is arbitrary precision)

    CRITICAL: Used to detect when a step is called with different parameters
    than previous execution (non-deterministic behavior).
    """

    return_value: bytes | None = None
    """Serialized result (using pickle), None if not yet complete.

    Rust: return_value: Option<Vec<u8>>
    Python: Optional[bytes]

    NOTE: In Rust, errors are stored here as serialized Result::Err.
    Python stores exceptions the same way.
    """

    # ==========================================================================
    # Status tracking (matches Rust lines 55, 57-58)
    # ==========================================================================

    timestamp: datetime = field(default_factory=datetime.now)
    """When this invocation was created (single timestamp).

    Rust: timestamp: DateTime<Utc>
    Python: datetime (we keep both timestamp and updated_at for convenience)
    """

    status: InvocationStatus = InvocationStatus.PENDING
    """Current execution status"""

    attempts: int = 0
    """Number of execution attempts (for retry tracking)"""

    updated_at: datetime = field(default_factory=datetime.now)
    """When this invocation was last updated (Python extension)"""

    # ==========================================================================
    # Retry configuration (matches Rust lines 62-68)
    # ==========================================================================

    delay: int | None = None
    """Optional delay before execution in milliseconds.

    Rust: delay: Option<i64>
    Python: Optional[int]

    Used for step delays: @step(delay=100, unit="MILLIS")
    """

    retry_policy: RetryPolicy | None = None
    """Retry policy for this invocation (None means no retries).

    Rust: retry_policy: Option<RetryPolicy>
    Python: Optional[RetryPolicy]

    Controls how many times and when to retry on transient errors.
    """

    is_retryable: bool | None = None
    """Whether the cached error is retryable.

    Rust: is_retryable: Option<bool>
    Python: Optional[bool]

    CRITICAL SEMANTIC:
    - None = not an error (Ok result)
    - Some(true) = retryable error (transient failure)
    - Some(false) = non-retryable error (permanent failure)

    This is NOT a bool! Must be Optional[bool] to match Rust semantics.
    """

    # ==========================================================================
    # Timer support (matches Rust lines 69-74)
    # ==========================================================================

    timer_fire_at: datetime | None = None
    """When the timer should fire (for WAITING_FOR_TIMER status).

    Rust: timer_fire_at: Option<DateTime<Utc>>
    Python: Optional[datetime]
    """

    timer_name: str | None = None
    """Optional timer name for debugging.

    Rust: timer_name: Option<String>
    Python: Optional[str]

    FIXED: Was str (empty string) before, now Optional[str] (None) to match Rust.
    """

    def __post_init__(self):
        """Validate invariants after creation."""
        if self.step < 0:
            raise ValueError(f"Step must be non-negative, got {self.step}")

        if self.attempts < 0:
            raise ValueError(f"Attempts must be non-negative, got {self.attempts}")

        if self.status == InvocationStatus.WAITING_FOR_TIMER and self.timer_fire_at is None:
            raise ValueError("timer_fire_at must be set when status is WAITING_FOR_TIMER")

    # ==========================================================================
    # Methods (matches Rust lines 161-218)
    # ==========================================================================

    def is_flow(self) -> bool:
        """Check if this invocation is for a flow entry point (step == 0).

        Rust: pub fn is_flow(&self) -> bool { self.step == 0 }
        """
        return self.step == 0

    def get_params_hash(self) -> int:
        """Get the parameters hash.

        Rust: pub fn params_hash(&self) -> u64 { self.params_hash }
        """
        return self.params_hash

    def get_delay(self) -> int | None:
        """Get the delay in milliseconds.

        Rust: pub fn delay(&self) -> Option<Duration>
        Python returns milliseconds directly (not Duration object).
        """
        return self.delay

    def get_retry_policy(self) -> RetryPolicy | None:
        """Get the retry policy.

        Rust: pub fn retry_policy(&self) -> Option<RetryPolicy>
        """
        return self.retry_policy

    def get_is_retryable(self) -> bool | None:
        """Get whether this invocation has a retryable error.

        Rust: pub fn is_retryable(&self) -> Option<bool>

        Returns:
            None if not an error, True if retryable, False if permanent
        """
        return self.is_retryable

    def increment_attempts(self) -> None:
        """Increment the attempt counter.

        Rust: pub fn increment_attempts(&mut self) { self.attempts += 1; }

        Note: Mutates the invocation (not frozen dataclass).
        """
        self.attempts += 1

    def set_status(self, status: InvocationStatus) -> None:
        """Set the invocation status.

        Rust: pub fn set_status(&mut self, status: InvocationStatus)
        """
        self.status = status
        self.updated_at = datetime.now()

    def set_return_value(self, return_value: bytes) -> None:
        """Set the return value.

        Rust: pub fn set_return_value(&mut self, return_value: Vec<u8>)
        """
        self.return_value = return_value
        self.updated_at = datetime.now()

    def set_is_retryable(self, is_retryable: bool | None) -> None:
        """Set whether this error is retryable.

        Rust: pub fn set_is_retryable(&mut self, is_retryable: Option<bool>)
        """
        self.is_retryable = is_retryable
        self.updated_at = datetime.now()

    def set_timer_fire_at(self, fire_at: datetime | None) -> None:
        """Set when the timer should fire.

        Rust: pub fn set_timer_fire_at(&mut self, fire_at: Option<DateTime<Utc>>)
        """
        self.timer_fire_at = fire_at
        self.updated_at = datetime.now()

    def set_timer_name(self, name: str | None) -> None:
        """Set the timer name.

        Rust: pub fn set_timer_name(&mut self, name: Option<String>)
        """
        self.timer_name = name
        self.updated_at = datetime.now()

    def deserialize_parameters(self, type_: type[T]) -> T:
        """Deserialize parameters to the specified type.

        Rust: pub fn deserialize_parameters<T: for<'de> Deserialize<'de>>(&self) -> Result<T>

        Args:
            type_: Target type (used for type hint only, not enforced)

        Returns:
            Deserialized parameters

        Raises:
            pickle.UnpicklingError: If deserialization fails
        """
        return pickle.loads(self.parameters)

    def deserialize_return_value(self, type_: type[T]) -> T | None:
        """Deserialize return value to the specified type.

        Rust: pub fn deserialize_return_value<T: for<'de> Deserialize<'de>>(
            &self
        ) -> Result<Option<T>>

        Args:
            type_: Target type (used for type hint only, not enforced)

        Returns:
            Deserialized return value, or None if not set

        Raises:
            pickle.UnpicklingError: If deserialization fails
        """
        if self.return_value is None:
            return None
        return pickle.loads(self.return_value)

    def is_timer_expired(self) -> bool:
        """Check if the timer has expired.

        Rust: pub fn is_timer_expired(&self) -> bool

        Returns:
            True if timer has expired, False otherwise
        """
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
