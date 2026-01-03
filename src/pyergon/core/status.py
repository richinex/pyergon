"""
Status enums for ergon execution tracking.

Following Dave Cheney's principle: "Make zero values useful"
The default status should represent the initial state.
"""

from enum import Enum


class InvocationStatus(Enum):
    """
    Status of a step invocation.

    RUST COMPLIANCE: Matches Rust InvocationStatus exactly (src/core/invocation.rs:11-16)
    No FAILED status - failures are represented as COMPLETE with error in return_value.

    Lifecycle:
    PENDING → WAITING_FOR_TIMER/WAITING_FOR_SIGNAL → COMPLETE

    Design: Simple state machine with clear transitions
    """

    PENDING = "PENDING"
    """Step is queued for execution.

    Rust: Pending (stored as 'PENDING' in DB)
    """

    WAITING_FOR_SIGNAL = "WAITING_FOR_SIGNAL"
    """Step is waiting for external signal.

    Rust: WaitingForSignal (stored as 'WAITING_FOR_SIGNAL' in DB)
    """

    WAITING_FOR_TIMER = "WAITING_FOR_TIMER"
    """Step is waiting for timer to fire.

    Rust: WaitingForTimer (stored as 'WAITING_FOR_TIMER' in DB)
    """

    COMPLETE = "COMPLETE"
    """Step completed (successfully or with error).

    Rust: Complete

    CRITICAL: In Rust/Python, errors are stored in return_value as serialized exceptions,
    not as a separate FAILED status. This allows retry logic to distinguish between
    retryable and non-retryable errors using the is_retryable field.
    """

    @property
    def is_complete(self) -> bool:
        """Check if this status represents completion."""
        return self == InvocationStatus.COMPLETE

    @property
    def is_waiting(self) -> bool:
        """Check if this status represents a waiting state."""
        return self in (InvocationStatus.WAITING_FOR_SIGNAL, InvocationStatus.WAITING_FOR_TIMER)

    def __str__(self) -> str:
        return self.value


class TaskStatus(Enum):
    """
    Status of a scheduled flow task.

    **Rust Reference**: `src/storage/queue.rs` lines 11-23

    Lifecycle:
    PENDING → RUNNING → SUSPENDED → PENDING → RUNNING → COMPLETE/FAILED

    Design: Status for the distributed queue
    """

    PENDING = "PENDING"
    """Task is queued, waiting for a worker.

    Rust: Pending (stored as 'PENDING' in DB - line 29)
    """

    RUNNING = "RUNNING"
    """Task is being executed by a worker.

    Rust: Running (stored as 'RUNNING' in DB - line 30)
    """

    SUSPENDED = "SUSPENDED"
    """Task is suspended, waiting for external signal or child flow completion.

    Rust: Suspended (stored as 'SUSPENDED' in DB - line 31)

    When a flow suspends (e.g., waiting for child flow result or external signal),
    the worker marks it SUSPENDED. Later, when the signal arrives, resume_flow()
    changes status back to PENDING so a worker can pick it up again.
    """

    COMPLETE = "COMPLETE"
    """Task completed successfully.

    Rust: Complete (stored as 'COMPLETE' in DB - line 32)
    """

    FAILED = "FAILED"
    """Task failed with non-retryable error.

    Rust: Failed
    """

    @property
    def is_terminal(self) -> bool:
        """Check if this status is terminal (no more work needed)."""
        return self in (TaskStatus.COMPLETE, TaskStatus.FAILED)

    def __str__(self) -> str:
        return self.value
