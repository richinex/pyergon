"""Status enumerations for workflow execution tracking.

Defines lifecycle states for individual step invocations and
queued flow tasks in the distributed work system.
"""

from enum import Enum


class InvocationStatus(Enum):
    """Status of a single step invocation.

    Lifecycle:
        PENDING → WAITING_FOR_TIMER/WAITING_FOR_SIGNAL → COMPLETE

    Design: No FAILED Status
        Failures are represented as COMPLETE with the error stored in
        return_value. This allows retry logic to distinguish between
        retryable and non-retryable errors via the is_retryable field.
    """

    PENDING = "PENDING"
    """Step is queued for execution."""

    WAITING_FOR_SIGNAL = "WAITING_FOR_SIGNAL"
    """Step is waiting for external signal."""

    WAITING_FOR_TIMER = "WAITING_FOR_TIMER"
    """Step is waiting for timer to fire."""

    COMPLETE = "COMPLETE"
    """Step completed successfully or with cached error."""

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
    """Status of a queued flow task in the distributed work system.

    Lifecycle:
        PENDING → RUNNING → SUSPENDED → PENDING → RUNNING → COMPLETE/FAILED

    Tasks can suspend when waiting for child flows, timers, or external signals,
    then resume back to PENDING when the awaited event occurs.
    """

    PENDING = "PENDING"
    """Task is queued, waiting for a worker to claim it."""

    RUNNING = "RUNNING"
    """Task is currently being executed by a worker."""

    SUSPENDED = "SUSPENDED"
    """Task is suspended, waiting for child flow, timer, or external signal.

    When the awaited event occurs, resume_flow() changes status back
    to PENDING so a worker can pick it up and continue execution.
    """

    COMPLETE = "COMPLETE"
    """Task completed successfully."""

    FAILED = "FAILED"
    """Task failed with non-retryable error after exhausting retries."""

    @property
    def is_terminal(self) -> bool:
        """Check if this status is terminal (no more work needed)."""
        return self in (TaskStatus.COMPLETE, TaskStatus.FAILED)

    def __str__(self) -> str:
        return self.value
