"""Scheduled flow task for distributed execution queue.

Represents a unit of work in the distributed work queue, tracking
execution state, retries, and parent-child relationships.
"""

from dataclasses import dataclass, field
from datetime import datetime

from pyergon.models.status import TaskStatus


@dataclass
class ScheduledFlow:
    """Workflow task in the distributed execution queue.

    Workers dequeue ScheduledFlows, execute them, and mark them complete.
    Supports retries, delayed execution, and parent-child flow relationships.

    Design: Value Object
        Represents queue state snapshot with all metadata needed for
        distributed execution and work tracking.
    """

    task_id: str
    """Unique identifier for this scheduled task (UUID string)."""

    flow_id: str
    """Unique identifier for the flow instance (UUID string)."""

    flow_type: str
    """Type name of the flow class (for handler registration lookup)."""

    flow_data: bytes
    """Serialized flow instance (pickle format)."""

    status: TaskStatus
    """Current task status (PENDING, RUNNING, SUSPENDED, COMPLETE, FAILED)."""

    locked_by: str | None = None
    """Worker ID that claimed this task, None if unclaimed."""

    retry_count: int = 0
    """Number of retry attempts so far."""

    created_at: datetime = field(default_factory=datetime.now)
    """When this task was created."""

    updated_at: datetime = field(default_factory=datetime.now)
    """When this task was last updated."""

    error_message: str | None = None
    """Error message from last failed execution, None if no error."""

    scheduled_for: datetime | None = None
    """When this task should execute (for delayed retry), None for immediate execution."""

    parent_metadata: tuple[str, str] | None = None
    """Parent flow metadata (parent_flow_id, signal_token) for child flows.

    If this flow was invoked as a child, contains parent flow ID and
    signal token for result delivery.
    """

    retry_policy: dict | None = None
    """Retry configuration (max_retries, backoff, etc.), None for no retries."""

    version: str | None = None
    """Semantic version of the flow code (e.g., "1.0.0").

    Tracks which version of code executed this flow. Set by
    Scheduler.with_version() at schedule time.
    """

    claimed_at: datetime | None = None
    """When this task was claimed by a worker."""

    completed_at: datetime | None = None
    """When this task completed execution."""

    def __repr__(self) -> str:
        """Readable representation for debugging."""
        return (
            f"ScheduledFlow(task_id={self.task_id!r}, flow_id={self.flow_id!r}, "
            f"flow_type={self.flow_type!r}, status={self.status}, "
            f"locked_by={self.locked_by!r}, retry_count={self.retry_count})"
        )
