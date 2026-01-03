"""
Common types used across ergon modules.

Design: Keep types minimal and focused.
Each type should have a clear, single purpose.

RUST COMPLIANCE: Matches Rust ergon src/storage/queue.rs types
"""

from dataclasses import dataclass, field
from datetime import datetime

from pyergon.models.status import TaskStatus


@dataclass
class ScheduledFlow:
    """
    Represents a flow scheduled for distributed execution.

    This is the unit of work in the distributed queue.
    Workers dequeue ScheduledFlows, execute them, and mark them complete.

    Design: Value object for the queue system

    RUST COMPLIANCE: Matches Rust ScheduledFlow (src/storage/queue.rs:pub struct ScheduledFlow)
    """

    task_id: str
    """Unique identifier for this scheduled task.

    Rust: task_id: Uuid
    Python: str (UUID as string)
    """

    flow_id: str
    """Unique identifier for the flow instance.

    Rust: flow_id: Uuid
    Python: str (UUID as string)
    """

    flow_type: str
    """Type name of the flow class (for handler lookup).

    Rust: flow_type: String
    """

    flow_data: bytes
    """Serialized flow instance.

    Rust: flow_data: Vec<u8>
    """

    status: TaskStatus
    """Current task status.

    Rust: status: TaskStatus
    """

    locked_by: str | None = None
    """Worker ID that claimed this task (for work distribution tracking).

    Rust: locked_by: Option<String>
    """

    retry_count: int = 0
    """Number of retry attempts.

    Rust: retry_count: u32
    """

    created_at: datetime = field(default_factory=datetime.now)
    """When this task was created.

    Rust: created_at: DateTime<Utc>
    Python: Renamed from scheduled_at to match Rust
    """

    updated_at: datetime = field(default_factory=datetime.now)
    """When this task was last updated.

    Rust: updated_at: DateTime<Utc>
    Python: ADDED to match Rust
    """

    error_message: str | None = None
    """Error message from last execution (if failed).

    Rust: error_message: Option<String>
    Python: ADDED to match Rust
    """

    scheduled_for: datetime | None = None
    """When this task should be executed (for delayed retry).

    Rust: scheduled_for: Option<DateTime<Utc>>
    If None, execute immediately.
    """

    parent_metadata: tuple[str, str] | None = None
    """Parent flow metadata for child invocation.

    Tuple of (parent_flow_id, signal_token) if this is a child flow.
    Maps to Rust fields: parent_flow_id and signal_token.
    """

    retry_policy: dict | None = None
    """Retry policy for this flow.

    Contains retry configuration (max_retries, backoff, etc.)
    """

    version: str | None = None
    """Semantic version of the flow (e.g., "1.0.0", "v2").

    **Rust Reference**: `src/storage/queue.rs` line 97

    Used to track which version of code executed a flow.
    Set by Scheduler.with_version() at schedule time.

    Rust: version: Option<String>
    """

    # Python extensions (not in Rust, but useful)
    claimed_at: datetime | None = None
    """When this task was claimed by a worker (Python extension)"""

    completed_at: datetime | None = None
    """When this task completed (Python extension)"""

    def __repr__(self) -> str:
        """Readable representation for debugging."""
        return (
            f"ScheduledFlow(task_id={self.task_id!r}, flow_id={self.flow_id!r}, "
            f"flow_type={self.flow_type!r}, status={self.status}, "
            f"locked_by={self.locked_by!r}, retry_count={self.retry_count})"
        )
