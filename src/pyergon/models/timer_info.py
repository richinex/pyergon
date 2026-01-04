"""Metadata for expired timers returned from storage."""

from dataclasses import dataclass
from datetime import datetime

__all__ = ["TimerInfo"]


@dataclass(frozen=True)
class TimerInfo:
    """Information about an expired timer ready for processing.

    Returned by ExecutionLog.get_expired_timers() to identify which
    flows need to be resumed after their timer fires.

    Immutable: frozen=True prevents modification after creation.
    """

    flow_id: str
    """Flow identifier waiting for this timer."""

    step: int
    """Step number waiting for this timer."""

    fire_at: datetime
    """When the timer was scheduled to fire."""

    timer_name: str | None = None
    """Optional timer name for debugging and deduplication."""

    def __repr__(self) -> str:
        """Readable representation for debugging."""
        name_str = f", name={self.timer_name!r}" if self.timer_name else ""
        return (
            f"TimerInfo(flow_id={self.flow_id!r}, step={self.step}, "
            f"fire_at={self.fire_at.isoformat()}{name_str})"
        )
